/*
 * Copyright (c) 2025, Natacha Porté
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/glebarez/go-sqlite"
	luajson "github.com/layeh/gopher-json"
	"github.com/yuin/gopher-lua"
	"instinctive.eu/go/mqttagent"
)

type fullMqttAgent struct {
	loggers    map[string]*sqlogger
	oldLoggers map[string]*sqlogger
}

func (agent *fullMqttAgent) Setup(L *lua.LState) {
	luajson.Preload(L)
	agent.loggers = make(map[string]*sqlogger)

	mt := L.NewTypeMetatable("sqlogger")
	L.SetGlobal("sqlogger", mt)
	L.SetField(mt, "new", L.NewFunction(func(L *lua.LState) int {
		return luaSqloggerNew(L, agent)
	}))
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), luaSqloggerMethods))
}

func (agent *fullMqttAgent) Teardown(L *lua.LState) {
	if agent.oldLoggers != nil {
		panic("Unexpected state")
	}
	for _, logger := range agent.loggers {
		logger.Close()
	}
	agent.loggers = nil
}

func (agent *fullMqttAgent) ReloadBegin(oldL, newL *lua.LState) {
	if agent.oldLoggers != nil {
		panic("Unexpected state")
	}
	agent.oldLoggers = agent.loggers
	agent.Setup(newL)
}

func (agent *fullMqttAgent) ReloadAbort(oldL, newL *lua.LState) {
	for key, logger := range agent.loggers {
		if _, found := agent.oldLoggers[key]; !found {
			logger.Close()
		}
	}
	agent.loggers = agent.oldLoggers
	agent.oldLoggers = nil
}

func (agent *fullMqttAgent) ReloadEnd(oldL, newL *lua.LState) {
	for key, logger := range agent.oldLoggers {
		if _, found := agent.loggers[key]; !found {
			logger.Close()
		}
	}
	agent.oldLoggers = nil
}

func (logger *sqlogger) Received(msg *mqttagent.MqttMessage) {
	if logger.insertTopic == nil || logger.insertReceived == nil {
		return
	}

	if _, err := logger.insertTopic.Exec(msg.Topic); err != nil {
		log.Println(err)
		return
	}

	if _, err := logger.insertReceived.Exec((msg.Timestamp/86400.0)+2440587.5, msg.Topic, msg.Message); err != nil {
		log.Println(err)
		return
	}
}

type sqlogger struct {
	db             *sql.DB
	insertTopic    *sql.Stmt
	insertReceived *sql.Stmt
}

func (logger *sqlogger) Close() {
	if logger == nil {
		return
	}

	if logger.insertReceived != nil {
		logger.insertReceived.Close()
	}

	if logger.insertTopic != nil {
		logger.insertTopic.Close()
	}

	if logger.db != nil {
		logger.db.Close()
	}
}

func connect(connectionString string) (*sqlogger, error) {
	db, err := sql.Open("sqlite", connectionString)
	if err != nil {
		return nil, err
	}

	for _, cmd := range []string{
		"CREATE TABLE IF NOT EXISTS topics" +
			"(id INTEGER PRIMARY KEY AUTOINCREMENT," +
			" name TEXT NOT NULL);",
		"CREATE INDEX IF NOT EXISTS i_topics ON topics(name);",
		"CREATE TABLE IF NOT EXISTS received" +
			"(timestamp INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			" topic_id INTEGER NOT NULL," +
			" message TEXT NOT NULL," +
			" FOREIGN KEY (topic_id) REFERENCES topics (id));",
		"CREATE INDEX IF NOT EXISTS i_time ON received(timestamp);",
		"CREATE INDEX IF NOT EXISTS i_topicid ON received(topic_id);",
	} {
		if _, err = db.Exec(cmd); err != nil {
			db.Close()
			return nil, err
		}
	}

	s1, err := db.Prepare("INSERT OR IGNORE INTO topics(name) VALUES (?);")
	if err != nil {
		db.Close()
		return nil, err
	}

	s2, err := db.Prepare(`
INSERT INTO received (timestamp, topic_id, message)
VALUES (?, (SELECT id FROM topics WHERE name = ?), ?);
`)
	if err != nil {
		s1.Close()
		db.Close()
		return nil, err
	}

	return &sqlogger{db: db, insertTopic: s1, insertReceived: s2}, nil
}

func checkSqlogger(L *lua.LState, index int) *sqlogger {
	ud := L.CheckUserData(index)
	if v, ok := ud.Value.(*sqlogger); ok {
		return v
	}
	L.ArgError(index, "sqlogger expected")
	return nil
}

func luaSqloggerNew(L *lua.LState, agent *fullMqttAgent) int {
	arg := L.CheckString(1)
	if logger, found := agent.loggers[arg]; found {
		ud := L.NewUserData()
		ud.Value = logger
		L.SetMetatable(ud, L.GetTypeMetatable("sqlogger"))
		L.Push(ud)
		return 1
	} else if logger, found := agent.oldLoggers[arg]; found {
		agent.loggers[arg] = logger
		ud := L.NewUserData()
		ud.Value = logger
		L.SetMetatable(ud, L.GetTypeMetatable("sqlogger"))
		L.Push(ud)
		return 1
	} else if logger, err := connect(arg); err != nil {
		log.Println(err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	} else {
		agent.loggers[arg] = logger
		ud := L.NewUserData()
		ud.Value = logger
		L.SetMetatable(ud, L.GetTypeMetatable("sqlogger"))
		L.Push(ud)
		return 1
	}
}

func luaSqloggerReceived(L *lua.LState) int {
	logger := checkSqlogger(L, 1)
	message := L.CheckString(2)
	topic := L.CheckString(3)
	timestamp := L.OptNumber(4, lua.LNumber(time.Now().UnixMicro())*1.0e-6)

	logger.Received(&mqttagent.MqttMessage{
		Timestamp: float64(timestamp),
		ClientId:  -1,
		Message:   []byte(message),
		Topic:     []byte(topic),
	})
	return 0
}

var luaSqloggerMethods = map[string]lua.LGFunction{
	"received": luaSqloggerReceived,
}

func main() {
	var agent fullMqttAgent

	main_script := "mqttagent.lua"
	if len(os.Args) > 1 {
		main_script = os.Args[1]
	}

	mqttagent.Run(&agent, main_script, 10)

	os.Exit(0)
}
