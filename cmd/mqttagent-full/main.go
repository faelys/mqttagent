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

	"instinctive.eu/go/mqttagent"
	_ "github.com/glebarez/go-sqlite"
	luajson "github.com/layeh/gopher-json"
	"github.com/yuin/gopher-lua"
)

type fullMqttAgent struct {
	db             *sql.DB
	insertTopic    *sql.Stmt
	insertReceived *sql.Stmt
}

func (agent *fullMqttAgent) Setup(L *lua.LState) {
	luajson.Preload(L)

	L.SetGlobal("sqlog", L.NewFunction(func(L *lua.LState) int {
		arg := L.CheckString(1)
		if err := agent.connect(arg); err != nil {
			log.Println(err)
			L.Push(lua.LNil)
			L.Push(lua.LString(err.Error()))
			return 2
		} else {
			L.Push(lua.LTrue)
			return 1
		}
	}))
}

func (agent *fullMqttAgent) Log(L *lua.LState, msg *mqttagent.MqttMessage) {
	if agent.insertTopic == nil || agent.insertReceived == nil {
		return
	}

	if _, err := agent.insertTopic.Exec(msg.Topic); err != nil {
		log.Println(err)
		return
	}

	if _, err := agent.insertReceived.Exec((msg.Timestamp/86400.0)+2440587.5, msg.Topic, msg.Message); err != nil {
		log.Println(err)
		return
	}
}

func (agent *fullMqttAgent) Teardown(L *lua.LState) {
	if agent.insertReceived != nil {
		agent.insertReceived.Close()
	}

	if agent.insertTopic != nil {
		agent.insertTopic.Close()
	}

	if agent.db != nil {
		agent.db.Close()
	}
}

func (agent *fullMqttAgent) connect(connectionString string) error {
	if agent.insertReceived != nil {
		agent.insertReceived.Close()
		agent.insertReceived = nil
	}

	if agent.insertTopic != nil {
		agent.insertTopic.Close()
		agent.insertTopic = nil
	}

	if agent.db != nil {
		agent.db.Close()
		agent.db = nil
	}

	db, err := sql.Open("sqlite", connectionString)
	if err != nil {
		return err
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
			return err
		}
	}

	s1, err := db.Prepare("INSERT OR IGNORE INTO topics(name) VALUES (?);")
	if err != nil {
		db.Close()
		return err
	}

	s2, err := db.Prepare(`
INSERT INTO received (timestamp, topic_id, message)
VALUES (?, (SELECT id FROM topics WHERE name = ?), ?);
`)
	if err != nil {
		s1.Close()
		db.Close()
		return err
	}

	agent.db = db
	agent.insertTopic = s1
	agent.insertReceived = s2
	return nil
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
