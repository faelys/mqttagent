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
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cjoudrey/gluahttp"
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
	L.PreloadModule("http", gluahttp.NewHttpModule(&http.Client{}).Loader)
	L.SetGlobal("urlencode", L.NewFunction(luaUrlEncode))
	setBuildInfo(L, "buildinfo")
	setVersion(L, "version")
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

func luaUrlEncode(L *lua.LState) int {
	tbl := L.CheckTable(1)
	result := make([]string, 0, tbl.Len())

	L.ForEach(tbl, func(key, value lua.LValue) {
		skey := url.QueryEscape(lua.LVAsString(key))
		sval := url.QueryEscape(lua.LVAsString(value))
		result = append(result, skey+"="+sval)
	})

	L.Push(lua.LString(strings.Join(result, "&")))
	return 1
}

type sqlogger struct {
	db             *sql.DB
	insertReceived *sql.Stmt
	insertSent     *sql.Stmt
}

func logErr(context string, err error) {
	if err != nil {
		log.Println(context, err)
	}
}

func (logger *sqlogger) Close() {
	if logger == nil {
		return
	}

	if logger.insertReceived != nil {
		logErr("Close insertReceived:", logger.insertReceived.Close())
	}

	if logger.insertSent != nil {
		logErr("Close insertSend:", logger.insertSent.Close())
	}

	if logger.db != nil {
		logErr("Close DB:", logger.db.Close())
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
		"CREATE UNIQUE INDEX IF NOT EXISTS i_topics ON topics(name);",
		"CREATE TABLE IF NOT EXISTS received" +
			"(timestamp REAL NOT NULL," +
			" topic_id INTEGER NOT NULL," +
			" message TEXT NOT NULL," +
			" FOREIGN KEY (topic_id) REFERENCES topics (id));",
		"CREATE TABLE IF NOT EXISTS sent" +
			"(timestamp REAL NOT NULL," +
			" topic_id INTEGER NOT NULL," +
			" message TEXT NOT NULL," +
			" FOREIGN KEY (topic_id) REFERENCES topics (id));",
		"CREATE INDEX IF NOT EXISTS i_rtime ON received(timestamp);",
		"CREATE INDEX IF NOT EXISTS i_rtopicid ON received(topic_id);",
		"CREATE INDEX IF NOT EXISTS i_stime ON received(timestamp);",
		"CREATE INDEX IF NOT EXISTS i_stopicid ON received(topic_id);",
		"CREATE VIEW IF NOT EXISTS receivedf" +
			"(timestamp,topic,message)" +
			" AS SELECT datetime(timestamp),topics.name,message" +
			" FROM received LEFT OUTER JOIN topics" +
			" ON topics.id = topic_id;",
		"CREATE VIEW IF NOT EXISTS sentf" +
			"(timestamp,topic,message)" +
			" AS SELECT datetime(timestamp),topics.name,message" +
			" FROM sent LEFT OUTER JOIN topics" +
			" ON topics.id = topic_id;",
		"CREATE TRIGGER IF NOT EXISTS insert_received" +
			" INSTEAD OF INSERT ON receivedf BEGIN" +
			" INSERT INTO topics(name)" +
			" SELECT NEW.topic WHERE NOT EXISTS" +
			" (SELECT 1 FROM topics WHERE name = NEW.topic);" +
			" INSERT INTO received(timestamp,topic_id,message)" +
			" VALUES (NEW.timestamp," +
			" (SELECT id FROM topics WHERE name = NEW.topic)," +
			" NEW.message); END;",
		"CREATE TRIGGER IF NOT EXISTS insert_sent" +
			" INSTEAD OF INSERT ON sentf BEGIN" +
			" INSERT INTO topics(name)" +
			" SELECT NEW.topic WHERE NOT EXISTS" +
			" (SELECT 1 FROM topics WHERE name = NEW.topic);" +
			" INSERT INTO sent(timestamp,topic_id,message)" +
			" VALUES (NEW.timestamp," +
			" (SELECT id FROM topics WHERE name = NEW.topic)," +
			" NEW.message); END;",
	} {
		if _, err = db.Exec(cmd); err != nil {
			logErr("Close DB:", db.Close())
			return nil, err
		}
	}

	s1, err := db.Prepare("INSERT INTO receivedf(timestamp,topic,message)" +
		" VALUES (?,?,?);")
	if err != nil {
		logErr("Close DB:", db.Close())
		return nil, err
	}

	s2, err := db.Prepare("INSERT INTO sentf(timestamp,topic,message)" +
		" VALUES (?,?,?);")
	if err != nil {
		logErr("Close insertReceived:", s1.Close())
		logErr("Close DB:", db.Close())
		return nil, err
	}

	return &sqlogger{db: db, insertReceived: s1, insertSent: s2}, nil
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

func luaSqloggerInsert(L *lua.LState, stmt *sql.Stmt) int {
	message := L.CheckString(2)
	topic := L.CheckString(3)
	timestamp := L.OptNumber(4, lua.LNumber(time.Now().UnixMicro())*1.0e-6)
	julian := (float64(timestamp) / 86400.0) + 2440587.5

	if _, err := stmt.Exec(julian, topic, message); err != nil {
		log.Println(err)
	}

	return 0
}

func luaSqloggerReceived(L *lua.LState) int {
	logger := checkSqlogger(L, 1)
	return luaSqloggerInsert(L, logger.insertReceived)
}

func luaSqloggerSent(L *lua.LState) int {
	logger := checkSqlogger(L, 1)
	return luaSqloggerInsert(L, logger.insertSent)
}

var luaSqloggerMethods = map[string]lua.LGFunction{
	"received": luaSqloggerReceived,
	"sent":     luaSqloggerSent,
}

func setBuildInfo(L *lua.LState, name string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	L.SetGlobal(name, luaBuildInfo(L, info))
}

func setVersion(L *lua.LState, name string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	version := info.Main.Version

	if version == "(devel)" {
		vcs := ""
		rev := ""
		dirty := ""
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs":
				vcs = setting.Value + "-"
			case "vcs.revision":
				rev = setting.Value[0:8]
			case "vcs.modified":
				if setting.Value == "true" {
					dirty = "*"
				}
			}
		}

		if rev != "" {
			version = vcs + rev + dirty
		}
	}

	L.SetGlobal(name, lua.LString(version))
}

func luaBuildInfo(L *lua.LState, info *debug.BuildInfo) lua.LValue {
	tbl := L.NewTable()
	tbl.RawSetString("go_version", lua.LString(info.GoVersion))
	tbl.RawSetString("path", lua.LString(info.Path))
	tbl.RawSetString("main", luaModule(L, &info.Main))
	tbl.RawSetString("deps", luaModules(L, info.Deps))
	tbl.RawSetString("settings", luaSettings(L, info.Settings))
	return tbl
}

func luaModule(L *lua.LState, module *debug.Module) lua.LValue {
	tbl := L.NewTable()
	tbl.RawSetString("path", lua.LString(module.Path))
	tbl.RawSetString("version", lua.LString(module.Version))
	tbl.RawSetString("sum", lua.LString(module.Sum))

	if module.Replace != nil {
		tbl.RawSetString("replace", luaModule(L, module.Replace))
	}
	return tbl
}

func luaModules(L *lua.LState, modules []*debug.Module) lua.LValue {
	tbl := L.NewTable()

	for index, module := range modules {
		tbl.RawSetInt(index+1, luaModule(L, module))
	}

	return tbl
}

func luaSettings(L *lua.LState, settings []debug.BuildSetting) lua.LValue {
	tbl := L.NewTable()

	for _, setting := range settings {
		tbl.RawSetString(setting.Key, lua.LString(setting.Value))
	}

	return tbl
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
