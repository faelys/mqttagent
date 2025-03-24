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

package mqttagent

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-mqtt/mqtt"
	"github.com/yuin/gluamapper"
	"github.com/yuin/gopher-lua"
)

type MqttAgent interface {
	Setup(L *lua.LState)
	Log(L *lua.LState, msg *MqttMessage)
	Teardown(L *lua.LState)
}

type MqttReloadingAgent interface {
	Setup(L *lua.LState)
	Log(L *lua.LState, msg *MqttMessage)
	ReloadBegin(oldL, newL *lua.LState)
	ReloadAbort(oldL, newL *lua.LState)
	ReloadEnd(oldL, newL *lua.LState)
	Teardown(L *lua.LState)
}

type MqttMessage struct {
	Timestamp float64
	ClientId  int
	Topic     []byte
	Message   []byte
}

func Run(agent MqttAgent, main_script string, capacity int) {
	fromMqtt := make(chan MqttMessage, capacity)

	L := lua.NewState()
	defer L.Close()

	agent.Setup(L)
	defer agent.Teardown(L)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "<unknown>"
	}

	idString := fmt.Sprintf("mqttagent-%s-%d", hostname, os.Getpid())
	registerMqttClientType(L)
	registerTimerType(L)
	registerState(L, idString, fromMqtt)
	defer cleanupClients(L)

	if err := L.DoFile(main_script); err != nil {
		panic(err)
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	log.Println(idString, "started")

	for {
		select {
		case msg, ok := <-fromMqtt:

			if !ok {
				log.Println("fromMqtt is closed")
				break
			}

			processMsg(L, agent, &msg)

		case <-timer.C:
		}

		runTimers(L, timer)

		if stateReloadRequested(L) {
			L = reload(L, agent, main_script)
			runTimers(L, timer)
			stateRequestReload(L, lua.LNil)
		}

		if tableIsEmpty(stateCnxTable(L)) && tableIsEmpty(stateTimerTable(L)) {
			break
		}
	}

	log.Println(idString, "finished")
}

func cleanupClients(L *lua.LState) {
	cnxTbl := stateCnxTable(L)
	if cnxTbl == nil {
		return
	}

	L.ForEach(cnxTbl, func(key, value lua.LValue) {
		cnx := value.(*lua.LTable)
		client := L.RawGetInt(cnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)
		if err := client.Disconnect(nil); err != nil {
			log.Printf("cleanup client %s: %v", lua.LVAsString(key), err)
		}
	})
}

func dispatchMsg(L *lua.LState, msg *MqttMessage, cnx, key, value lua.LValue) {
	skey, ok := key.(lua.LString)
	topic := string(msg.Topic)

	if ok && match(topic, string(skey)) {
		err := L.CallByParam(lua.P{Fn: value, NRet: 0, Protect: true},
			cnx,
			lua.LString(string(msg.Message)),
			lua.LString(topic),
			lua.LNumber(msg.Timestamp))
		if err != nil {
			panic(err)
		}
	}
}

func matchSliced(actual, filter []string) bool {
	if len(filter) == 0 {
		return len(actual) == 0
	}

	if filter[0] == "#" {
		if len(filter) == 1 {
			return true
		}

		for i := range actual {
			if matchSliced(actual[i:], filter[1:]) {
				return true
			}
		}

		return false
	}

	if len(actual) > 0 && (filter[0] == "+" || filter[0] == actual[0]) {
		return matchSliced(actual[1:], filter[1:])
	}

	return false
}

func match(actual, filter string) bool {
	return matchSliced(strings.Split(actual, "/"), strings.Split(filter, "/"))
}

func tableIsEmpty(t *lua.LTable) bool {
	key, _ := t.Next(lua.LNil)
	return key == lua.LNil
}

func processMsg(L *lua.LState, agent MqttAgent, msg *MqttMessage) {
	agent.Log(L, msg)

	cnx := L.RawGetInt(stateCnxTable(L), msg.ClientId).(*lua.LTable)
	subTbl := L.RawGetInt(cnx, keySubTable).(*lua.LTable)
	L.ForEach(subTbl, func(key, value lua.LValue) { dispatchMsg(L, msg, cnx, key, value) })

	if tableIsEmpty(subTbl) {
		client := L.RawGetInt(cnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)
		if err := client.Disconnect(nil); err != nil {
			log.Println("disconnect empty client:", err)
		}
		L.RawSetInt(stateCnxTable(L), msg.ClientId, lua.LNil)
	}
}

func mqttRead(client *mqtt.Client, toLua chan<- MqttMessage, id int) {
	var big *mqtt.BigMessage

	for {
		message, topic, err := client.ReadSlices()
		t := float64(time.Now().UnixMicro()) * 1.0e-6

		switch {
		case err == nil:
			toLua <- MqttMessage{Timestamp: t, ClientId: id, Topic: dup(topic), Message: dup(message)}

		case errors.As(err, &big):
			data, err := big.ReadAll()
			if err != nil {
				log.Println("mqttRead big message:", err)
			} else {
				toLua <- MqttMessage{Timestamp: t, ClientId: id, Topic: dup(topic), Message: data}
			}

		case errors.Is(err, mqtt.ErrClosed):
			log.Println("mqttRead finishing:", err)
			return

		case mqtt.IsConnectionRefused(err):
			log.Println("mqttRead connection refused:", err)
			time.Sleep(15 * time.Minute)

		default:
			log.Println("mqttRead:", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func reload(oldL *lua.LState, agent MqttAgent, main_script string) *lua.LState {
	log.Println("Reloading", main_script)
	reloader, isReloader := agent.(MqttReloadingAgent)

	newL := lua.NewState()

	if isReloader {
		reloader.ReloadBegin(oldL, newL)
	} else {
		agent.Setup(newL)
	}

	registerMqttClientType(newL)
	registerTimerType(newL)

	stateReloadBegin(oldL, newL)

	if err := newL.DoFile(main_script); err != nil {
		log.Println("Reload failed:", err)
		stateReloadAbort(oldL, newL)
		if isReloader {
			reloader.ReloadAbort(oldL, newL)
		} else {
			agent.Teardown(newL)
		}
		newL.Close()
		return oldL
	} else {
		stateReloadEnd(oldL, newL)
		if isReloader {
			reloader.ReloadEnd(oldL, newL)
		} else {
			agent.Teardown(oldL)
		}
		oldL.Close()
		log.Println("Reload successful")
		return newL
	}
}

func dup(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}

func newUserData(L *lua.LState, v interface{}) *lua.LUserData {
	res := L.NewUserData()
	res.Value = v
	return res
}

/********** State Object in the Lua Interpreter **********/

const luaStateName = "_mqttagent"
const keyChanToLua = 1
const keyClientPrefix = 2
const keyClientNextId = 3
const keyCfgMap = 4
const keyCnxTable = 5
const keyTimerTable = 6
const keyReloadRequest = 7
const keyOldCfgMap = 8

func registerState(L *lua.LState, clientPrefix string, toLua chan<- MqttMessage) {
	st := L.NewTable()
	L.RawSetInt(st, keyChanToLua, newUserData(L, toLua))
	L.RawSetInt(st, keyClientPrefix, lua.LString(clientPrefix))
	L.RawSetInt(st, keyClientNextId, lua.LNumber(1))
	L.RawSetInt(st, keyCfgMap, newUserData(L, make(mqttConfigMap)))
	L.RawSetInt(st, keyCnxTable, L.NewTable())
	L.RawSetInt(st, keyTimerTable, L.NewTable())
	L.SetGlobal(luaStateName, st)
	L.SetGlobal("reload", L.NewFunction(requestReload))
}

func stateReloadBegin(oldL, newL *lua.LState) {
	oldSt := oldL.GetGlobal(luaStateName).(*lua.LTable)
	toLua := oldL.RawGetInt(oldSt, keyChanToLua).(*lua.LUserData).Value.(chan<- MqttMessage)
	clientPrefix := oldL.RawGetInt(oldSt, keyClientPrefix)
	nextId := oldL.RawGetInt(oldSt, keyClientNextId)
	cfgMap := oldL.RawGetInt(oldSt, keyCfgMap).(*lua.LUserData).Value.(mqttConfigMap)

	st := newL.NewTable()
	newL.RawSetInt(st, keyChanToLua, newUserData(newL, toLua))
	newL.RawSetInt(st, keyClientPrefix, clientPrefix)
	newL.RawSetInt(st, keyClientNextId, nextId)
	newL.RawSetInt(st, keyCfgMap, newUserData(newL, make(mqttConfigMap)))
	newL.RawSetInt(st, keyCnxTable, newL.NewTable())
	newL.RawSetInt(st, keyTimerTable, newL.NewTable())
	newL.RawSetInt(st, keyOldCfgMap, newUserData(newL, cfgMap))
	newL.SetGlobal(luaStateName, st)
	newL.SetGlobal("reload", newL.NewFunction(requestReload))
}

func stateReloadAbort(oldL, newL *lua.LState) {
	statePartialCleanup(newL, oldL)
}

func stateReloadEnd(oldL, newL *lua.LState) {
	statePartialCleanup(oldL, newL)
	newSt := newL.GetGlobal(luaStateName).(*lua.LTable)
	newL.RawSetInt(newSt, keyOldCfgMap, lua.LNil)
}

func statePartialCleanup(staleL, keptL *lua.LState) {
	staleCnxTable := stateCnxTable(staleL)
	keptCnxTable := stateCnxTable(keptL)

	staleL.ForEach(staleCnxTable, func(key, value lua.LValue) {
		clientId := int(key.(lua.LNumber))
		if keptL.RawGetInt(keptCnxTable, clientId) == lua.LNil {
			client := staleL.RawGetInt(value.(*lua.LTable), keyClient).(*lua.LUserData).Value.(*mqtt.Client)
			client.Close()
		}
	})

	keptL.ForEach(keptCnxTable, func(key, value lua.LValue) {
		clientId := int(key.(lua.LNumber))
		keptCnx := value.(*lua.LTable)
		if lua.LVIsFalse(keptL.RawGetInt(keptCnx, keyReused)) {
			return
		}

		client := keptL.RawGetInt(keptCnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)
		staleCnx := staleL.RawGetInt(staleCnxTable, clientId).(*lua.LTable)

		if client != staleL.RawGetInt(staleCnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client) {
			panic("This shouldn't happen")
		}

		keptSub := keptL.RawGetInt(keptCnx, keySubTable).(*lua.LTable)
		staleSub := staleL.RawGetInt(staleCnx, keySubTable).(*lua.LTable)

		staleL.ForEach(staleSub, func(key, _ lua.LValue) {
			topic := string(key.(lua.LString))
			if keptL.GetField(keptSub, topic) == lua.LNil {
				log.Println("Unsubscribing from stale topic", topic)
				if err := client.Unsubscribe(nil, topic); err != nil {
					log.Println("Failed to unsubscribe:", err)
				}
			}
		})

		keptL.ForEach(keptSub, func(key, _ lua.LValue) {
			topic := string(key.(lua.LString))
			if staleL.GetField(staleSub, topic) == lua.LNil {
				log.Println("Subscribing to new topic", topic)
				if err := client.Subscribe(nil, topic); err != nil {
					log.Println("Failed to subscribe:", err)
				}
			}
		})

		keptL.RawSetInt(keptCnx, keyReused, lua.LNil)
	})
}

func stateValue(L *lua.LState, key int) lua.LValue {
	st := L.GetGlobal(luaStateName)
	return L.RawGetInt(st.(*lua.LTable), key)
}

func stateChanToLua(L *lua.LState) chan<- MqttMessage {
	ud := stateValue(L, keyChanToLua)
	return ud.(*lua.LUserData).Value.(chan<- MqttMessage)
}

func stateClientNextId(L *lua.LState) (int, string) {
	st := L.GetGlobal(luaStateName).(*lua.LTable)
	result := int(L.RawGetInt(st, keyClientNextId).(lua.LNumber))
	L.RawSetInt(st, keyClientNextId, lua.LNumber(result+1))
	prefix := lua.LVAsString(L.RawGetInt(st, keyClientPrefix))
	return result, fmt.Sprintf("%s-%d", prefix, result)
}

func stateCfgMap(L *lua.LState) mqttConfigMap {
	return stateValue(L, keyCfgMap).(*lua.LUserData).Value.(mqttConfigMap)
}

func stateCnxTable(L *lua.LState) *lua.LTable {
	return stateValue(L, keyCnxTable).(*lua.LTable)
}

func stateTimerTable(L *lua.LState) *lua.LTable {
	return stateValue(L, keyTimerTable).(*lua.LTable)
}

func stateReloadRequested(L *lua.LState) bool {
	return lua.LVAsBool(stateValue(L, keyReloadRequest))
}

func stateRequestReload(L *lua.LState, v lua.LValue) {
	st := L.GetGlobal(luaStateName).(*lua.LTable)
	L.RawSetInt(st, keyReloadRequest, v)
}

func stateOldCfgMap(L *lua.LState) mqttConfigMap {
	val := stateValue(L, keyOldCfgMap)
	if val == lua.LNil {
		return nil
	} else {
		return val.(*lua.LUserData).Value.(mqttConfigMap)
	}
}

func requestReload(L *lua.LState) int {
	stateRequestReload(L, lua.LTrue)
	return 0
}

/********** Lua Object for MQTT client **********/

const luaMqttClientTypeName = "mqttclient"
const keyClient = 1
const keySubTable = 2
const keyReused = 3

func registerMqttClientType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaMqttClientTypeName)
	L.SetGlobal(luaMqttClientTypeName, mt)
	L.SetField(mt, "new", L.NewFunction(newMqttClient))
	L.SetField(mt, "__call", L.NewFunction(luaPublish))
	L.SetField(mt, "__index", L.NewFunction(luaQuery))
	L.SetField(mt, "__newindex", L.NewFunction(luaSubscribe))
}

type mqttConfig struct {
	Connection     string
	TLS            bool
	PauseTimeout   string
	AtLeastOnceMax int
	ExactlyOnceMax int
	UserName       string
	Password       string
	Will           struct {
		Topic       string
		Message     string
		Retain      bool
		AtLeastOnce bool
		ExactlyOnce bool
	}
	KeepAlive    uint16
	CleanSession bool
}

type mqttClientEntry struct {
	client *mqtt.Client
	id     int
}

type mqttConfigMap map[mqttConfig]mqttClientEntry

func mqttConfigBytes(src string) []byte {
	if src == "" {
		return nil
	} else {
		return []byte(src)
	}
}

func newClient(config *mqttConfig, id string) (*mqtt.Client, error) {
	pto, err := time.ParseDuration(config.PauseTimeout)
	if err != nil {
		pto = time.Second
	}

	var dialer mqtt.Dialer

	if config.TLS {
		dialer = mqtt.NewTLSDialer("tcp", config.Connection, nil)
	} else {
		dialer = mqtt.NewDialer("tcp", config.Connection)
	}

	processed_cfg := mqtt.Config{
		Dialer:         dialer,
		PauseTimeout:   pto,
		AtLeastOnceMax: config.AtLeastOnceMax,
		ExactlyOnceMax: config.ExactlyOnceMax,
		UserName:       config.UserName,
		Password:       mqttConfigBytes(config.Password),
		Will: struct {
			Topic       string
			Message     []byte
			Retain      bool
			AtLeastOnce bool
			ExactlyOnce bool
		}{
			Topic:       config.Will.Topic,
			Message:     mqttConfigBytes(config.Will.Message),
			Retain:      config.Will.Retain,
			AtLeastOnce: config.Will.AtLeastOnce,
			ExactlyOnce: config.Will.ExactlyOnce,
		},
		KeepAlive:    config.KeepAlive,
		CleanSession: config.CleanSession,
	}

	return mqtt.VolatileSession(id, &processed_cfg)
}

func registerClient(L *lua.LState, id int, client *mqtt.Client, reused bool) lua.LValue {
	res := L.NewTable()
	L.RawSetInt(res, keyClient, newUserData(L, client))
	L.RawSetInt(res, keySubTable, L.NewTable())
	if reused {
		L.RawSetInt(res, keyReused, lua.LTrue)
	}
	L.SetMetatable(res, L.GetTypeMetatable(luaMqttClientTypeName))
	L.RawSetInt(stateCnxTable(L), id, res)
	return res
}

func newMqttClient(L *lua.LState) int {
	var config mqttConfig
	if err := gluamapper.Map(L.CheckTable(1), &config); err != nil {
		log.Println("newMqttClient:", err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	cfgMap := stateCfgMap(L)

	if cfg, found := cfgMap[config]; found {
		res := L.RawGetInt(stateCnxTable(L), cfg.id)
		tbl := res.(*lua.LTable)
		if L.RawGetInt(tbl, keyClient).(*lua.LUserData).Value.(*mqtt.Client) != cfg.client {
			panic("Inconsistent configuration table")
		}

		L.Push(res)
		return 1
	}

	if oldCfgMap := stateOldCfgMap(L); oldCfgMap != nil {
		if cfg, found := oldCfgMap[config]; found {
			cfgMap[config] = cfg
			L.Push(registerClient(L, cfg.id, cfg.client, true))
			return 1
		}
	}

	id, idString := stateClientNextId(L)
	client, err := newClient(&config, idString)
	if err != nil {
		log.Println("newMqttClient:", err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	go mqttRead(client, stateChanToLua(L), id)

	cfgMap[config] = mqttClientEntry{id: id, client: client}

	L.Push(registerClient(L, id, client, false))
	return 1
}

func luaPublish(L *lua.LState) int {
	cnx := L.CheckTable(1)
	client := L.RawGetInt(cnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)

	if L.GetTop() == 1 {
		if err := client.Ping(nil); err != nil {
			log.Println("luaPing:", err)
			L.Push(lua.LNil)
			L.Push(lua.LString(err.Error()))
			return 2
		} else {
			L.Push(lua.LTrue)
			return 1
		}
	}

	message := L.CheckString(2)
	topic := L.CheckString(3)

	if err := client.Publish(nil, []byte(message), topic); err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	} else {
		L.Push(lua.LTrue)
		return 1
	}
}

func luaQuery(L *lua.LState) int {
	cnx := L.CheckTable(1)
	topic := L.CheckString(2)
	subTbl := L.RawGetInt(cnx, keySubTable).(*lua.LTable)
	L.Push(L.GetField(subTbl, topic))
	return 1
}

func luaSubscribe(L *lua.LState) int {
	var err error
	cnx := L.CheckTable(1)
	topic := L.CheckString(2)
	callback := L.OptFunction(3, nil)
	client := L.RawGetInt(cnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)
	tbl := L.RawGetInt(cnx, keySubTable).(*lua.LTable)

	_, is_new := L.GetField(tbl, topic).(*lua.LNilType)

	if lua.LVIsFalse(L.RawGetInt(cnx, keyReused)) {
		if callback == nil {
			err = client.Unsubscribe(nil, topic)
		} else if is_new {
			err = client.Subscribe(nil, topic)
		}
	}

	if err != nil {
		log.Println("luaSubscribe:", err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	} else {
		if callback == nil {
			if is_new {
				log.Printf("Not subscribed to %q", topic)
			} else {
				log.Printf("Unsubscribed from %q", topic)
			}
			L.SetField(tbl, topic, lua.LNil)
		} else {
			if is_new {
				log.Printf("Subscribed to %q", topic)
			} else {
				log.Printf("Updating subscription to %q", topic)
			}
			L.SetField(tbl, topic, callback)
		}

		L.Push(lua.LTrue)
		return 1
	}
}

/********** Lua Object for timers **********/

const luaTimerTypeName = "timer"

func registerTimerType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaTimerTypeName)
	L.SetGlobal(luaTimerTypeName, mt)
	L.SetField(mt, "new", L.NewFunction(newTimer))
	L.SetField(mt, "schedule", L.NewFunction(timerSchedule))
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), timerMethods))
}

func newTimer(L *lua.LState) int {
	atTime := L.Get(1)
	cb := L.CheckFunction(2)
	L.Pop(2)
	L.SetMetatable(cb, L.GetTypeMetatable(luaTimerTypeName))
	L.Push(cb)
	L.Push(atTime)
	return timerSchedule(L)
}

var timerMethods = map[string]lua.LGFunction{
	"cancel":   timerCancel,
	"schedule": timerSchedule,
}

func timerCancel(L *lua.LState) int {
	timer := L.CheckFunction(1)
	L.RawSet(stateTimerTable(L), timer, lua.LNil)
	return 0
}

func timerSchedule(L *lua.LState) int {
	timer := L.CheckFunction(1)
	atTime := lua.LNil
	if L.Get(2) != lua.LNil {
		atTime = L.CheckNumber(2)
	}

	L.RawSet(stateTimerTable(L), timer, atTime)
	return 0
}

func toTime(lsec lua.LNumber) time.Time {
	fsec := float64(lsec)
	sec := int64(fsec)
	nsec := int64((fsec - float64(sec)) * 1.0e9)

	return time.Unix(sec, nsec)
}

func runTimers(L *lua.LState, parentTimer *time.Timer) {
	hasNext := false
	var nextTime time.Time

	now := time.Now()
	timers := stateTimerTable(L)

	timer, luaT := timers.Next(lua.LNil)
	for timer != lua.LNil {
		t := toTime(luaT.(lua.LNumber))
		if t.Compare(now) <= 0 {
			L.RawSet(timers, timer, lua.LNil)
			err := L.CallByParam(lua.P{Fn: timer, NRet: 0, Protect: true}, timer, luaT)
			if err != nil {
				panic(err)
			}
			timer = lua.LNil
			hasNext = false
		} else if !hasNext || t.Compare(nextTime) < 0 {
			hasNext = true
			nextTime = t
		}

		timer, luaT = timers.Next(timer)
	}

	if hasNext {
		parentTimer.Reset(time.Until(nextTime))
	} else {
		parentTimer.Stop()
	}
}
