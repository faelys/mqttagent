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

type MqttMessage struct {
	Timestamp float64
	ClientId  int
	Topic     []byte
	Message   []byte
}

func Run(agent MqttAgent, main_script string) {
	fromMqtt := make(chan MqttMessage)

	L := lua.NewState()
	defer L.Close()

	agent.Setup(L)
	defer agent.Teardown(L)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "<unknown>"
	}

	registerMqttClientType(L)
	registerState(L, fmt.Sprintf("mqttagent-%s-%d", hostname, os.Getpid()), &fromMqtt)
	defer cleanupClients(L)

	if err := L.DoFile(main_script); err != nil {
		panic(err)
	}

	for {
		msg, ok := <-fromMqtt

		if !ok {
			break
		}

		agent.Log(L, &msg)

		cnx := L.RawGetInt(stateCnxTable(L), msg.ClientId).(*lua.LTable)
		subTbl := L.RawGetInt(cnx, keySubTable).(*lua.LTable)
		L.ForEach(subTbl, func(key, value lua.LValue) { dispatchMsg(L, &msg, cnx, key, value) })

		if key, _ := subTbl.Next(lua.LNil); key == lua.LNil {
			client := L.RawGetInt(cnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)
			client.Disconnect(nil)
			L.RawSetInt(stateCnxTable(L), msg.ClientId, lua.LNil)
		}

		if stateCnxTable(L).Len() == 0 {
			break
		}
	}
}

func cleanupClients(L *lua.LState) {
	cnxTbl := stateCnxTable(L)
	if cnxTbl == nil {
		return
	}

	L.ForEach(cnxTbl, func(key, value lua.LValue) {
		cnx := value.(*lua.LTable)
		client := L.RawGetInt(cnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)
		client.Disconnect(nil)
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

func mqttRead(client *mqtt.Client, toLua chan MqttMessage, id int) error {
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
				log.Println(err)
			} else {
				toLua <- MqttMessage{Timestamp: t, ClientId: id, Topic: dup(topic), Message: data}
			}
		default:
			log.Println(err)
			return err
		}
	}
}

func dup(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}

/********** State Object in the Lua Interpreter **********/

const luaStateName = "_mqttagent"
const keyChanToLua = 1
const keyClientPrefix = 2
const keyCnxTable = 3

func registerState(L *lua.LState, clientPrefix string, toLua *chan MqttMessage) {
	ud := L.NewUserData()
	ud.Value = toLua

	st := L.NewTable()
	L.RawSetInt(st, keyChanToLua, ud)
	L.RawSetInt(st, keyClientPrefix, lua.LString(clientPrefix))
	L.RawSetInt(st, keyCnxTable, L.NewTable())
	L.SetGlobal(luaStateName, st)
}

func stateValue(L *lua.LState, key int) lua.LValue {
	st := L.GetGlobal(luaStateName)
	return L.RawGetInt(st.(*lua.LTable), key)
}

func stateChanToLua(L *lua.LState) *chan MqttMessage {
	ud := stateValue(L, keyChanToLua)
	return ud.(*lua.LUserData).Value.(*chan MqttMessage)
}

func stateClientPrefix(L *lua.LState) string {
	return lua.LVAsString(stateValue(L, keyClientPrefix))
}

func stateCnxTable(L *lua.LState) *lua.LTable {
	return stateValue(L, keyCnxTable).(*lua.LTable)
}

/********** Lua Object for MQTT client **********/

const luaMqttClientTypeName = "mqttclient"
const keyClient = 1
const keySubTable = 2

func registerMqttClientType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaMqttClientTypeName)
	L.SetGlobal(luaMqttClientTypeName, mt)
	L.SetField(mt, "new", L.NewFunction(newMqttClient))
	L.SetField(mt, "__gc", L.NewFunction(deleteMqttClient))
	L.SetField(mt, "__call", L.NewFunction(luaPublish))
	L.SetField(mt, "__index", L.NewFunction(luaQuery))
	L.SetField(mt, "__newindex", L.NewFunction(luaSubscribe))
}

type mqttConfig struct {
	Connection     string
	PauseTimeout   string
	AtLeastOnceMax int
	ExactlyOnceMax int
	UserName       string
	Password       []byte
	Will           struct {
		Topic       string
		Message     []byte
		Retain      bool
		AtLeastOnce bool
		ExactlyOnce bool
	}
	KeepAlive    uint16
	CleanSession bool
}

func newClient(L *lua.LState, id string) (*mqtt.Client, error) {
	var config mqttConfig
	if err := gluamapper.Map(L.CheckTable(1), &config); err != nil {
		return nil, err
	}

	pto, err := time.ParseDuration(config.PauseTimeout)
	if err != nil {
		pto = time.Second
	}

	processed_cfg := mqtt.Config{
		Dialer:         mqtt.NewDialer("tcp", config.Connection),
		PauseTimeout:   pto,
		AtLeastOnceMax: config.AtLeastOnceMax,
		ExactlyOnceMax: config.ExactlyOnceMax,
		UserName:       config.UserName,
		Password:       config.Password,
		Will: struct {
			Topic       string
			Message     []byte
			Retain      bool
			AtLeastOnce bool
			ExactlyOnce bool
		}{
			Topic:       config.Will.Topic,
			Message:     config.Will.Message,
			Retain:      config.Will.Retain,
			AtLeastOnce: config.Will.AtLeastOnce,
			ExactlyOnce: config.Will.ExactlyOnce,
		},
		KeepAlive:    config.KeepAlive,
		CleanSession: config.CleanSession,
	}

	return mqtt.VolatileSession(id, &processed_cfg)
}

func newMqttClient(L *lua.LState) int {
	id := stateCnxTable(L).Len() + 1
	idString := fmt.Sprintf("%s-%d", stateClientPrefix(L), id)

	client, err := newClient(L, idString)
	if err != nil {
		log.Println(err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	go mqttRead(client, *stateChanToLua(L), id)

	ud := L.NewUserData()
	ud.Value = client

	res := L.NewTable()
	L.RawSetInt(res, keyClient, ud)
	L.RawSetInt(res, keySubTable, L.NewTable())
	L.SetMetatable(res, L.GetTypeMetatable(luaMqttClientTypeName))
	L.RawSetInt(stateCnxTable(L), id, res)
	L.Push(res)
	return 1
}

func deleteMqttClient(L *lua.LState) int {
	log.Println("deleteMqttClient: TODO")
	return 0
}

func luaPublish(L *lua.LState) int {
	cnx := L.CheckTable(1)
	message := L.CheckString(2)
	topic := L.CheckString(3)
	client := L.RawGetInt(cnx, keyClient).(*lua.LUserData).Value.(*mqtt.Client)

	err := client.Publish(nil, []byte(message), topic)

	if err != nil {
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

	if callback == nil {
		err = client.Unsubscribe(nil, topic)
	} else {
		err = client.Subscribe(nil, topic)
	}

	if err != nil {
		log.Println(err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	} else {
		tbl := L.RawGetInt(cnx, keySubTable).(*lua.LTable)

		if callback == nil {
			L.SetField(tbl, topic, lua.LNil)
		} else {
			L.SetField(tbl, topic, callback)
		}

		L.Push(lua.LTrue)
		return 1
	}
}
