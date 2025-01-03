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
	"os"

	"instinctive.eu/go/mqttagent"
	"github.com/yuin/gopher-lua"
)

type liteMqttAgent struct{}

func (agent liteMqttAgent) Setup(L *lua.LState)                           {}
func (agent liteMqttAgent) Log(L *lua.LState, msg *mqttagent.MqttMessage) {}
func (agent liteMqttAgent) Teardown(L *lua.LState)                        {}

func main() {
	var agent liteMqttAgent

	main_script := "mqttagent.lua"
	if len(os.Args) > 1 {
		main_script = os.Args[1]
	}

	mqttagent.Run(agent, main_script)

	os.Exit(0)
}
