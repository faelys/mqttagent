# mqttagent

[![Casual Maintenance Intended](https://casuallymaintained.tech/badge.svg)](https://casuallymaintained.tech/)

This is a daemon MQTT client which runs Lua callbacks on received messages.

It started with a backup graph that did not match the connectivity graph,
so some data had to be pushed while other had to be pulled.
So I needed machine-to-machine signalling to pull data from a source after it
has been pushed completely.

I thought of MQTT for the signalling, and a kind of
cron-but-for-MQTT-messages-instead-of-time to run actions.
But then I imagined writing a parser for a crontab-like configuration file,
and then writing such a configuration file, so I reconsidered my life choices.

It turns out that a Lua script is much easier for development (thanks to
existing interpreters), for setup (thanks to a friendlier language), and
for maintenance (thanks to basic logic not being scattered across a lot
of small shell scripts).

## Manual

### Commands

The provided commands provide varying levels of extra primitives bound
to the Lua environment.

For now, the generated commands run `mqttagent.lua` in the current
directory and stay in the foreground.

Once the script is initially run, the program runs until all connections
and all timers are finished (or the script calls `os.exit`).

### MQTT client creation

```lua
client = mqttclient.new(config)
```

It creates a new MQTT client, using a configuration table which
is deserialized into [a `go-mqtt/mqtt.Config`
structure](https://pkg.go.dev/github.com/go-mqtt/mqtt#Config),
with the extra field `connection` used to make the `Dialer`.

### MQTT reception callbacks

The client has a table-like API to set or reset callbacks:

```lua
-- Set a callback
client[topic_filer_string] = callback_function
-- Call or query an existing callback
client[topic_filter_string](self, message, topic, t)
local f = client[topic_filter_string]
-- Reset a callback
client[topic_filter_string] = nil
```

The callbacks are called with four parameters:

 - the client object (`self`);
 - the received message, as a string;
 - the topic of the received message, as a string;
 - the reception time, as a number in seconds (compatible with timers).

Setting a new callback automatically subscribes to the filter,
and resetting the callback automaticall unsubscribes.

Note that when subscribing to overlapping-but-different filters, messages
may be duplicated (i.e. when the overlap makes the broker send the message
twice, each callback will be called twice).
Since QoS is not supported yet, callbacks should always be ready to handle
multiple and missing messages anyway.

### Internal event callbacks

When the connection of a client comes up or down, Lua callback is triggered
as if an empty message has been sent on topic `$SYS/self/online` or
`$SYS/self/offline`.

Note that due to being offline, the client should not be used in the offline
callback.

Also note that mqttagent assumes without checking that subscriptions are
preserved by the broker when coming back online.
The online callback is a good place to resubscribe if needed.

### MQTT message sending

The client has a function-like API to send messages:

```lua
-- Send a ping to the server
client()
-- Send a message to the given topic
client(message, topic)
```

### Timers

Timers are created using `timer.new` with a time and a callback:

```lua
timer_obj = timer.new(t, callback)
```

Timers **are NOT** automatically repeated, the callback is called only
once, after `t`, and then the timer is destroyed unless explicitly
rescheduled:

```lua
timer_obj:schedule(next_t)
```

To make repeating timers, explicitly call `self:schedule` within the
callback with the next time.

When the time given in `timer.new` or `:schedule` is in the past, the
callback is called as soon as possible.

The `:cancel` method is also available to de-schedule a timer without
destroying it.

## Examples

### Simplest client prints one message and leaves

```lua
-- Create an anonymous client
local c = mqttclient.new({ connection = "127.0.0.1:1883" })

-- Subscribe to all topics under `test`
c["test/#"] = function(self, message, topic)
	-- Print the received message
	print(message)
	-- Unsubscribe
	c["test/#"] = nil
end
```

### Client with LWT, keepalive and timer

```lua
-- Keep alive in seconds
local keepalive = 60
-- Create the client
local c = mqttclient.new{
	connection = "127.0.0.1:1883",
	user_name = "mqttagent",
	password = "1234",
	will = {
		topic = "test/status/mqttagent",
		message = "Offline",
	},
	keep_alive = keepalive,
}

-- Setup a ping timer for the keepalive
if keepalive > 0 then
	-- Create a new timer, dropping the variable (`self` is enough)
	timer.new(os.time() + keepalive, function(self, t)
		-- Send a ping
		c()
		-- Schedule next keepalive
		self:schedule(t + keepalive)
	end)
end

-- Print the next 10 messages send to `test` topic
local count = 10
c["test"] = function(self, message, topic)
	print(message)
	count = count - 1
	if count <= 0 then
		os.exit(0)
	end
end

-- Announce start
c("Online", "test/status/mqttagent")
```

### One-way MQTT Bridge

```lua
-- Create the source client
local source = mqttclient.new{
	connection = "mqtt.example.com:1883",
	user_name = "mqttagent",
	password = "1234",
}
-- Create the destination client
local dest = mqttclient.new{ "127.0.0.1:1883" }
-- Make the one-way bridge
source["#"] = function(self, message, topic)
	dest(message, topic)
end
```

### RRD Update Using JSON Payload Form Tasmota Energy Sensor

```lua
local json = require("json")
local c = mqttclient.new{ connection = "127.0.0.1:1883" }

function millis(n)
	if n then
		return math.floor(n * 1000 + 0.5)
	else
		return "U"
	end
end

c["tele/+/SENSOR"] = function(self, message, topic)
	-- Deduce file name from middle part of the topic
	local name = string.sub(topic, 6, -8)
	-- Sanity check
	if string.find(name, "[^_%w%-]") then return end
	-- Decode JSON payload
	local obj, err = json.decode(message)
	if err then
		print(err)
		return
	end
	if not obj.ENERGY then return end

	os.execute("rrdtool update \"" .. dbdir .. "/energy-sensor-" ..
	            name .. ".rrd\" N:" ..
	            millis(obj.ENERGY.Total) ..
	            ":" .. (obj.ENERGY.Period or "U") ..
	            ":" .. (obj.ENERGY.Power or "U") ..
	            ":" .. millis(obj.ENERGY.Factor) ..
	            ":" .. (obj.ENERGY.Voltage or "U") ..
	            ":" .. millis(obj.ENERGY.Current))
end
```

### Backup Monitoring

```lua
local c = mqttclient.new{ connection = "127.0.0.1:1883" }
-- List of topic suffix to check
local backups = {
	["dest1/src1"] = 0,
	["dest1/src2"] = 0,
	["dest2/src2"] = 0,
	["dest3/src1"] = 0,
}

-- Check every day at 5:02:42 UTC that all backups have been done
function next_check()
	local tbl = os.date("!*t")
	tbl.hour = 5
	tbl.min = 2
	tbl.sec = 42
	return os.time(tbl) + 86400
end

timer.new(next_check(), function(self, t)
	local msg = ""
	-- Accumulate missing backup names in msg and reset the table
	for k, v in pairs(backups) do
		if v == 0 then
			msg = msg .. (#msg > 0 and ", " or "") .. k
		end
		backups[k] = 0
	end
	-- Send an alert when a backup is missing
	if #msg > 0 then
		c("Missing: " .. msg, "alert/backup")
	end
	-- Check again tomorrow
	self:schedule(t + 86400)
end)

-- Mark backups as seen when notified
c["backup/#"] = function(self, _, topic, t)
	local name = string.sub(topic, 8)
	if backups[name] then
		backups[name] = t
	end
end
```
