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
