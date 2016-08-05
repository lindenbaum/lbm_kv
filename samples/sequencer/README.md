
sequencer
=========

A very simple service providing distributed sequence numbers. To build it use

```erlang
rebar3 do clean, compile, release
```

Now you have a release which can be started using the `start.sh` script. The
script will start a node and drop you on the console of the running node.

The application comes with a simple process that periodically queries the next
available sequence number and logs in using `error_logger`. Ideally, you'll now
start a few nodes on different shells and look at the logged output.

The [bootstrap](https://github.com/schlagert/bootstrap) project is used to
automatically discover and connect the different nodes (may take up to 10s).
The current (bootstrap) configuration allows up to three nodes to be started on
one host simultaneously. If you use multiple machines there's no limitation.

What you should see is that `lbm_kv` distributes the sequence number among the
different nodes as soon as they connect. When a new node starts it's initial
sequence number is `0`, so you will see duplicates on these nodes. These will
disappear, when the nodes get connected. Feel free to kill/restart random nodes.
