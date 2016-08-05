[![Build Status](https://travis-ci.org/lindenbaum/lbm_kv.png?branch=master)](https://travis-ci.org/lindenbaum/lbm_kv)

lbm_kv
======

A dynamically-distributed, highly-available, partition-tolerant, in-memory
key-value store built with [Mnesia](http://www.erlang.org/doc/apps/mnesia/).

One of the main goals of this application is to enable developers to enjoy the
pleasures of distributed Mnesia without the need of exploring the complex
background. Therefore, `lbm_kv` provides a primitive API along with code to
handle and work around the dirty details and pitfals related to distributed
Mnesia.

Why use it?
-----------

Mnesia is a powerful DBMS with support for table replication, transactions,
netsplit detection and much more. _So why use something on top of it?_
Unfortunately, as with other powerful DBMSs its use is quite complex and making
a Mnesia cluster dynamic requires a lot of research and the use of
_undocumented_ features. `lbm_kv` is here to release you from this pain.

What does lbm_kv offer?
-----------------------

* Mnesia replication management in dynamic Erlang clusters
* automated table merges and netsplit recovery based on
  [vector clocks](https://en.wikipedia.org/wiki/Vector_clock) and user-provided
  callbacks
* a primitive and (hopefully) intuitive API
* small, documented, fully-typed code-base
* no additional/transitive dependencies introduced

How does it work?
-----------------

`lbm_kv` is a simple Erlang application that gets dropped into your release. It
is not necessary to know the cluster topology in advance, since `lbm_kv` can
handle dynamic clusters. It listens for new node connections and replicates all
its tables to the new nodes. When connected nodes go down, `lbm_kv`
automatically shrinks the Mnesia cluster to the remaining nodes preserving the
writability to its tables. The user decides when and what tables to create, no
internal tables are created behind the scenes.

`lbm_kv` is able to merge tables automatically (based on lamport/vector clocks).
This is needed when a netsplit gets resolved or when the same table gets created
on several nodes independently (not a special case for `lbm_kv`). If `lbm_kv`
cannot merge two table entries itself, it will look for a user-defined callback
to help with the merge. This `handle_conflict/3` callback is specified in the
`lbm_kv` behaviour and needs to reside in a module with the same name as the
table to merge values for, e.g. if your table is called `my_table` the callback
to implement would be `my_table:handle_conflict/3`.

If no appropriate callback is found or the callback throws an exception during
the conflict resolution, `lbm_kv` will deterministically __restart__ one of the
offending nodes using `init:restart/0` (the restarted node will be the one that
tried to perform the merge).

Please note that a merge cannot delete values (except for the case when the
user callback gets involved). This means that if a mapping gets deleted during
a netsplit, the mapping might get re-established when the netsplit gets
resolved.

Examples
--------

A very simple example application/release can be found in the
[samples](https://github.com/lindenbaum/lbm_kv/tree/master/samples) directory.
