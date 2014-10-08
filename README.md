[![Build Status](https://travis-ci.org/lindenbaum/lbm_kv.png?branch=master)](https://travis-ci.org/lindenbaum/lbm_kv)

lbm_kv
======

A simple, lightweight application hiding the dirty details/pitfalls of
distributed Mnesia tables. Furthermore it provides a primitive API to use
Mnesia tables as distributed/RAM-only key-value-storage.

One of the main goals of this application is to enable developers to enjoy the
pleasures of distributed Mnesia without the need of exploring the complex
background.

What does it do?
----------------

When you start the `lbm_kv` application it ensures that the Mnesia `schema`
table is distributed among the connected cluster nodes (as RAM-only copy). This
is the main requirement for distributed Mnesia. It is not necessary to know the
cluster topology in advance, since `lbm_kv` can handle dynamic clusters. In this
initial state, all you have is distributed Mnesia. That means no tables are
created behind the scenes. Any existing Mnesia table can be accessed from all
connected nodes.

Creating tables
---------------

Calling `lbm_kv:create(Table)` will create a new, local, RAM-only
key-value-store with the name `Table`. This table/store can immediately be
accessed from all nodes in the cluster. For redundancy or speed this table/store
can be replicated to other nodes in the cluster either by calling
`lbm_kv:replicate_to(Table, Node)` from any node or by calling
`lbm_kv:create(Table)` on the node you wish to replicate to.

Actually, both functions will create the table `Table` storing tuples of the
form `{Table, Key :: lbm_kv:key(), Value :: lbm_kv:value()}`. Of course, you can
modify the table with the `lbm_kv` or the ordinary Mnesia API.

Netsplits/Inconsistency conditions
----------------------------------

Mnesia does recognize conditions of inconsistency, e.g. after netsplits.
However, Mnesia doesn't handle those conditions. Since this is a field of
research on its own, `lbm_kv` provides only the simplest of all mechanisms.

If Mnesia detects a DB inconsistency, `lbm_kv` will check whether one of its
tables/stores is affected. If this is the case `lbm_kv` will deterministically
__restart__ one of the offending nodes using `init:restart/0` (you can look at
the exact condition in `lbm_kv_mon:default_resolve_conflict/1`). Of course, this
may not be the desired behaviour for all use-cases. Therefore, `lbm_kv` offers
the possibility to change this (on a per table basis). To implement a custom
conflict resolver for a table `Table`, create a module `Table` that implements
the `lbm_kv` behaviour. However, be aware that __if only one table exists
without custom conflict resolver__ a node may get restarted anyway!
