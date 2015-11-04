%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @copyright (C) 2014, Lindenbaum GmbH
%%%
%%% Permission to use, copy, modify, and/or distribute this software for any
%%% purpose with or without fee is hereby granted, provided that the above
%%% copyright notice and this permission notice appear in all copies.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%%=============================================================================

-module(lbm_kv_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-type safe() :: '' | '!' | a | b |
                number() |
                boolean() |
                binary() |
                tuple(safe()) |
                tuple(safe(), safe()) |
                tuple(safe(), safe(), safe()).
%% Unfortunately, Mnesia is quite picky when it comes to allowed types for
%% keys and values, e.g. all special atoms of `match_specs' are not allowed and
%% lead to undefined behaviour when used.

-define(TABLE, table).
-define(NODE, master).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {foreach, setup(), teardown(),
     [
      fun bad_type/0,
      fun empty/0,
      fun put3_get_and_del2/0,
      fun put2_get_and_del2/0,
      fun update/0,
      fun update_table/0,
      fun integration/0,
      fun distributed/0,
      fun netsplit/0
     ]}.

bad_type() ->
    ?assertEqual(
       {aborted,{bad_type,{?TABLE,{{a}},['_']}}},
       mnesia:transaction(
         fun() ->
                 mnesia:write({?TABLE, {{'a'}}, ['_']}),
                 mnesia:delete_object({?TABLE, {{'a'}}, '_'})
         end)).

empty() ->
    ?assertEqual({ok, []}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, []}, lbm_kv:match(?TABLE, '_')).

put3_get_and_del2() ->
    qc(?FORALL(
          {Key, Value},
          {safe(), safe()},
          begin
              KeyValue = {Key, Value},
              ?assertEqual({ok, []}, lbm_kv:put(?TABLE, Key, Value)),
              ?assertEqual({ok, [KeyValue]}, lbm_kv:get(?TABLE, Key)),
              {ok, Matched} = lbm_kv:match(?TABLE, '_'),
              ?assert(lists:member(KeyValue, Matched)),
              ?assertEqual({ok, [KeyValue]}, lbm_kv:del(?TABLE, Key)),
              true
          end)).

put2_get_and_del2() ->
    qc(?FORALL(
          {{Key1, Key2}, Value1, Value2},
          {?SUCHTHAT({Key1, Key2}, {safe(), safe()}, Key1 =/= Key2), safe(), safe()},
          begin
              KeyValue1 = {Key1, Value1},
              KeyValue2 = {Key2, Value2},
              ?assertEqual({ok, []}, lbm_kv:put(?TABLE, [KeyValue1, KeyValue2])),
              {ok, Get} = lbm_kv:match(?TABLE, '_', dirty),
              ?assert(lists:member(KeyValue1, Get)),
              ?assert(lists:member(KeyValue2, Get)),
              {ok, Delete} = lbm_kv:del(?TABLE, [Key1, Key2]),
              ?assert(lists:member(KeyValue1, Delete)),
              ?assert(lists:member(KeyValue2, Delete)),
              true
          end)).

update() ->
    qc(?FORALL(
          {Key, Value, Update},
          {safe(), safe(), safe()},
          begin
              KeyValue = {Key, Value},
              KeyUpdate = {Key, Update},

              Add = fun(_, undefined) -> {value, Value} end,
              ?assertEqual({ok, []}, lbm_kv:update(?TABLE, Key, Add)),
              ?assertEqual({ok, [KeyValue]}, lbm_kv:get(?TABLE, Key)),

              Modify = fun(_, {value, V}) when V == Value -> {value, Update} end,
              ?assertEqual({ok, [KeyValue]}, lbm_kv:update(?TABLE, Key, Modify)),
              ?assertEqual({ok, [KeyUpdate]}, lbm_kv:get(?TABLE, Key)),

              Delete = fun(_, {value, V}) when V == Update -> delete end,
              ?assertEqual({ok, [KeyUpdate]}, lbm_kv:update(?TABLE, Key, Delete)),
              ?assertEqual({ok, []}, lbm_kv:get(?TABLE, Key)),
              true
          end)).

update_table() ->
    qc(?FORALL(
          {Key, Value, Update},
          {safe(), safe(), safe()},
          begin
              KeyValue = {Key, Value},
              KeyUpdate = {Key, Update},

              Identity = fun(_, {value, V}) -> {value, V} end,
              ?assertEqual({ok, []}, lbm_kv:update(?TABLE, Identity)),

              Modify = fun(K, {value, V}) when K == Key, V == Value ->
                               {value, Update};
                          (_, {value, V}) ->
                               {value, V}
                          end,
              ?assertEqual({ok, []}, lbm_kv:put(?TABLE, Key, Value)),
              ?assertEqual({ok, [KeyValue]}, lbm_kv:get(?TABLE, Key)),
              {ok, Modified} = lbm_kv:update(?TABLE, Modify),
              ?assert(lists:member(KeyValue, Modified)),
              ?assertEqual({ok, [KeyUpdate]}, lbm_kv:get(?TABLE, Key)),

              Delete = fun(K, {value, V}) when K == Key, V == Update ->
                               delete;
                          (_, {value, V}) ->
                               {value, V}
                       end,
              {ok, Deleted} = lbm_kv:update(?TABLE, Delete),
              ?assert(lists:member(KeyUpdate, Deleted)),
              ?assertEqual({ok, []}, lbm_kv:get(?TABLE, Key)),

              DeleteAll = fun(_, _) -> delete end,
              ?assertEqual({ok, []}, lbm_kv:update(?TABLE, DeleteAll)),
              ?assertEqual({ok, []}, lbm_kv:match(?TABLE, '_')),
              true
          end)).

integration() ->
    %% initial empty
    ?assertEqual({ok, []}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, []}, lbm_kv:match(?TABLE, '_')),

    %% info
    ?assertEqual(ok, lbm_kv:info()),

    %% add key => value
    ?assertEqual({ok, []}, lbm_kv:put(?TABLE, key, value)),
    ?assertEqual({ok, [{key, value}]}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, [{key, value}]}, lbm_kv:match(?TABLE, '_')),

    %% update to key => value1
    Update1 = fun(key, {value, value}) -> {value, value1} end,
    ?assertEqual({ok, [{key, value}]}, lbm_kv:update(?TABLE, key, Update1)),
    ?assertEqual({ok, [{key, value1}]}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, [{key, value1}]}, lbm_kv:match(?TABLE, '_')),

    %% update to key => value2
    UpdateAll1 = fun(key, {value, value1}) -> {value, value2} end,
    ?assertEqual({ok, [{key, value1}]}, lbm_kv:update(?TABLE, UpdateAll1)),
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:match(?TABLE, '_')),

    %% empty table with update
    Update2 = fun(key, {value, value2}) -> delete end,
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:update(?TABLE, key, Update2)),
    ?assertEqual({ok, []}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, []}, lbm_kv:match(?TABLE, '_')),

    %% add key => value2
    ?assertEqual({ok, []}, lbm_kv:put(?TABLE, key, value2)),
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:match(?TABLE, '_')),

    %% empty table with update_all
    UpdateAll2 = fun(key, {value, value2}) -> delete end,
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:update(?TABLE, UpdateAll2)),
    ?assertEqual({ok, []}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, []}, lbm_kv:match(?TABLE, '_')),

    %% no update for non-existing key
    Update3 = fun(key, undefined) -> undefined end,
    ?assertEqual({ok, []}, lbm_kv:update(?TABLE, key, Update3)),
    ?assertEqual({ok, []}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, []}, lbm_kv:match(?TABLE, '_')),

    %% add key => value with update to non-existing key
    Update4 = fun(key, undefined) -> {value, value} end,
    ?assertEqual({ok, []}, lbm_kv:update(?TABLE, key, Update4)),
    ?assertEqual({ok, [{key, value}]}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, [{key, value}]}, lbm_kv:match(?TABLE, '_')),

    %% del key => value
    ?assertEqual({ok, [{key, value}]}, lbm_kv:del(?TABLE, key)),
    ?assertEqual({ok, []}, lbm_kv:get(?TABLE, key)),
    ?assertEqual({ok, []}, lbm_kv:match(?TABLE, '_')).

distributed() ->
    process_flag(trap_exit, true),

    %% start slave node with local replica of table
    {ok, Slave1} = slave_setup(slave1),
    ok = lbm_kv:replicate_to(?TABLE, Slave1),

    %% start two slave nodes without replicas
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    %% Put a value from the local node
    PutValue = fun() -> {ok, []} = lbm_kv:put(?TABLE, key, value) end,
    PutValue(),

    %% Read the written value from all nodes
    GetValue = fun() -> {ok, [{key, value}]} = lbm_kv:get(?TABLE, key) end,
    GetValue(),
    ?assertEqual(ok, slave_execute(Slave1, GetValue)),
    ?assertEqual(ok, slave_execute(Slave2, GetValue)),
    ?assertEqual(ok, slave_execute(Slave3, GetValue)),

    %% Read the whole table from all nodes
    GetAll = fun() -> {ok, [{key, value}]} = lbm_kv:match(?TABLE, '_') end,
    GetAll(),
    ?assertEqual(ok, slave_execute(Slave1, GetAll)),
    ?assertEqual(ok, slave_execute(Slave2, GetAll)),
    ?assertEqual(ok, slave_execute(Slave3, GetAll)),

    %% Delete the value from a slave node
    Update = fun() -> {ok, [{key, value}]} = lbm_kv:del(?TABLE, key) end,
    ?assertEqual(ok, slave_execute(Slave1, Update)),

    %% Read the update from all nodes
    GetEmpty = fun() -> {ok, []} = lbm_kv:get(?TABLE, key) end,
    GetEmpty(),
    ?assertEqual(ok, slave_execute(Slave1, GetEmpty)),
    ?assertEqual(ok, slave_execute(Slave2, GetEmpty)),
    ?assertEqual(ok, slave_execute(Slave3, GetEmpty)),

    %% Shutdown a slave node
    ?assertEqual(ok, slave:stop(Slave2)),

    %% Put a value from a slave node
    ?assertEqual(ok, slave_execute(Slave3, PutValue)),

    %% Start previously exited node
    {ok, Slave2} = slave_setup(slave2),

    %% Read the written value from all nodes
    GetValue(),
    ?assertEqual(ok, slave_execute(Slave1, GetValue)),
    ?assertEqual(ok, slave_execute(Slave2, GetValue)),
    ?assertEqual(ok, slave_execute(Slave3, GetValue)),

    ok.

netsplit() ->
    process_flag(trap_exit, true),

    %% start slave nodes with local replica of table
    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    ok = lbm_kv:replicate_to(?TABLE, Slave1),
    ok = lbm_kv:replicate_to(?TABLE, Slave2),

    %% start slave node without local replica of table
    {ok, Slave3} = slave_setup(slave3),

    ok = net_kernel:monitor_nodes(true),

    %% simualte netsplit between Slave1 and Slave2, Slave3
    Netsplit = fun() ->
                       true = net_kernel:disconnect(Slave2),
                       true = net_kernel:disconnect(Slave3),
                       true = net_kernel:connect(Slave2),
                       true = net_kernel:connect(Slave3)
               end,
    ok = slave_execute(Slave1, Netsplit),

    %% Expect that Slave2 gets restarted by the default conflict resolver.
    %% Unfortunately, slave nodes cannot be restarted using init:restart/1,
    %% so we cannot test actual recovery.
    receive {nodedown, Slave2} -> ok end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            ok = distribute(?NODE),
            Apps = setup_apps(),
            ok = lbm_kv:create(?TABLE),
            ok = lbm_kv:replicate_to(?TABLE, node()),
            Apps
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup_apps() ->
    application:load(sasl),
    ok = application:set_env(sasl, sasl_error_logger, false),
    {ok, Apps} = application:ensure_all_started(lbm_kv),
    Apps.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
teardown() ->
    fun(Apps) ->
            [application:stop(App) || App <- Apps]
    end.

%%------------------------------------------------------------------------------
%% @private
%% Make this node a distributed node.
%%------------------------------------------------------------------------------
distribute(Name) ->
    os:cmd("epmd -daemon"),
    case net_kernel:start([Name]) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Error                         -> Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Start a slave node and setup its environment (code path, applications, ...).
%%------------------------------------------------------------------------------
slave_setup(Name) ->
    {ok, Node} = slave:start_link(hostname(), Name),
    true = lists:member(Node, nodes()),
    slave_setup_env(Node),
    {ok, Node}.

%%------------------------------------------------------------------------------
%% @private
%% Setup the slave node environment (code path, applications, ...).
%%------------------------------------------------------------------------------
slave_setup_env(Node) ->
    Paths = code:get_path(),
    ok = slave_execute(Node, fun() -> [code:add_patha(P)|| P <- Paths] end),
    ok = slave_execute(Node, fun() -> setup_apps() end).

%%------------------------------------------------------------------------------
%% @private
%% Execute `Fun' on the given node.
%%------------------------------------------------------------------------------
slave_execute(Node, Fun) ->
    Pid = spawn_link(Node, Fun),
    receive
        {'EXIT', Pid, normal} -> ok;
        {'DOWN', Pid, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
hostname() -> list_to_atom(element(2, inet:gethostname())).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
qc(Block) -> ?assert(proper:quickcheck(Block, [long_result, verbose])).
