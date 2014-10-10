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
                [safe()] |
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
      fun put3_get_and_del3/0,
      fun put2_get_and_del2/0,
      fun update/0,
      fun update_all/0,
      fun integration/0,
      fun distributed/0,
      fun netsplit/0
     ]}.

bad_type() ->
    ?assertEqual(
       {aborted,{bad_type,table,{?TABLE,{{a}},'_'}}},
       mnesia:transaction(
         fun() ->
                 mnesia:write({?TABLE, {{'a'}}, ['_']}),
                 mnesia:delete_object({?TABLE, {{'a'}}, '_'})
         end)).

empty() ->
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)).

put3_get_and_del3() ->
    qc(?FORALL(
          {Key, Value},
          {safe(), safe()},
          begin
              KeyValue = {Key, Value},
              ?assertEqual([], lbm_kv:put(?TABLE, Key, Value)),
              ?assertEqual([KeyValue], lbm_kv:get(?TABLE, Key)),
              ?assert(lists:member(KeyValue, lbm_kv:get_all(?TABLE))),
              ?assertEqual([KeyValue], lbm_kv:del(?TABLE, Key, Value)),
              true
          end)).

put2_get_and_del2() ->
    qc(?FORALL(
          {{Key1, Key2}, Value1, Value2},
          {?SUCHTHAT({Key1, Key2}, {safe(), safe()}, Key1 =/= Key2), safe(), safe()},
          begin
              KeyValue1 = {Key1, Value1},
              KeyValue2 = {Key2, Value2},
              ?assertEqual([], lbm_kv:put(?TABLE, [KeyValue1, KeyValue2])),
              Get = lbm_kv:get(?TABLE, '_', dirty),
              ?assert(lists:member(KeyValue1, Get)),
              ?assert(lists:member(KeyValue2, Get)),
              GetAll = lbm_kv:get_all(?TABLE),
              ?assert(lists:member(KeyValue1, GetAll)),
              ?assert(lists:member(KeyValue2, GetAll)),
              Delete = lbm_kv:del(?TABLE, [Key1, Key2]),
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

              Add = fun([]) -> [Value] end,
              ?assertEqual([KeyValue], lbm_kv:update(?TABLE, Key, Add)),
              ?assertEqual([KeyValue], lbm_kv:get(?TABLE, Key)),

              Modify = fun([V]) when V == Value -> [Update] end,
              ?assertEqual([KeyUpdate], lbm_kv:update(?TABLE, Key, Modify)),
              ?assertEqual([KeyUpdate], lbm_kv:get(?TABLE, Key)),

              Delete = fun([V]) when V == Update -> [] end,
              ?assertEqual([], lbm_kv:update(?TABLE, Key, Delete)),
              ?assertEqual([], lbm_kv:get(?TABLE, Key)),
              true
          end)).

update_all() ->
    qc(?FORALL(
          {Key, Value, Update},
          {safe(), safe(), safe()},
          begin
              KeyValue = {Key, Value},
              KeyUpdate = {Key, Update},

              Identity = fun(_, V) -> {ok, V} end,
              ?assertEqual([], lbm_kv:update_all(?TABLE, Identity)),

              Modify = fun(K, V) when K== Key, V == Value ->
                               {ok, Update};
                          (_, V) ->
                               {ok, V}
                          end,
              ?assertEqual([], lbm_kv:put(?TABLE, Key, Value)),
              ?assertEqual([KeyValue], lbm_kv:get(?TABLE, Key)),
              ?assert(lists:member(KeyUpdate, lbm_kv:update_all(?TABLE, Modify))),
              ?assertEqual([KeyUpdate], lbm_kv:get(?TABLE, Key)),

              Delete = fun(K, V) when K == Key, V == Update ->
                               delete;
                          (_, V) ->
                               {ok, V}
                       end,
              ?assert(not lists:member(KeyUpdate, lbm_kv:update_all(?TABLE, Delete))),
              ?assertEqual([], lbm_kv:get(?TABLE, Key)),

              DeleteAll = fun(_, _) -> delete end,
              ?assertEqual([], lbm_kv:update_all(?TABLE, DeleteAll)),
              ?assertEqual([], lbm_kv:get_all(?TABLE)),
              true
          end)).

integration() ->
    %% initial empty
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% add key => value
    ?assertEqual([], lbm_kv:put(?TABLE, key, value)),
    ?assertEqual([{key, value}], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value}], lbm_kv:get_all(?TABLE)),

    %% update to key => value1
    Update1 = fun([value]) -> [value1] end,
    ?assertEqual([{key, value1}], lbm_kv:update(?TABLE, key, Update1)),
    ?assertEqual([{key, value1}], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value1}], lbm_kv:get_all(?TABLE)),

    %% update to key => value2
    UpdateAll1 = fun(key, value1) -> {ok, value2} end,
    ?assertEqual([{key, value2}], lbm_kv:update_all(?TABLE, UpdateAll1)),
    ?assertEqual([{key, value2}], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value2}], lbm_kv:get_all(?TABLE)),

    %% empty table with update
    Update2 = fun([value2]) -> [] end,
    ?assertEqual([], lbm_kv:update(?TABLE, key, Update2)),
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% add key => value2
    ?assertEqual([], lbm_kv:put(?TABLE, key, value2)),
    ?assertEqual([{key, value2}], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value2}], lbm_kv:get_all(?TABLE)),

    %% empty table with update_all
    UpdateAll2 = fun(key, value2) -> delete end,
    ?assertEqual([], lbm_kv:update_all(?TABLE, UpdateAll2)),
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% no update for non-existing key
    Update3 = fun([]) -> [] end,
    ?assertEqual([], lbm_kv:update(?TABLE, key, Update3)),
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% add key => value with update to non-existing key
    Update4 = fun([]) -> [value] end,
    ?assertEqual([{key, value}], lbm_kv:update(?TABLE, key, Update4)),
    ?assertEqual([{key, value}], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value}], lbm_kv:get_all(?TABLE)),

    %% del key => value
    ?assertEqual([{key, value}], lbm_kv:del(?TABLE, key, value)),
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)).

distributed() ->
    process_flag(trap_exit, true),

    %% start slave node with local replica of table
    {ok, Slave1} = slave_setup(slave1),
    ok = lbm_kv:replicate_to(?TABLE, Slave1),

    %% start two slave nodes without replicas
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    %% Put a value from the local node
    PutValue = fun() -> [] = lbm_kv:put(?TABLE, key, value) end,
    PutValue(),

    %% Read the written value from all nodes
    GetValue = fun() -> [{key, value}] = lbm_kv:get(?TABLE, key) end,
    GetValue(),
    ?assertEqual(ok, slave_execute(Slave1, GetValue)),
    ?assertEqual(ok, slave_execute(Slave2, GetValue)),
    ?assertEqual(ok, slave_execute(Slave3, GetValue)),

    %% Read the whole table from all nodes
    GetAll = fun() -> [{key, value}] = lbm_kv:get_all(?TABLE) end,
    GetAll(),
    ?assertEqual(ok, slave_execute(Slave1, GetAll)),
    ?assertEqual(ok, slave_execute(Slave2, GetAll)),
    ?assertEqual(ok, slave_execute(Slave3, GetAll)),

    %% Delete the value from a slave node
    Update = fun() -> [{key, value}] = lbm_kv:del(?TABLE, key, value) end,
    ?assertEqual(ok, slave_execute(Slave1, Update)),

    %% Read the update from all nodes
    GetEmpty = fun() -> [] = lbm_kv:get(?TABLE, key) end,
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
            {ok, Apps} = application:ensure_all_started(lbm_kv),
            ok = lbm_kv:create(?TABLE),
            ok = lbm_kv:replicate_to(?TABLE, node()),
            Apps
    end.

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
    PathFun = fun() -> [code:add_patha(P)|| P <- Paths] end,
    ok = slave_execute(Node, PathFun),
    AppFun = fun() -> {ok, _} = application:ensure_all_started(lbm_kv) end,
    ok = slave_execute(Node, AppFun).

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
