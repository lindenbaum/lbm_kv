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

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {foreach, setup(), teardown(),
     [
      fun bad_type/0,
      fun empty/0,
      fun put_and_get/0,
      fun update/0,
      fun update_all/0,
      fun integration/0,
      fun distributed/0
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

put_and_get() ->
    qc(?FORALL(
          {Key, Value},
          {safe(), safe()},
          begin
              ?assertEqual(ok, lbm_kv:put(?TABLE, Key, Value)),
              ?assertEqual([Value], lbm_kv:get(?TABLE, Key)),
              ?assert(lists:member({Key, Value}, lbm_kv:get_all(?TABLE))),
              true
          end)).

update() ->
    qc(?FORALL(
          {Key, Value, Update},
          {safe(), safe(), safe()},
          begin
              Add = fun([]) -> [Value] end,
              ?assertEqual({ok, [Value]}, lbm_kv:update(?TABLE, Key, Add)),
              ?assertEqual([Value], lbm_kv:get(?TABLE, Key)),

              Modify = fun([V]) when V == Value -> [Update] end,
              ?assertEqual({ok, [Update]}, lbm_kv:update(?TABLE, Key, Modify)),
              ?assertEqual([Update], lbm_kv:get(?TABLE, Key)),

              Delete = fun([V]) when V == Update -> [] end,
              ?assertEqual({ok, []}, lbm_kv:update(?TABLE, Key, Delete)),
              ?assertEqual([], lbm_kv:get(?TABLE, Key)),
              true
          end)).

update_all() ->
    qc(?FORALL(
          {Key, Value, Update},
          {safe(), safe(), safe()},
          begin
              Identity = fun(_, V) -> {ok, V} end,
              ?assertEqual({ok, []}, lbm_kv:update_all(?TABLE, Identity)),

              Modify = fun(K, V) when K== Key, V == Value ->
                               {ok, Update};
                          (_, V) ->
                               {ok, V}
                          end,
              ?assertEqual(ok, lbm_kv:put(?TABLE, Key, Value)),
              ?assertEqual([Value], lbm_kv:get(?TABLE, Key)),
              {ok, Content1} = lbm_kv:update_all(?TABLE, Modify),
              ?assert(lists:member({Key, Update}, Content1)),
              ?assertEqual([Update], lbm_kv:get(?TABLE, Key)),

              Delete = fun(K, V) when K == Key, V == Update ->
                               delete;
                          (_, V) ->
                               {ok, V}
                       end,
              {ok, Content2} = lbm_kv:update_all(?TABLE, Delete),
              ?assert(not lists:member({Key, Update}, Content2)),
              ?assertEqual([], lbm_kv:get(?TABLE, Key)),

              DeleteAll = fun(_, _) -> delete end,
              ?assertEqual({ok, []}, lbm_kv:update_all(?TABLE, DeleteAll)),
              ?assertEqual([], lbm_kv:get_all(?TABLE)),
              true
          end)).

integration() ->
    %% initial empty
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% add key => value
    ?assertEqual(ok, lbm_kv:put(?TABLE, key, value)),
    ?assertEqual([value], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value}], lbm_kv:get_all(?TABLE)),

    %% update to key => value1
    Update1 = fun([value]) -> [value1] end,
    ?assertEqual({ok, [value1]}, lbm_kv:update(?TABLE, key, Update1)),
    ?assertEqual([value1], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value1}], lbm_kv:get_all(?TABLE)),

    %% update to key => value2
    UpdateAll1 = fun(key, value1) -> {ok, value2} end,
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:update_all(?TABLE, UpdateAll1)),
    ?assertEqual([value2], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value2}], lbm_kv:get_all(?TABLE)),

    %% empty table with update
    Update2 = fun([value2]) -> [] end,
    ?assertEqual({ok, []}, lbm_kv:update(?TABLE, key, Update2)),
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% add key => value2
    ?assertEqual(ok, lbm_kv:put(?TABLE, key, value2)),
    ?assertEqual([value2], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value2}], lbm_kv:get_all(?TABLE)),

    %% empty table with update_all
    UpdateAll2 = fun(key, value2) -> delete end,
    ?assertEqual({ok, []}, lbm_kv:update_all(?TABLE, UpdateAll2)),
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% no update for non-existing key
    Update3 = fun([]) -> [] end,
    ?assertEqual({ok, []}, lbm_kv:update(?TABLE, key, Update3)),
    ?assertEqual([], lbm_kv:get(?TABLE, key)),
    ?assertEqual([], lbm_kv:get_all(?TABLE)),

    %% add key => value with update to non-existing key
    Update4 = fun([]) -> [value] end,
    ?assertEqual({ok, [value]}, lbm_kv:update(?TABLE, key, Update4)),
    ?assertEqual([value], lbm_kv:get(?TABLE, key)),
    ?assertEqual([{key, value}], lbm_kv:get_all(?TABLE)).

distributed() ->
    process_flag(trap_exit, true),

    %% start slave node with local replica of table
    {ok, Slave1} = slave_setup(slave1),
    ok = lbm_kv:replicate_to(?TABLE, Slave1),

    %% start two slave nodes without replicas
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    %% Put a value from the local node
    PutValue = fun() -> ok = lbm_kv:put(?TABLE, key, value) end,
    PutValue(),

    %% Read the written value from all nodes
    GetValue = fun() -> [value] = lbm_kv:get(?TABLE, key) end,
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
    Update = fun() -> lbm_kv:update(?TABLE, key, fun([value]) -> [] end) end,
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

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            ok = distribute(master),
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
    Paths = code:get_path(),
    PathFun = fun() -> [code:add_patha(P)|| P <- Paths] end,
    ok = slave_execute(Node, PathFun),
    AppFun = fun() -> {ok, _} = application:ensure_all_started(lbm_kv) end,
    ok = slave_execute(Node, AppFun),
    {ok, Node}.

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
