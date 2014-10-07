%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @author Sven Heyll <sven.heyll@lindenbaum.eu>
%%% @author Timo Koepke <timo.koepke@lindenbaum.eu>
%%% @author Tobias Schlager <tobias.schlager@lindenbaum.eu>
%%% @copyright (C) 2014, Lindenbaum GmbH
%%%
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
      fun integration/0
     ]}.

bad_type() ->
    ?assertEqual(
       {aborted,{bad_type,table,{table,{{a}},'_'}}},
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
    ?assertEqual([], lbm_kv:get(table, key)),
    ?assertEqual([], lbm_kv:get_all(table)),

    %% add key => value
    ?assertEqual(ok, lbm_kv:put(table, key, value)),
    ?assertEqual([value], lbm_kv:get(table, key)),
    ?assertEqual([{key, value}], lbm_kv:get_all(table)),

    %% update to key => value1
    Update1 = fun([value]) -> [value1] end,
    ?assertEqual({ok, [value1]}, lbm_kv:update(table, key, Update1)),
    ?assertEqual([value1], lbm_kv:get(table, key)),
    ?assertEqual([{key, value1}], lbm_kv:get_all(table)),

    %% update to key => value2
    UpdateAll1 = fun(key, value1) -> {ok, value2} end,
    ?assertEqual({ok, [{key, value2}]}, lbm_kv:update_all(table, UpdateAll1)),
    ?assertEqual([value2], lbm_kv:get(table, key)),
    ?assertEqual([{key, value2}], lbm_kv:get_all(table)),

    %% empty table with update
    Update2 = fun([value2]) -> [] end,
    ?assertEqual({ok, []}, lbm_kv:update(table, key, Update2)),
    ?assertEqual([], lbm_kv:get(table, key)),
    ?assertEqual([], lbm_kv:get_all(table)),

    %% add key => value2
    ?assertEqual(ok, lbm_kv:put(table, key, value2)),
    ?assertEqual([value2], lbm_kv:get(table, key)),
    ?assertEqual([{key, value2}], lbm_kv:get_all(table)),

    %% empty table with update_all
    UpdateAll2 = fun(key, value2) -> delete end,
    ?assertEqual({ok, []}, lbm_kv:update_all(table, UpdateAll2)),
    ?assertEqual([], lbm_kv:get(table, key)),
    ?assertEqual([], lbm_kv:get_all(table)),

    %% no update for non-existing key
    Update3 = fun([]) -> [] end,
    ?assertEqual({ok, []}, lbm_kv:update(table, key, Update3)),
    ?assertEqual([], lbm_kv:get(table, key)),
    ?assertEqual([], lbm_kv:get_all(table)),

    %% add key => value with update to non-existing key
    Update4 = fun([]) -> [value] end,
    ?assertEqual({ok, [value]}, lbm_kv:update(table, key, Update4)),
    ?assertEqual([value], lbm_kv:get(table, key)),
    ?assertEqual([{key, value}], lbm_kv:get_all(table)).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            {ok, Apps} = application:ensure_all_started(mnesia),
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
%%------------------------------------------------------------------------------
qc(Block) -> ?assert(proper:quickcheck(Block, [long_result, verbose])).
