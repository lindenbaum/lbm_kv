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

-include_lib("eunit/include/eunit.hrl").

%%%=============================================================================
%%% TESTS
%%%=============================================================================

basic_test() ->
    ?assertEqual(ok, application:start(mnesia)),

    ?assertEqual(ok, lbm_kv:create(table)),
    ?assertEqual(ok, lbm_kv:replicate_to(table, node())),

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
    ?assertEqual([{key, value}], lbm_kv:get_all(table)),

    ?assertEqual(ok, application:stop(mnesia)).
