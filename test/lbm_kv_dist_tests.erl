%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @copyright (C) 2014-2016, Lindenbaum GmbH
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

-module(lbm_kv_dist_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TABLE, table).

-define(NETSPLIT_EVENT, {mnesia_system_event, {inconsistent_database, _, _}}).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {foreach, setup(), teardown(),
     [
      {timeout, 10, [fun unique_table/0]},
      {timeout, 10, [fun simple_netsplit/0]}
     ]}.

unique_table() ->
    process_flag(trap_exit, true),

    error_logger:info_msg("TEST: ~s~n", [unique_table]),

    %% create table locally
    Create = fun() -> ok = lbm_kv:create(?TABLE) end,
    Create(),

    %% start three slave nodes
    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    %% Put a value from the local node
    PutValue = fun() -> {ok, []} = lbm_kv:put(?TABLE, key, value) end,
    PutValue(),

    %% Wait for the table to become available on all nodes
    Wait = fun() -> ok = mnesia:wait_for_tables([?TABLE], 2000) end,
    ?assertEqual(ok, slave_execute(Slave1, Wait)),
    ?assertEqual(ok, slave_execute(Slave2, Wait)),
    ?assertEqual(ok, slave_execute(Slave3, Wait)),

    %% Read the written value from all nodes
    GetValue = fun() -> {ok, [{key, value}]} = lbm_kv:get(?TABLE, key) end,
    GetValue(),
    ?assertEqual(ok, slave_execute(Slave1, GetValue)),
    ?assertEqual(ok, slave_execute(Slave2, GetValue)),
    ?assertEqual(ok, slave_execute(Slave3, GetValue)),

    %% Read the whole table from all nodes
    GetAll = fun() -> {ok, [{key, value}]} = lbm_kv:match_key(?TABLE, '_') end,
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
    ?assertEqual(ok, slave_execute(Slave2, Wait)),

    %% Read the written value from all nodes
    GetValue(),
    ?assertEqual(ok, slave_execute(Slave1, GetValue)),
    ?assertEqual(ok, slave_execute(Slave2, GetValue)),
    ?assertEqual(ok, slave_execute(Slave3, GetValue)),

    ok.

simple_netsplit() ->
    process_flag(trap_exit, true),

    error_logger:info_msg("TEST: ~s~n", [simple_netsplit]),

    %% start two slave nodes
    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),

    %% create table
    Create = fun() -> ok = lbm_kv:create(?TABLE) end,
    Create(),
    ?assertEqual(ok, slave_execute(Slave1, Create)),
    ?assertEqual(ok, slave_execute(Slave2, Create)),

    %% Put some (non-conflicting) values
    PutValue0 = fun() -> {ok, []} = lbm_kv:put(?TABLE, node(), value0) end,
    PutValue0(),
    ?assertEqual(ok, slave_execute(Slave1, PutValue0)),
    ?assertEqual(ok, slave_execute(Slave2, PutValue0)),

    %% Read the values written before from all nodes
    NumValues = length(nodes()) + 1,
    GetValues = fun() ->
                        {ok, Vals} = lbm_kv:match_key(?TABLE, '_'),
                        NumValues = length(Vals)
                end,
    GetValues(),
    ?assertEqual(ok, slave_execute(Slave1, GetValues)),
    ?assertEqual(ok, slave_execute(Slave2, GetValues)),

    PutValue1 = fun() -> {ok, _} = lbm_kv:put(?TABLE, node(), value1) end,

    %% simulate netsplit between both slaves
    Netsplit = fun() ->
                       {ok, _} = mnesia:subscribe(system),
                       true = net_kernel:disconnect(Slave2),

                       %% Make the merge a bit more meaningful
                       PutValue1(),

                       true = net_kernel:connect(Slave2),
                       receive ?NETSPLIT_EVENT -> ok end
               end,
    ok = slave_execute(Slave1, Netsplit),

    PutValue1(),
    ?assertEqual(ok, slave_execute(Slave2, PutValue1)),

    %% sorry, but there's no event we can wait for...
    timer:sleep(1000),

    GetValue1 = fun(K) -> {ok, [{K, value1}]} = lbm_kv:get(?TABLE, K) end,
    GetValues1 = fun() -> [GetValue1(N) || N <- nodes()] end,
    GetValues1(),
    ?assertEqual(ok, slave_execute(Slave1, GetValues1)),
    ?assertEqual(ok, slave_execute(Slave2, GetValues1)),

    ok.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            ok = distribute('master@localhost'),
            setup_apps()
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup_apps() ->
    {ok, Apps} = application:ensure_all_started(lbm_kv, permanent),
    Apps.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
teardown() -> fun(Apps) -> [application:stop(App) || App <- Apps] end.

%%------------------------------------------------------------------------------
%% @private
%% Make this node a distributed node.
%%------------------------------------------------------------------------------
distribute(Name) ->
    os:cmd("epmd -daemon"),
    case net_kernel:start([Name, shortnames]) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Error                         -> Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Start a slave node and setup its environment (code path, applications, ...).
%%------------------------------------------------------------------------------
slave_setup(Name) ->
    Arg = string:join(["-pa " ++ P || P <- code:get_path()], " "),
    {ok, Node} = slave:start_link(localhost, Name, Arg),
    %% Make sure slave node started correctly and is now connected
    true = lists:member(Node, nodes()),
    %% Start the needed applications
    ok = slave_execute(Node, fun() -> setup_apps() end),
    {ok, Node}.

%%------------------------------------------------------------------------------
%% @private
%% Execute `Fun' on the given node.
%%------------------------------------------------------------------------------
slave_execute(Node, Fun) ->
    slave_execute(Node, Fun, sync).
slave_execute(Node, Fun, no_block) ->
    spawn(Node, Fun),
    ok;
slave_execute(Node, Fun, _) ->
    Pid = spawn_link(Node, Fun),
    receive
        {'EXIT', Pid, normal} -> ok;
        {'EXIT', Pid, Reason} -> {error, Reason}
    end.
