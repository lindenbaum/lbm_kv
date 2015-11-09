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

-module(lbm_kv_dist_test).

-include_lib("eunit/include/eunit.hrl").

-define(TABLE, table).
-define(NODE, master).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {foreach, setup(), teardown(),
     [
      {timeout, 10, [fun unique_table/0]}
     ]}.

unique_table() ->
    process_flag(trap_exit, true),

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
    ?assertEqual(ok, slave_execute(Slave2, Wait)),

    %% Read the written value from all nodes
    GetValue(),
    ?assertEqual(ok, slave_execute(Slave1, GetValue)),
    ?assertEqual(ok, slave_execute(Slave2, GetValue)),
    ?assertEqual(ok, slave_execute(Slave3, GetValue)),

    ok.

%% netsplit() ->
%%     process_flag(trap_exit, true),

%%     %% start slave nodes with local replica of table
%%     {ok, Slave1} = slave_setup(slave1),
%%     {ok, Slave2} = slave_setup(slave2),

%%     %% start slave node without local replica of table
%%     {ok, Slave3} = slave_setup(slave3),

%%     ok = net_kernel:monitor_nodes(true),

%%     %% simualte netsplit between Slave1 and Slave2, Slave3
%%     Netsplit = fun() ->
%%                        true = net_kernel:disconnect(Slave2),
%%                        true = net_kernel:disconnect(Slave3),
%%                        true = net_kernel:connect(Slave2),
%%                        true = net_kernel:connect(Slave3)
%%                end,
%%     ok = slave_execute(Slave1, Netsplit),

%%     %% Expect that Slave2 gets restarted by the default conflict resolver.
%%     %% Unfortunately, slave nodes cannot be restarted using init:restart/1,
%%     %% so we cannot test actual recovery.
%%     receive {nodedown, Slave2} -> ok end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            ok = distribute(?NODE),
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
        {'EXIT', Pid, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
hostname() -> list_to_atom(element(2, inet:gethostname())).
