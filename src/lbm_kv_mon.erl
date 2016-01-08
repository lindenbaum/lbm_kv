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
%%%
%%% @doc
%%% A registered server that manages dynamic expansion and reduction of the
%%% Mnesia cluster. The server also subscribes for Mnesia system events. In
%%% case a DB inconsistency is detected (split brain) the server tries to
%%% resolve the conflict using the vector clocks of the `lbm_kv' tables as well
%%% as user-provided resolve callbacks. If conflict resolution fails, one of the
%%% conflicting nodes will be restarted!
%%% @end
%%%=============================================================================

-module(lbm_kv_mon).

-behaviour(gen_server).

%% Internal API
-export([start_link/0,
         info/0]).

%% gen_server callbacks
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("lbm_kv.hrl").

-define(DUMP, "mnesia_core.dump").
-define(ERR(Fmt, Args), error_logger:error_msg(Fmt, Args)).
-define(INFO(Fmt, Args), error_logger:info_msg(Fmt, Args)).

%%%=============================================================================
%%% Internal API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Simply start the server (registered).
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @private
%% Print the all `lbm_kv` tables visible to this node.
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() ->
    Tables = relevant_tables(node()),
    io:format("~w lbm_kv tables visible to ~s~n", [length(Tables), node()]),
    [io:format(" * ~s~n", [Table]) || Table <- Tables],
    io:format("~n").

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(state, {}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    {ok, _} = mnesia:subscribe(system),
    [self() ! {nodeup, Node} || Node <- nodes()],
    ok = net_kernel:monitor_nodes(true),
    {ok, #state{}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(_Request, _From, State) -> {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast(_Request, State) -> {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({mnesia_system_event, Event}, State) ->
    handle_mnesia_event(Event, State),
    {noreply, State};
handle_info({nodeup, Node}, State) ->
    ok = global:trans({?MODULE, self()}, fun() -> expand(Node) end),
    {noreply, State};
handle_info({nodedown, Node}, State) ->
    ok = global:trans({?MODULE, self()}, fun() -> reduce(Node) end),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) -> ok.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Handle Mnesia system events. Fatal conditions will be resolved with node
%% restart.
%%------------------------------------------------------------------------------
handle_mnesia_event({mnesia_fatal, Format, Args, BinaryCore}, _State) ->
    ?ERR("Fatal condition: " ++ Format, Args),
    file:write_file(?DUMP, BinaryCore),
    handle_unresolved_conflict();
handle_mnesia_event({mnesia_info, Format, Args}, _State) ->
    ?INFO(Format, Args);
handle_mnesia_event(_Event, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% @private
%% Expand the mnesia cluster to `Node'. This will add table copies of local
%% tables to `Node' as well as table copies remote tables on the local node.
%% This is the central function handling dynamic expansion of mnesia on new
%% nodes as well as recovery from partitioned networks.
%%
%% A word about table replication:
%% When using RAM copies, Mnesia is able to `merge' schema tables of different
%% nodes, as long as one of the schema's to merge is clean (no tables with the
%% same name on both nodes). By default tables are assigned a `cookie' value
%% that differs even for tables with the same name and makes them incompatible
%% by default. `lbm_kv' avoids this by assigning each of its tables a custom
%% cookie (which is always the same). However, even using this trick, special
%% magic is needed to merge these tables. Each of these tables must be
%% configured as RAM copy on the remote node __before__ merging the schemas.
%%
%% Thanks again to Ulf Wiger (`unsplit') for pointing us in the right direction.
%%------------------------------------------------------------------------------
expand(Node) ->
    expand(is_running(Node), Node).
expand(true, Node) ->
    LocalTables = relevant_tables(node()),
    RemoteTables = relevant_tables(Node),
    ?LBM_KV_DBG("Relevant tables on ~s: ~w~n", [node(), LocalTables]),
    ?LBM_KV_DBG("Relevant tables on ~s: ~w~n", [Node, RemoteTables]),

    LocalOnlyTables = LocalTables -- RemoteTables,
    RemoteOnlyTables = RemoteTables -- LocalTables,
    ?LBM_KV_DBG("Local-only tables on ~s: ~w~n", [node(), LocalOnlyTables]),
    ?LBM_KV_DBG("Local-only tables on ~s: ~w~n", [Node, RemoteOnlyTables]),

    LocalSchema = get_cookie(node(), schema),
    RemoteSchema = get_cookie(Node, schema),
    IsRemoteRunning = lists:member(Node, get_running_nodes(node())),
    case {LocalSchema =:= RemoteSchema, IsRemoteRunning} of
        {true, true} ->
            %% Schemas are already merged and we see the node as running,
            %% someone else did the work. However, this also means that the
            %% respective remote node must have this node in its
            %% `running_db_nodes'.
            true = lists:member(node(), get_running_nodes(Node)),

            %% Now distribute tables only found on one of the nodes.
            add_table_copies(node(), Node, LocalOnlyTables),
            add_table_copies(Node, node(), RemoteOnlyTables),

            ?INFO("Already connected to ~s~n", [Node]);
        {IsMerged, IsRemoteRunning} ->
            case {IsMerged, IsRemoteRunning} of
                {false, false} ->
                    %% The newly connected node has a different schema. That
                    %% means the schemas must be merged now.
                    ?INFO("Expanding to ~s...~n", [Node]);
                {true, false} ->
                    %% Both nodes have the same schema, but none of the nodes
                    %% has its respective counterpart in its `running_db_nodes',
                    %% this must be a reconnect after a netsplit.
                    ?INFO("Recovering from network partition (~s)...~n", [Node])
            end,
            connect_nodes(Node, LocalTables, LocalOnlyTables, RemoteOnlyTables)
    end;
expand(false, _) ->
    ok. %% Do not expand to the remote node if mnesia is not running.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
connect_nodes(Node, LocalTables, LocalOnlyTables, RemoteOnlyTables) ->
    %% All duplicate tables must be interconnected (with mutual RAM copies)
    %% __before__ connecting the mnesia instances.
    LocalAndRemoteTables = LocalTables -- LocalOnlyTables,
    add_table_copies(node(), Node, LocalAndRemoteTables),
    add_table_copies(Node, node(), LocalAndRemoteTables),

    %% Connect both nodes and merge values of duplicate tables.
    Fun = connect_nodes_user_fun(LocalAndRemoteTables, Node),
    case mnesia_controller:connect_nodes([Node], Fun) of
        {ok, NewNodes} ->
            %% The merge is only successful if Node is now part of the
            %% mnesia cluster
            true = lists:member(Node, NewNodes),

            %% Now distribute tables only found on one of the nodes.
            add_table_copies(node(), Node, LocalOnlyTables),
            add_table_copies(Node, node(), RemoteOnlyTables),

            ?INFO("Successfully connected to ~s~n", [Node]);
        Error ->
            ?ERR("Failed to connect to ~s: ~w~n", [Node, Error]),

            %% last resort
            handle_unresolved_conflict()
    end.

%%------------------------------------------------------------------------------
%% @private
%% A custom user function that merges the schemas of two node islands as well as
%% the respective values in conflicting tables.
%%------------------------------------------------------------------------------
connect_nodes_user_fun(TablesToMerge, Node) ->
    fun(SchemaMergeFun) ->
            case SchemaMergeFun(TablesToMerge) of
                Result = {merged, OldFriends, NewFriends} ->
                    ?LBM_KV_DBG("Schemas successfully merged~n", []),
                    ?LBM_KV_DBG("NewFriends: ~w~n", [NewFriends]),

                    %% Sorry, but we must be part of `db_nodes' ourselves or I
                    %% loose my mind (see mnesia_schema:do_merge_schema/1).
                    true = lists:member(node(), OldFriends),

                    %% What does this mean?
                    %% Unfortunately, this is also not really clear to me. My
                    %% assumption:
                    %% `NewFriends' seems to be the list of nodes the local node
                    %% successfully merged its schema with (without the local
                    %% node itself). The local node is usually part of the
                    %% `OldFriends' list. This seems to be equivalent to the
                    %% current `db_nodes'. In case of asymmetric netsplits there
                    %% may be a non-empty intersection between the two lists.
                    %% The merge function may be called multiple times in case
                    %% additional nodes become visible by merged schemas or
                    %% through merge retries.
                    %%
                    %% What do we do here?
                    %% We try to make sure to always merge with a sane candidate
                    %% from another (new) island mnesia connects us to.
                    case lists:member(Node, OldFriends) of
                        false ->
                            %% If we did not already merge with `Node', it
                            %% __must__ be a member of `NewFriends'.
                            true = lists:member(Node, NewFriends),
                            MergeWith = [Node];
                        true ->
                            %% Otherwise `Node' is already part of `db_nodes'
                            %% and thus a member of `OldFriends'. In this case
                            %% we simple choose a representative of
                            %% `NewFriends' that is not already an old friend.
                            MergeWith = NewFriends -- OldFriends
                    end,

                    case lbm_kv_merge:tables(TablesToMerge, MergeWith) of
                        ok              -> Result;
                        {error, Reason} -> mnesia:abort(Reason)
                    end;
                Result ->
                    Result
            end
    end.

%%------------------------------------------------------------------------------
%% @private
%% Remove `Node' from the distributed mnesia cluster.
%%------------------------------------------------------------------------------
reduce(Node) ->
    case lists:member(Node, get_db_nodes(node())) of
        true ->
            %% Remove the disconnected node from the global schema. This will
            %% remove all ram copies copied onto this node (for all tables).
            case mnesia:del_table_copy(schema, Node) of
                {atomic, ok} ->
                    ?INFO("Successfully disconnected from ~s~n", [Node]);
                Error = {aborted, _} ->
                    ?LBM_KV_DBG("Failed to remove schema from ~s: ~w~n",
                                [Node, Error])
            end;
        false ->
            %% The disconnected node is not part of the seen `db_nodes' anymore,
            %% someone else did the work or the node did never participate.
            ok
    end.

%%------------------------------------------------------------------------------
%% @private
%% Returns whether mnesia is running on `Node'.
%%------------------------------------------------------------------------------
is_running(Node) -> rpc_mnesia(Node, system_info, [is_running]) =:= yes.

%%------------------------------------------------------------------------------
%% @private
%% Add RAM copies of all `Tables' at `FromNode' to `ToNode'. Crashes, if a table
%% cannot be replicated.
%%------------------------------------------------------------------------------
add_table_copies(FromNode, ToNode, Tables) ->
    [ok = add_table_copy(FromNode, ToNode, T) || T <- Tables].

%%------------------------------------------------------------------------------
%% @private
%% Add a RAM copy of `Table' at `FromNode' to `ToNode'.
%%------------------------------------------------------------------------------
add_table_copy(FromNode, ToNode, Table) ->
    case rpc_mnesia(FromNode, add_table_copy, [Table, ToNode, ram_copies]) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, Table, ToNode}} ->
            ok;
        {aborted, Reason} ->
            {error, Reason};
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Returns the tables on `Node' that are managed by `lbm_kv'.
%%------------------------------------------------------------------------------
relevant_tables(Node) ->
    Tables = rpc_mnesia(Node, system_info, [tables]),
    [T || T <- Tables, is_relevant(Node, T)].

%%------------------------------------------------------------------------------
%% @private
%% Returns whether a table is managed by `lbm_kv'.
%%------------------------------------------------------------------------------
is_relevant(Node, Table) -> get_cookie(Node, Table) =:= ?LBM_KV_COOKIE.

%%------------------------------------------------------------------------------
%% @private
%% Returns the cookie of `Table'.
%%------------------------------------------------------------------------------
get_cookie(Node, Table) -> rpc_mnesia(Node, table_info, [Table, cookie]).

%%------------------------------------------------------------------------------
%% @private
%% Returns the current `running_db_nodes' as seen by `Node'.
%%------------------------------------------------------------------------------
get_running_nodes(Node) -> rpc_mnesia(Node, system_info, [running_db_nodes]).

%%------------------------------------------------------------------------------
%% @private
%% Returns the current `db_nodes' as seen by `Node'.
%%------------------------------------------------------------------------------
get_db_nodes(Node) -> rpc_mnesia(Node, system_info, [db_nodes]).

%%------------------------------------------------------------------------------
%% @private
%% Make an RPC call to the mnesia module on node `Node'. The `rpc' module knows
%% when a call is local and optimizes that.
%%------------------------------------------------------------------------------
rpc_mnesia(Node, Function, Args) ->
    Timeout = application:get_env(lbm_kv, rpc_timeout, ?LBM_KV_RPC_TIMEOUT),
    case rpc:call(Node, mnesia, Function, Args, Timeout) of
        {badrpc, Reason} -> {error, Reason};
        Result           -> Result
    end.

%%------------------------------------------------------------------------------
%% @private
%% Final conflict resolver for inconsistent database states, e.g. after
%% netsplits. This will simply restart the local node.
%%------------------------------------------------------------------------------
handle_unresolved_conflict() ->
    ?ERR("Final conflict resolution necessary, restarting ~s...~n", [node()]),
    init:restart().
