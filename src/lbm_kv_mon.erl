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

-define(DUMP,           "mnesia_core.dump").
-define(ERR(Fmt, Args), error_logger:error_msg(Fmt, Args)).
-define(RPC_TIMEOUT,    2000).

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
%% Expand the mnesia cluster to `Node'. This will add table copies of local
%% tables to `Node' as well as table copies remote tables on the local node.
%% This is the function handling dynamic expansion of mnesia on new nodes.
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
%%------------------------------------------------------------------------------
expand(Node) ->
    case is_running(Node) of
        true ->
            ?LBM_KV_DBG("~s is about to expand to ~s~n", [node(), Node]),
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
            ?LBM_KV_DBG("Schema on ~s: ~w~n", [node(), LocalSchema]),
            ?LBM_KV_DBG("Schema on ~s: ~w~n", [Node, RemoteSchema]),
            case LocalSchema =:= RemoteSchema of
                true ->
                    %% Schemas are already merged, someone else did the work.
                    ?LBM_KV_DBG("Schemas already merged~n", []),

                    %% This also means that the respective remote node must be
                    %% part of the node's `running_db_nodes'.
                    true = lists:member(Node, get_running_nodes(node())),
                    true = lists:member(node(), get_running_nodes(Node)),

                    %% Now distribute tables only found on one of the nodes.
                    ok = add_table_copies(node(), Node, LocalOnlyTables),
                    ok = add_table_copies(Node, node(), RemoteOnlyTables),
                    ok;
                false ->
                    %% The newly connected node has a different schema. That
                    %% means the schemas must be merged now.
                    ?LBM_KV_DBG("Schemas need merge~n", []),

                    %% All duplicate tables must be interconnected (with
                    %% mutual RAM copies) __before__ connecting the nodes.
                    LocalAndRemoteTables = LocalTables -- LocalOnlyTables,
                    ?LBM_KV_DBG("Tables to merge: ~w~n", [LocalAndRemoteTables]),
                    ok = add_table_copies(node(), Node, LocalAndRemoteTables),
                    ok = add_table_copies(Node, node(), LocalAndRemoteTables),

                    %% Connect both nodes and merge values of duplicate tables.
                    Fun = connect_nodes(LocalAndRemoteTables, Node),
                    {ok, [Node]} = mnesia_controller:connect_nodes([Node], Fun),

                    %% Now distribute tables only found on one of the nodes.
                    ok = add_table_copies(node(), Node, LocalOnlyTables),
                    ok = add_table_copies(Node, node(), RemoteOnlyTables),
                    ok
            end;
        false ->
            ok %% Do not expand to the remote node if mnesia is not running.
    end.

%%------------------------------------------------------------------------------
%% @private
%% A custom user function that merges the schema of two nodes as well as the
%% respective values in conflicting tables.
%%------------------------------------------------------------------------------
connect_nodes(TablesToMerge, Node) ->
    fun(SchemaMergeFun) ->
            ?LBM_KV_DBG("Merging schemas on node ~s...~n", [node()]),
            case SchemaMergeFun(TablesToMerge) of
                Result = {merged, OldFriends, NewFriends} ->
                    ?LBM_KV_DBG("OldFriends: ~w, NewFriends: ~w~n", [OldFriends, NewFriends]),
                    case lbm_kv_merge:tables(TablesToMerge, Node) of
                        ok ->
                            Result;
                        {error, Reason} ->
                            ?ERR("Failed to merge tables: ~p~n", [Reason]),
                            ok = final_resolve_conflict(Node),
                            Result
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
            mnesia:del_table_copy(schema, Node),
            ok;
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
%% Add RAM copies of all `Tables' at `FromNode' to `ToNode'.
%%------------------------------------------------------------------------------
add_table_copies(FromNode, ToNode, Tables) ->
    [ok = add_table_copy(FromNode, ToNode, T) || T <- Tables],
    ok.

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
        {badrpc, Reason} ->
            {error, Reason}
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
    rpc:call(Node, mnesia, Function, Args, ?RPC_TIMEOUT).

%%------------------------------------------------------------------------------
%% @private
%% Final conflict resolver for inconsistent database states, e.g. after
%% netsplits. This will compare the local node with the offending node with
%% {@link erlang:'>'/2} and restart the greater node.
%%------------------------------------------------------------------------------
final_resolve_conflict(Node) ->
    case node() > Node of
        true ->
            ?ERR("Final conflict resolution, restarting ~s~n", [node()]),
            init:restart();
        _ ->
            ok
    end.























%%------------------------------------------------------------------------------
%% @private
%% Handle Mnesia system events. Fatal conditions will be resolved with node
%% restart. Inconsistent database states will be delegated to table specific
%% handlers (if any).
%%------------------------------------------------------------------------------
handle_mnesia_event({mnesia_fatal, _Format, _Args, BinaryCore}, _State) ->
    ?ERR("FATAL CONDITION (restarting ~p to recover)", [node()]),
    file:write_file(?DUMP, BinaryCore),
    init:restart();
handle_mnesia_event({mnesia_info, Format, Args}, _State) ->
    error_logger:info_msg(Format, Args);
handle_mnesia_event({inconsistent_database, _Context, _Node}, _State) ->
    %% TODO merge all values
    ok;
%%    [resolve_conflict(Node, Table) || Table <- conflicting_tables(State)];
handle_mnesia_event(_Event, _State) ->
    ok.

%% %%------------------------------------------------------------------------------
%% %% @private
%% %% Return all `lbm_kv' tables with local RAM copies.
%% %%------------------------------------------------------------------------------
%% conflicting_tables(#state{tables = Tables}) ->
%%     LocalTables = mnesia:table_info(schema, local_tables),
%%     [Table || Table <- LocalTables, lists:member(Table, Tables)].

%% %%------------------------------------------------------------------------------
%% %% @private
%% %% Call the conflict handler for a table along with the offending node. If no
%% %% conflict handler is found (exported function `resolve_conflict/1') the
%% %% default conflict handler is invoked.
%% %%------------------------------------------------------------------------------
%% resolve_conflict(Node, Table) ->
%%     case code:ensure_loaded(Table) of
%%         {module, Table} ->
%%             case erlang:function_exported(Table, resolve_conflict, 1) of
%%                 true ->
%%                     Table:resolve_conflict(Node);
%%                 false ->
%%                     default_resolve_conflict(Node)
%%             end;
%%         _ ->
%%             default_resolve_conflict(Node)
%%     end.

%% %%------------------------------------------------------------------------------
%% %% @private
%% %% Default conflict resolver for inconsistent database states, e.g. after
%% %% netsplits. This will compare the local node with the offending node with
%% %% {@link erlang:'>'/2} and restart the greater node.
%% %%------------------------------------------------------------------------------
%% default_resolve_conflict(Node) ->
%%     case node() > Node of
%%         true ->
%%             ?ERR("NETSPLIT DETECTED (offender ~p, restarting ~p to resolve)",
%%                  [Node, node()]),
%%             init:restart();
%%         _ ->
%%             ok
%%     end.
