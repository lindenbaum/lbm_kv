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
%%% A registered server that dynamically adds copies of the Mnesia `schema'
%%% to connected nodes. The server also subscribes for Mnesia system events. In
%%% case a DB inconsistency is detected (split brain) the server tries to
%%% resolve the conflict by calling the table module's `resolve_conflict/1'
%%% function. If no such function is exported/found, the conflict will be
%%% resolved by restarting one of the conflicting nodes.
%%% @end
%%%=============================================================================

-module(lbm_kv_mon).

-behaviour(gen_server).

%% Internal API
-export([start_link/0,
         add_table/2]).

%% gen_server callbacks
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         code_change/3,
         terminate/2]).

-define(DUMP,           "mnesia_core.dump").
-define(ERR(Fmt, Args), error_logger:error_msg(Fmt, Args)).

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
%% Called from `lbm_kv' when a table has been replicated to this node.
%%------------------------------------------------------------------------------
-spec add_table(node(), atom()) -> ok.
add_table(Node, Table) -> gen_server:cast({?MODULE, Node}, {add_table, Table}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(state, {
          nodes = []  :: [node()],
          tables = [] :: [atom()]}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    {ok, _} = mnesia:subscribe(system),
    case application:get_env(lbm_kv, include_hidden, false) of
        false ->
            Nodes = [{nodeup, Node} || Node <- nodes(visible)],
            ok = net_kernel:monitor_nodes(true, [{node_type, visible}]);
        true ->
            Nodes = [{nodeup, Node} || Node <- nodes(connected)],
            ok = net_kernel:monitor_nodes(true, [{node_type, all}])
    end,
    {ok, lists:foldl(fun handle_node_event/2, #state{}, Nodes)}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(_Request, _From, State) -> {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({add_table, Table}, State = #state{tables = Tables}) ->
    {noreply, State#state{tables = lists:usort([Table | Tables])}};
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({mnesia_system_event, Event}, State) ->
    handle_mnesia_event(Event, State),
    {noreply, State};
handle_info({nodeup, Node, _InfoList}, State) ->
    {noreply, handle_node_event({nodeup, Node}, State)};
handle_info({nodedown, Node, _InfoList}, State) ->
    {noreply, handle_node_event({nodedown, Node}, State)};
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
%% restart. Inconsistent database states will be delegated to table specific
%% handlers (if any).
%%------------------------------------------------------------------------------
handle_mnesia_event({inconsistent_database, _Context, Node}, State) ->
    [resolve_conflict(Node, Table) || Table <- conflicting_tables(State)];
handle_mnesia_event({mnesia_fatal, _Format, _Args, BinaryCore}, _State) ->
    ?ERR("FATAL CONDITION (restarting ~p to recover)", [node()]),
    file:write_file(?DUMP, BinaryCore),
    init:restart();
handle_mnesia_event(_Event, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_node_event({nodeup, Node}, State = #state{nodes = Nodes}) ->
    mnesia:change_config(extra_db_nodes, [Node | Nodes]),
    State#state{nodes = [Node | Nodes]};
handle_node_event({nodedown, Node}, State = #state{nodes = Nodes}) ->
    mnesia:change_config(extra_db_nodes, Nodes -- [Node]),
    State#state{nodes = Nodes -- [Node]}.

%%------------------------------------------------------------------------------
%% @private
%% Return all `lbm_kv' tables with local RAM copies.
%%------------------------------------------------------------------------------
conflicting_tables(#state{tables = Tables}) ->
    LocalTables = mnesia:table_info(schema, local_tables),
    [Table || Table <- LocalTables, lists:member(Table, Tables)].

%%------------------------------------------------------------------------------
%% @private
%% Call the conflict handler for a table along with the offending node. If no
%% conflict handler is found (exported function `resolve_conflict/1') the
%% default conflict handler is invoked.
%%------------------------------------------------------------------------------
resolve_conflict(Node, Table) ->
    case code:ensure_loaded(Table) of
        {module, Table} ->
            case erlang:function_exported(Table, resolve_conflict, 1) of
                true ->
                    Table:resolve_conflict(Node);
                false ->
                    default_resolve_conflict(Node)
            end;
        _ ->
            default_resolve_conflict(Node)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Default conflict resolver for inconsistent database states, e.g. after
%% netsplits. This will compare the local node with the offending node with
%% {@link erlang:'>'/2} and restart the greater node.
%%------------------------------------------------------------------------------
default_resolve_conflict(Node) ->
    case node() > Node of
        true ->
            ?ERR("NETSPLIT DETECTED (restarting ~p to resolve)", [node()]),
            init:restart();
        _ ->
            ok
    end.
