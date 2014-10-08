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
%%% Provides simple, Mnesia-based, distributed key value tables. When started,
%%% this application distributes Mnesia over all dynamically connected nodes.
%%%
%%% All tables created, have key/value semantic (after all its still Mnesia).
%%% A new key-value-table can be created locally using {@link create/1}. Another
%%% way to distribute RAM copies of a table is to replicate the table explicitly
%%% using {@link replicate_to/2}.
%%%
%%% Every connected node has read and write access to all Mnesia tables. There's
%%% no need to replicate a table locally. This should only be done for
%%% redundancy reasons.
%%%
%%% To be able to use `lbm_kv' none of the connected nodes is allowed to have
%%% `disk_copies' of its `schema' table, because Mnesia will fail to merge
%%% schemas on disk nodes (which means that it is likely they can't
%%% participate). If you need `disk_copies' (it can be brought to work) you're
%%% on your own here.
%%% @end
%%%=============================================================================

-module(lbm_kv).

-behaviour(application).
-behaviour(supervisor).

%% API
-export([create/1,
         replicate_to/2,
         put/3,
         get/2,
         get/3,
         get_all/1,
         update/3,
         update_all/2,
         subscribe/1,
         unsubscribe/1]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-type table() :: atom().
-type key()   :: term().
%% Unfortunately, Mnesia is quite picky when it comes to allowed types for
%% keys, e.g. all special atoms of `match_specs' are not allowed and lead to
%% undefined behaviour when used.
-type value() :: term().
%% Unfortunately, Mnesia is quite picky when it comes to allowed types for
%% values, e.g. all special atoms of `match_specs' are not allowed and lead to
%% undefined behaviour when used.

-export_type([table/0, key/0, value/0]).

%%%=============================================================================
%%% Behaviour
%%%=============================================================================

-callback resolve_conflict(node()) -> any().
%% Can be implemented by modules handling inconsistent DB state (as detected
%% after netplits). When a netsplit is detected for a certain table `tab',
%% {@link lbm_kv_mon} will look for the existence of `tab:resolve_conflict/1' to
%% resolve the conflict. If this is not found a default conflict resolver is
%% called. The default resolver will *restart* on of the conflicting nodes.

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link replicate_to/2} on the local node.
%% @end
%%------------------------------------------------------------------------------
-spec create(table()) -> ok | {error, term()}.
create(Table) -> replicate_to(Table, node()).

%%------------------------------------------------------------------------------
%% @doc
%% Replicate a RAM copy of a certain table to the given node. If the table
%% does not yet exist, it will be created. This can be called multiple times
%% (even) with the same node.
%%
%% The table will be ready for reads and writes when this function returns.
%% @end
%%------------------------------------------------------------------------------
-spec replicate_to(table(), node()) -> ok | {error, term()}.
replicate_to(Table, Node) ->
    case mnesia:create_table(Table, [{ram_copies, [Node]}]) of
        {atomic, ok} ->
            await_table(Table, Node);
        {aborted, {already_exists, Table}} ->
            case add_table_copy(Table, Node) of
                ok ->
                    await_table(Table, Node);
                Error ->
                    Error
            end;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Puts a key value pair into a table. Previous values for key will be
%% overridden.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec put(table(), key(), value()) -> ok | {error, term()}.
put(Table, Key, Value) -> do(fun() -> w(Table, Key, Value) end).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link get/3} with `Type' set to transaction.
%% @end
%%------------------------------------------------------------------------------
-spec get(table(), key()) -> [value()] | {error, term()}.
get(Table, Key) -> get(Table, Key, transaction).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves the value for the given key from a table. Specifying `dirty' will
%% issue a faster dirty read operation (no isolation/atomicity).
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec get(table(), key(), dirty | transaction) -> [value()] | {error, term()}.
get(Table, Key, dirty) ->
    [Val || {_, _, Val} <- mnesia:dirty_read(Table, Key)];
get(Table, Key, transaction) ->
    do(fun() -> r(Table, Key, read) end).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves all values from a table.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec get_all(table()) -> [{key(), value()}] | {error, term()}.
get_all(Table) -> do(fun() -> r_all(Table, read) end).

%%------------------------------------------------------------------------------
%% @doc
%% Updates mappings associated with `Key'. This function can be used to modify,
%% add or delete a mapping to `Key'.
%%
%% `Fun' will be called regardless whether a mapping currently exists or not.
%% The argument passed to the function is a list containing the values currently
%% associated with `Key'. In case of sets, this will usually be a list with one
%% or no entry.
%%
%% The returned list of values will be the new associations for `Key'. In case
%% of sets returning more than one value will cause all but the last value to
%% be ignored. In case the empty list is returned all associations will be
%% deleted.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec update(table(), key(), fun(([value()]) -> [value()])) ->
                    {ok, [value()]} | {error, term()}.
update(Table, Key, Fun) when is_function(Fun) ->
    do(fun() ->
               Values = r(Table, Key, write),
               {ok, w_and_d(Table, Key, Values, Fun(Values))}
       end).

%%------------------------------------------------------------------------------
%% @doc
%% Updates all mappings of a table. This function can be used to modify or
%% delete random mappings.
%%
%% `Fun' will be invoked consecutively for all table entries. In case of sets
%% `Fun' will be invoked exactly once per contained key. To modify a mapping
%% simply return `{ok, NewVal}', all other return values will cause the current
%% mapping to be deleted.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec update_all(table(), fun((key(), value()) -> {ok, value()} | any())) ->
                        {ok, [{key(), value()}]} | {error, term()}.
update_all(Table, Fun) when is_function(Fun) ->
    do(fun() ->
               {ok, [{K, NewV} || {K, V} <- r_all(Table, write),
                                  NewV <- w_or_d(Table, K, V, Fun(K, V))]}
       end).

%%------------------------------------------------------------------------------
%% @doc
%% Subscribe the caller for Mnesia events concerning the given table. The caller
%% must be prepared to receive messages of the form
%% `{mnesia_table_event, Event}'.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(table()) -> ok | {error, term()}.
subscribe(Table) -> manage_subscription(Table, subscribe).

%%------------------------------------------------------------------------------
%% @doc
%% Unsubscribe the caller from Mnesia events concerning the given table.
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(table()) -> ok | {error, term()}.
unsubscribe(Table) -> manage_subscription(Table, unsubscribe).

%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start(_StartType, _StartArgs) -> supervisor:start_link(?MODULE, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
stop(_State) -> ok.

%%%=============================================================================
%%% supervisor callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) -> {ok, {{one_for_one, 0, 1}, [spec(lbm_kv_mon, [])]}}.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
spec(M, As) -> {M, {M, start_link, As}, permanent, 1000, worker, [M]}.

%%------------------------------------------------------------------------------
%% @private
%% Add a RAM copy of a certain table on a certain node.
%%------------------------------------------------------------------------------
add_table_copy(Table, Node) ->
    case mnesia:add_table_copy(Table, Node, ram_copies) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, Table, Node}} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Blocks the calling process until a certain table is available to this node.
%%------------------------------------------------------------------------------
await_table(Table, Node) ->
    Timeout = application:get_env(?MODULE, wait_timeout, 10000),
    case mnesia:wait_for_tables([Table], Timeout) of
        ok ->
            lbm_kv_mon:add_table(Node, Table);
        {timeout, [Table]} ->
            {error, timeout};
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Subscribe or unsubscribe the caller for `simple' Mnesia table events
%% regarding a certain table.
%%------------------------------------------------------------------------------
manage_subscription(Table, Action) ->
    case mnesia:Action({table, Table, simple}) of
        {ok, _Node} ->
            ok;
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Spawns `Fun' in a mnesia transaction.
%%------------------------------------------------------------------------------
do(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Read `Key' from `Tab', only allowed within transaction context.
%%------------------------------------------------------------------------------
r(Tab, Key, Lock) -> [Val || {_, _, Val} <- mnesia:read(Tab, Key, Lock)].

%%------------------------------------------------------------------------------
%% @private
%% Read everything from `Tab', only allowed within transaction context.
%%------------------------------------------------------------------------------
r_all(Tab, Lock) ->
    Spec = [{{Tab, '_', '_'}, [], ['$_']}],
    [{Key, Val} || {_, Key, Val} <- mnesia:select(Tab, Spec, Lock)].

%%------------------------------------------------------------------------------
%% @private
%% Establish mapping `Key' to `Val'in `Tab', only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
w(Tab, Key, Val) -> mnesia:write({Tab, Key, Val}).

%%------------------------------------------------------------------------------
%% @private
%% Delete mapping `Key' to `Val' from `Tab' (if any), only allowed within
%% transaction context.
%%------------------------------------------------------------------------------
d(Tab, Key, Val) -> mnesia:delete_object({Tab, Key, Val}).

%%------------------------------------------------------------------------------
%% @private
%% Delete all mappings from `Key' to `Vals' and establish the new mappings `Key'
%% to `NewVals', only allowed within transaction context.
%%------------------------------------------------------------------------------
w_and_d(_Tab, _Key, Vals, Vals) ->
    Vals;
w_and_d(Tab, Key, Vals, NewVals) ->
    [d(Tab, Key, Val) || Val <- Vals -- NewVals],
    [w(Tab, Key, Val) || Val <- NewVals -- Vals],
    NewVals.

%%------------------------------------------------------------------------------
%% @private
%% Either replace the mapping `Key' to `Val' with `NewVal' (if `{ok, NewVal}' is
%% passed) or delete the mapping from `Key' to `Val', only allowed within
%% transaction context.
%%------------------------------------------------------------------------------
w_or_d(_Tab, _Key, Val, {ok, Val}) ->
    [Val];
w_or_d(Tab, Key, _Val, {ok, NewVal}) ->
    w(Tab, Key, NewVal),
    [NewVal];
w_or_d(Tab, Key, Val, _) ->
    d(Tab, Key, Val),
    [].
