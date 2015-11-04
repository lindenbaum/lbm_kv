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
%%% A word about when to call {@link create/1} or {@link replicate_to/2}:
%%% When using RAM copies, Mnesia is able to `merge' schema tables of different
%%% nodes, as long as one of the schema's to merge is clean (no tables created
%%% yet). This has implications on the timing of table distribution. The above
%%% mentioned functions should only be called after the respective nodes
%%% connected. If e.g. {@link create/1} is called on two nodes independently
%%% before these nodes have a `net_kernel' connection, the two tables can't be
%%% merged and will stay independent, resulting in the condition that one of the
%%% nodes will not be able to take part in Mnesia distribution!
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
         put/2,
         del/2,
         get/2,
         get/3,
         match/2,
         match/3,
         update/2,
         update/3,
         info/0]).

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
-opaque version() :: lbm_kv_vclock:vclock().
%% An opaque type describing the version of a table entry.

-type update_fun() :: fun((key(), {value, value()} | undefined) ->
                                 {value, value()} | term()).
%% The definition for a function passed to {@link update/2,3}. If there is no
%% mapping associated with a key the atom `undefined' is passed to the function,
%% otherwise the value will be provided as the tuple `{value, Value}'. To add
%% a not yet existing or change an existing mapping the function must return a
%% tuple of the similar form. Any other return value will delete the mapping
%% (if any).

-export_type([table/0, key/0, value/0, version/0, update_fun/0]).

-define(join(LofLs), lists:append(LofLs)).

-include("lbm_kv.hrl").

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
    Options = [{ram_copies, [Node]}, ?LBM_KV_ATTRIBUTES],
    case mnesia:create_table(Table, Options) of
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
%% Puts a key value pair into a table returning the previous mappings. The
%% previous mapping will be overridden if existed.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec put(table(), key(), value()) ->
                 {ok, [{key(), value()}]} | {error, term()}.
put(Table, Key, Value) -> ?MODULE:put(Table, [{Key, Value}]).

%%------------------------------------------------------------------------------
%% @doc
%% Puts multiple key value pairs into a table returning the previous mappings.
%% All previous mappings will be overridden.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec put(table(), [{key(), value()}]) ->
                 {ok, [{key(), value()}]} | {error, term()}.
put(_Table, []) ->
    [];
put(Table, KeyValues) when is_list(KeyValues) ->
    do(fun() -> strip_vs(r_and_w(Table, KeyValues)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Deletes all values for the given key or list of keys from a table. Previous
%% values for the keys will be returned.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec del(table(), key() | [key()]) ->
                 {ok, [{key(), value()}]} | {error, term()}.
del(_Table, []) ->
    [];
del(Table, KeyOrKeys) ->
    do(fun() -> strip_vs(r_and_d(Table, KeyOrKeys)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link get/3} with `Type' set to transaction.
%% @end
%%------------------------------------------------------------------------------
-spec get(table(), key()) -> {ok, [{key(), value()}]} | {error, term()}.
get(Table, Key) -> get(Table, Key, transaction).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves the value for the given key from a table. Specifying `dirty' will
%% issue a faster dirty read operation (no isolation/atomicity).
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec get(table(), key(), dirty | transaction) ->
                 {ok, [{key(), value()}]} | {error, term()}.
get(Table, Key, dirty)       -> dirty_r(Table, Key);
get(Table, Key, transaction) -> do(fun() -> strip_vs(r(Table, Key, read)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link match/3} with `Type' set to transaction.
%% @end
%%------------------------------------------------------------------------------
-spec match(table(), key()) -> {ok, [{key(), value()}]} | {error, term()}.
match(Table, KeySpec) -> match(Table, KeySpec, transaction).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves the value that match the given key spec from a table. Specifying
%% `dirty' will issue a faster dirty select operation (no isolation/atomicity).
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec match(table(), key(), dirty | transaction) ->
                   {ok, [{key(), value()}]} | {error, term()}.
match(Table, KeySpec, dirty) ->
    dirty_m(Table, KeySpec);
match(Table, KeySpec, transaction) ->
    do(fun() -> strip_vs(m(Table, KeySpec, read)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Updates all mappings of a table. This function can be used to modify or
%% delete random mappings.
%%
%% `Fun' will be invoked consecutively for all table entries and will be invoked
%% exactly once per contained key. To modify a mapping simply return
%% `{value, NewVal}', to preserve the current mapping just return a tuple with
%% the old value. All other return values will cause the current mapping to be
%% deleted. {@link update/2} returns a list with all previous mappings.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec update(table(), update_fun()) ->
                    {ok, [{key(), value()}]} | {error, term()}.
update(Table, Fun) when is_function(Fun) ->
    do(fun() -> strip_vs(u(Table, Fun)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Updates the mapping associated with `Key'. This function can be used to
%% modify, add or delete a mapping to `Key'.
%%
%% `Fun' will be called regardless whether a mapping currently exists or not.
%% The argument passed to the function is either `{value, Value}' or `undefined'
%% if there is no mapping at the moment.
%%
%% To add or change a mapping return `{value, NewValue}'. Returning this with
%% the old value will simply preserve the mapping. Returning anything else will
%% remove the mapping from the table. {@link update/3} returns a list with the
%% previous mappings for `Key'.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec update(table(), key(), update_fun()) ->
                    {ok, [{key(), value()}]} | {error, term()}.
update(Table, Key, Fun) when is_function(Fun) ->
    do(fun() -> strip_vs(u(Table, Fun, Key)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Print information about the `lbm_kv' state to stdout.
%% @end
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() -> lbm_kv_mon:info().

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
%% Spawns `Fun' in a mnesia transaction.
%%------------------------------------------------------------------------------
do(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, Result} ->
            {ok, Result};
        {aborted, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Get every entry associated with `Key' from `Tab' in a dirty manner, no
%% transaction required.
%%------------------------------------------------------------------------------
-spec dirty_r(table(), key()) -> [{key(), value()}] | {error, term()}.
dirty_r(Tab, Key) -> dirty(Tab, Key, dirty_read).

%%------------------------------------------------------------------------------
%% @private
%% Select every entry matching `Key' from `Tab' in a dirty manner, no
%% transaction required.
%%------------------------------------------------------------------------------
-spec dirty_m(table(), key()) -> [{key(), value()}] | {error, term()}.
dirty_m(Tab, KeySpec) -> dirty(Tab, m_spec(Tab, KeySpec), dirty_select).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec dirty(table(), key(), dirty_read | dirty_select) ->
                   [{key(), value()}] | {error, term()}.
dirty(Tab, Key, Function) ->
    try mnesia:Function(Tab, Key) of
        Entries -> {ok, [{K, V} || ?LBM_KV_LONG(_, K, V, _) <- Entries]}
    catch
        exit:{aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Read `Key' from `Tab', only allowed within transaction context.
%%------------------------------------------------------------------------------
-spec r(table(), key(), read | write) -> [{key(), value(), version()}].
r(Tab, Key, Lock) ->
    [?LBM_KV_SHORT(K, V, Vs)
     || ?LBM_KV_LONG(_, K, V, Vs)
            <- mnesia:read(Tab, Key, Lock)].

%%------------------------------------------------------------------------------
%% @private
%% Read every entry matching `Key' from `Tab', only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
-spec m(table(), key(), read | write) -> [{key(), value(), version()}].
m(Tab, KeySpec, Lock) ->
    [?LBM_KV_SHORT(K, V, Vs)
     || ?LBM_KV_LONG(_, K, V, Vs)
            <- mnesia:select(Tab, m_spec(Tab, KeySpec), Lock)].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec m_spec(table(), key()) -> [tuple()].
m_spec(Tab, KeySpec) -> [{?LBM_KV_LONG(Tab, KeySpec, '_', '_'), [], ['$_']}].

%%------------------------------------------------------------------------------
%% @private
%% Establish mapping `Key' to `Val'in `Tab', only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
-spec w(table(), key(), value(), version()) -> ok.
w(Tab, Key, Val, Vs) ->
    NewVs = lbm_kv_vclock:increment(node(), Vs),
    mnesia:write({Tab, Key, Val, NewVs}).

%%------------------------------------------------------------------------------
%% @private
%% Delete mapping `Key' to `Val' from `Tab' (if any), only allowed within
%% transaction context.
%%------------------------------------------------------------------------------
-spec d(table(), key(), read | write) -> ok.
d(Tab, Key, Lock) -> mnesia:delete(Tab, Key, Lock).

%%------------------------------------------------------------------------------
%% @private
%% Read all mappings for `Key' or `Keys' in `Tab', delete them and return the
%% previous mappings. Only allowed within transaction context.
%%------------------------------------------------------------------------------
-spec r_and_d(table(), key() | [key()]) -> [{key(), value(), version()}].
r_and_d(Tab, Keys) when is_list(Keys) ->
    ?join([r_and_d(Tab, Key) || Key <- Keys]);
r_and_d(Tab, Key) ->
    KeyValueVersions = r(Tab, Key, write),
    ok = d(Tab, Key, write),
    KeyValueVersions.

%%------------------------------------------------------------------------------
%% @private
%% Similar to {@link r_and_w/3} but operates on an input list.
%%------------------------------------------------------------------------------
-spec r_and_w(table(), [{key(), value()}]) -> [{key(), value(), version()}].
r_and_w(Tab, KeyValues) ->
    ?join([r_and_w(Tab, Key, Val) || {Key, Val} <- KeyValues]).

%%------------------------------------------------------------------------------
%% @private
%% Read all mappings for `Key' in `Tab', establish a new mapping from `Key' to
%% `Val' and return the previous mappings. Only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
-spec r_and_w(table(), key(), value()) -> [{key(), value(), version()}].
r_and_w(Tab, Key, Val) ->
    case r(Tab, Key, write) of
        KeyValueVersions = [?LBM_KV_SHORT(Key, Val, _) | _] ->
            ok; %% no change, no write
        KeyValueVersions = [?LBM_KV_SHORT(Key, _, Vs) | _] ->
            ok = w(Tab, Key, Val, Vs);
        KeyValueVersions = [] ->
            ok = w(Tab, Key, Val, lbm_kv_vclock:fresh())
    end,
    KeyValueVersions.


%%------------------------------------------------------------------------------
%% @private
%% Update the mappings for all `Key's in a table. Only allowed within
%% transaction context.
%%------------------------------------------------------------------------------
-spec u(table(), update_fun()) -> [{key(), value(), version()}].
u(Tab, Fun) -> ?join([u(Tab, Fun, Key) || Key <- mnesia:all_keys(Tab)]).

%%------------------------------------------------------------------------------
%% @private
%% Update the mapping for `Key'. Only allowed within transaction context.
%%------------------------------------------------------------------------------
-spec u(table(), update_fun(), key()) -> [{key(), value(), version()}].
u(Tab, Fun, Key) ->
    case r(Tab, Key, write) of
        KeyValueVersions = [?LBM_KV_SHORT(Key, Val, Vs) | _] ->
            case Fun(Key, {value, Val}) of
                {value, Val} ->
                    ok; %% no change, no write
                {value, NewVal} ->
                    ok = d(Tab, Key, write),
                    ok = w(Tab, Key, NewVal, Vs);
                _ ->
                    ok = d(Tab, Key, write)
            end;
        KeyValueVersions = [] ->
            case Fun(Key, undefined) of
                {value, Val} ->
                    ok = w(Tab, Key, Val, lbm_kv_vclock:fresh());
                _ ->
                    ok
            end
    end,
    KeyValueVersions.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
strip_vs(KeyValueVersions) ->
    [{K, V} || ?LBM_KV_SHORT(K, V, _) <- KeyValueVersions].
