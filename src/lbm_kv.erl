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
         put/2,
         del/3,
         del/2,
         get/2,
         get/3,
         get_all/1,
         update/3,
         update_all/2]).

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

-define(join(LofLs), lists:append(LofLs)).

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
%% Puts a key value pair into a table returning the previous mappings. In case
%% of table type `set' the previous mapping will be overridden if existed.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec put(table(), key(), value()) -> [{key(), value()}] | {error, term()}.
put(Table, Key, Value) -> ?MODULE:put(Table, [{Key, Value}]).

%%------------------------------------------------------------------------------
%% @doc
%% Puts multiple key value pairs into a table returning the previous mappings.
%% In case of table type `set' all previous mappings will be overridden.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec put(table(), [{key(), value()}]) -> [{key(), value()}] | {error, term()}.
put(_Table, []) ->
    [];
put(Table, KeyValues) when is_list(KeyValues) ->
    do(fun() -> ?join([r_and_w_all(Table, K, V) || {K, V} <- KeyValues]) end).

%%------------------------------------------------------------------------------
%% @doc
%% Deletes the mapping from `Key' to `Val' from a table. If the mapping existed
%% it will be returned in a list, if the mapping did not exist, the empty list
%% is returned.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec del(table(), key(), value()) -> [{key(), value()}] | {error, term()}.
del(Table, Key, Value) -> do(fun() -> r_and_d_exact(Table, Key, Value) end).

%%------------------------------------------------------------------------------
%% @doc
%% Deletes all values for the given list of keys from a table. Previous values
%% for the keys will be returned.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec del(table(), [key()]) -> [{key(), value()}] | {error, term()}.
del(_Table, []) ->
    [];
del(Table, Keys) when is_list(Keys) ->
    do(fun() -> ?join([r_and_d_all(Table, K) || K <- Keys]) end).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link get/3} with `Type' set to transaction.
%% @end
%%------------------------------------------------------------------------------
-spec get(table(), key()) -> [{key(), value()}] | {error, term()}.
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
                 [{key(), value()}] | {error, term()}.
get(Table, Key, dirty)       -> dirt_r_key(Table, Key);
get(Table, Key, transaction) -> do(fun() -> r_key(Table, Key, read) end).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves all values from a table.
%%
%% It is not necessary to have a local RAM copy to call this function.
%% @end
%%------------------------------------------------------------------------------
-spec get_all(table()) -> [{key(), value()}] | {error, term()}.
get_all(Table) -> do(fun() -> r_key(Table, '_', read) end).

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
                    [{key(), value()}] | {error, term()}.
update(Table, Key, Fun) when is_function(Fun) ->
    do(fun() ->
               Values = to_values(Key, r(Table, Key, write)),
               to_key_values(Key, d_and_w(Table, Key, Values, Fun(Values)))
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
                        [{key(), value()}] | {error, term()}.
update_all(Table, Fun) when is_function(Fun) ->
    do(fun() ->
               [{K, NewV} || {K, V} <- r_key(Table, '_', write),
                             NewV <- w_or_d(Table, K, V, Fun(K, V))]
       end).

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
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Read every entry matching `Key' from `Tab' in a dirty manner, no transaction
%% required.
%%------------------------------------------------------------------------------
dirt_r_key(Tab, Key) ->
    try mnesia:dirty_select(Tab, r_key_spec(Tab, Key)) of
        Entries -> [{K, V} || {_, K, V} <- Entries]
    catch
        exit:{aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Read `Key' from `Tab', only allowed within transaction context.
%%------------------------------------------------------------------------------
r(Tab, Key, Lock) -> [{K, V} || {_, K, V} <- mnesia:read(Tab, Key, Lock)].

%%------------------------------------------------------------------------------
%% @private
%% Read every entry matching `Key' from `Tab', only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
r_key(Tab, Key, Lock) ->
    [{K, V} || {_, K, V} <- mnesia:select(Tab, r_key_spec(Tab, Key), Lock)].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
r_key_spec(Tab, Key) -> [{{Tab, Key, '_'}, [], ['$_']}].

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
%% Read all mappings for `Key' in `Tab', delete them and return the previous
%% mappings. Only allowed within transaction context.
%%------------------------------------------------------------------------------
r_and_d_all(Tab, Key) ->
    KeyValues = r(Tab, Key, write),
    ok = mnesia:delete({Tab, Key}),
    KeyValues.

%%------------------------------------------------------------------------------
%% @private
%% Delete the exact mapping from `Key' to `Val' and delete it. Only allowed
%% within transaction context.
%%------------------------------------------------------------------------------
r_and_d_exact(Tab, Key, Val) ->
    KeyValues = r(Tab, Key, write),
    ok = d(Tab, Key, Val),
    [{K, V} || {K, V} <- KeyValues, K =:= Key, V =:= Val].

%%------------------------------------------------------------------------------
%% @private
%% Read all mappings for `Key' in `Tab', establish a new mapping from `Key' to
%% `Val' and return the previous mappings. Only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
r_and_w_all(Tab, Key, Val) ->
    KeyValues = r(Tab, Key, write),
    ok = w(Tab, Key, Val),
    KeyValues.

%%------------------------------------------------------------------------------
%% @private
%% Delete all mappings from `Key' to `Vals' and establish the new mappings `Key'
%% to `NewVals', only allowed within transaction context.
%%------------------------------------------------------------------------------
d_and_w(_Tab, _Key, Vals, Vals) ->
    Vals;
d_and_w(Tab, Key, Vals, NewVals) ->
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

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
to_values(Key, KeyValues) -> [Value || {K, Value} <- KeyValues, K =:= Key].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
to_key_values(Key, Values) -> [{Key, Value} || Value <- Values].
