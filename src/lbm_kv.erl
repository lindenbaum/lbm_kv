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
%%% this application distributes Mnesia (and all `lbm_kv' tables) over all
%%% dynamically connected nodes. The Mnesia cluster can grow and shrink
%%% dynamically.
%%%
%%% All tables created, have key/value semantic (after all its still Mnesia).
%%% A new key-value-table can be created using {@link create/1}. The table will
%%% automatically be replicated to other nodes as new node connections are
%%% detected. Every connected node has read and write access to all Mnesia
%%% tables. If desired, it is possible to use the default Mnesia API to
%%% manipulate `lbm_kv' tables. However, `lbm_kv' uses vector clocks that need
%%% to be updated on every write to be able to use automatic netsplit recovery!
%%% Use the `#lbm_kv{}' record from the `lbm_kv.hrl' header file to match
%%% `lbm_kv' table entries.
%%%
%%% Every `lbm_kv' table uses vector clocks to keep track of the its entries.
%%% In case of new node connections or netsplits, `lbm_kv' will use these to
%%% merge the island itself without interaction. However, if there are diverged
%%% entries `lbm_kv' will look for a user defined callback to resolve the
%%% conflict. If no such callback can be found one of the conflicting nodes will
%%% be restarted!
%%%
%%% To be able to use `lbm_kv' none of the connected nodes is allowed to have
%%% `disk_copies' of its `schema' table, because Mnesia will fail to merge
%%% schemas on disk nodes (which means that it is likely they can't
%%% participate). If you need `disk_copies', you're on your own here. Do not
%%% mess with table replication and mnesia configuration changes yourself!
%%% There's a lot of black magic happening inside Mnesia and `lbm_kv' will do
%%% the necessary tricks and workarounds for you. At best you should avoid
%%% having tables created from outside `lbm_kv'. At least do not create tables
%%% with conflicting names.
%%% @end
%%%=============================================================================

-module(lbm_kv).

-behaviour(application).
-behaviour(supervisor).

%% API
-export([create/1,
         put/3,
         put/2,
         del/2,
         get/2,
         get/3,
         match_key/2,
         match_key/3,
         match/3,
         match/4,
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
-type version() :: lbm_kv_vclock:vclock().
%% A type describing the version of a table entry.

-type update_fun() :: fun((key(), {value, value()} | undefined) ->
                                 {value, value()} | term()).
%% The definition for a function passed to {@link update/2,3}. If there is no
%% mapping associated with a key the atom `undefined' is passed to the function,
%% otherwise the value will be provided as the tuple `{value, Value}'. To add
%% a not yet existing or change an existing mapping the function must return a
%% tuple of the similar form. Any other return value will delete the mapping
%% (if any).

-export_type([table/0, key/0, value/0, version/0, update_fun/0]).

-include("lbm_kv.hrl").

%%%=============================================================================
%%% Behaviour
%%%=============================================================================

-callback resolve_conflict(key(), Local :: value(), Remote :: value()) ->
    {value, value()} | delete | term().
%% An optional callback that will be called on the node performing a table
%% merge (usually an arbitrary node) whenever an entry of table cannot be
%% merged automatically. The callback must be implemented in a module with the
%% same name as the respective table name, e.g. to handle conflicts for values
%% in the table `my_table' the module/function `my_table:resolve_conflict/3' has
%% to be implemented.
%%
%% The function can resolve conflicts in several ways. It can provide a (new)
%% value for `Key' by returning `{value, Val}', it can delete all associations
%% for `Key' on all nodes by returning `delete' or it can ignore the
%% inconsistency by returning anything else or crash. When ignoring an
%% inconsistency the values for key will depend on the location of retrieval
%% until a new value gets written for `Key'.
%%
%% If an appropriate callback is not provided, the default conflict resolution
%% strategy is to __restart__ one of the conflicting node islands!

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Create a new key value table which will be replicated as RAM copy across all
%% nodes in the cluster. The table will only be created, if not yet existing.
%% This can be called multiple times (even) on the same node.
%%
%% The table will be ready for reads and writes when this function returns.
%% @end
%%------------------------------------------------------------------------------
-spec create(table()) -> ok | {error, term()}.
create(Table) ->
    case mnesia:create_table(Table, ?LBM_KV_TABLE_OPTS()) of
        {atomic, ok} ->
            await_table(Table);
        {aborted, {already_exists, Table}} ->
            await_table(Table);
        {aborted, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Puts a key value pair into a table returning the previous mappings. The
%% previous mapping will be overridden if existed.
%% @end
%%------------------------------------------------------------------------------
-spec put(table(), key(), value()) ->
                 {ok, [{key(), value()}]} | {error, term()}.
put(Table, Key, Value) -> ?MODULE:put(Table, [{Key, Value}]).

%%------------------------------------------------------------------------------
%% @doc
%% Puts multiple key value pairs into a table returning the previous mappings.
%% All previous mappings will be overridden.
%% @end
%%------------------------------------------------------------------------------
-spec put(table(), [{key(), value()}]) ->
                 {ok, [{key(), value()}]} | {error, term()}.
put(_Table, []) ->
    [];
put(Table, KeyValues) when is_list(KeyValues) ->
    do(fun() -> strip(r_and_w(Table, KeyValues)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Deletes all values for the given key or list of keys from a table. Previous
%% values for the keys will be returned.
%% @end
%%------------------------------------------------------------------------------
-spec del(table(), key() | [key()]) ->
                 {ok, [{key(), value()}]} | {error, term()}.
del(_Table, [])       -> [];
del(Table, KeyOrKeys) -> do(fun() -> strip(r_and_d(Table, KeyOrKeys)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link get/3} with `Type' set to transaction.
%% @end
%%------------------------------------------------------------------------------
-spec get(table(), key()) -> {ok, [{key(), value()}]} | {error, term()}.
get(Table, Key) -> get(Table, Key, transaction).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves the entry for the given key from a table. Specifying `dirty' will
%% issue a faster dirty read operation (no isolation/atomicity).
%% @end
%%------------------------------------------------------------------------------
-spec get(table(), key(), dirty | transaction) ->
                 {ok, [{key(), value()}]} | {error, term()}.
get(Table, Key, dirty)       -> dirty_r(Table, Key);
get(Table, Key, transaction) -> do(fun() -> strip(r(Table, Key, read)) end).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link match_key/3} with `Type' set to transaction.
%% @end
%%------------------------------------------------------------------------------
-spec match_key(table(), key()) -> {ok, [{key(), value()}]} | {error, term()}.
match_key(Table, KeySpec) -> match_key(Table, KeySpec, transaction).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves the entries that match the given key spec from a table. Specifying
%% `dirty' will issue a faster dirty select operation (no isolation/atomicity).
%% @end
%%------------------------------------------------------------------------------
-spec match_key(table(), key(), dirty | transaction) ->
                       {ok, [{key(), value()}]} | {error, term()}.
match_key(Table, KeySpec, Type) -> match(Table, KeySpec, '_', Type).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link match/4} with `Type' set to transaction.
%% @end
%%------------------------------------------------------------------------------
-spec match(table(), key(), value()) ->
                   {ok, [{key(), value()}]} | {error, term()}.
match(Table, KeySpec, ValueSpec) ->
    match(Table, KeySpec, ValueSpec, transaction).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieves the entries that match the given key and value spec from a table.
%% Specifying `dirty' will issue a faster dirty select operation (no
%% isolation/atomicity).
%% @end
%%------------------------------------------------------------------------------
-spec match(table(), key(), value(), dirty | transaction) ->
                   {ok, [{key(), value()}]} | {error, term()}.
match(Table, KeySpec, ValueSpec, dirty) ->
    dirty_m(Table, KeySpec, ValueSpec);
match(Table, KeySpec, ValueSpec, transaction) ->
    do(fun() -> strip(m(Table, KeySpec, ValueSpec, read)) end).

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
%% @end
%%------------------------------------------------------------------------------
-spec update(table(), update_fun()) ->
                    {ok, [{key(), value()}]} | {error, term()}.
update(Table, Fun) when is_function(Fun) ->
    do(fun() -> strip(u(Table, Fun)) end).

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
%% @end
%%------------------------------------------------------------------------------
-spec update(table(), key(), update_fun()) ->
                    {ok, [{key(), value()}]} | {error, term()}.
update(Table, Key, Fun) when is_function(Fun) ->
    do(fun() -> strip(u(Table, Fun, Key)) end).

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
init([]) -> {ok, {{one_for_one, 5, 1}, [spec(lbm_kv_mon, [])]}}.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
spec(M, As) -> {M, {M, start_link, As}, permanent, 1000, worker, [M]}.

%%------------------------------------------------------------------------------
%% @private
%% Blocks the calling process until a certain table is available to this node.
%%------------------------------------------------------------------------------
await_table(Table) ->
    Timeout = application:get_env(?MODULE, wait_timeout, 10000),
    case mnesia:wait_for_tables([Table], Timeout) of
        ok ->
            ok;
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
%% Select every entry matching the given key and value spec from `Tab' in a
%% dirty manner, no transaction required.
%%------------------------------------------------------------------------------
-spec dirty_m(table(), key(), value()) -> [{key(), value()}] | {error, term()}.
dirty_m(Tab, KeySpec, ValueSpec) ->
    dirty(Tab, m_spec(KeySpec, ValueSpec), dirty_select).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec dirty(table(), key(), dirty_read | dirty_select) ->
                   [{key(), value()}] | {error, term()}.
dirty(Tab, Key, Function) ->
    try mnesia:Function(Tab, Key) of
        Records -> {ok, strip(Records)}
    catch
        exit:{aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Read `Key' from `Tab', only allowed within transaction context.
%%------------------------------------------------------------------------------
-spec r(table(), key(), read | write) -> [#lbm_kv{}].
r(Tab, Key, Lock) -> mnesia:read(Tab, Key, Lock).

%%------------------------------------------------------------------------------
%% @private
%% Read every entry matching the given key and value specs from `Tab', only
%% allowed within transaction
%% context.
%%------------------------------------------------------------------------------
-spec m(table(), key(), value(), read | write) -> [#lbm_kv{}].
m(Tab, KeySpec, ValueSpec, Lock) ->
    mnesia:select(Tab, m_spec(KeySpec, ValueSpec), Lock).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec m_spec(key(), value()) -> [tuple()].
m_spec(KeySpec, ValueSpec) ->
    [{#lbm_kv{key = KeySpec, val = ValueSpec, _ = '_'}, [], ['$_']}].

%%------------------------------------------------------------------------------
%% @private
%% Establish mapping `Key' to `Val'in `Tab', only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
-spec w(table(), key(), value(), version()) -> ok.
w(Tab, Key, Val, Ver) ->
    NewVer = lbm_kv_vclock:increment(node(), Ver),
    mnesia:write(Tab, #lbm_kv{key = Key, val = Val, ver = NewVer}, write).

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
-spec r_and_d(table(), key() | [key()]) -> [#lbm_kv{}].
r_and_d(Tab, Keys) when is_list(Keys) ->
    lists:append([r_and_d(Tab, Key) || Key <- Keys]);
r_and_d(Tab, Key) ->
    Records = r(Tab, Key, write),
    ok = d(Tab, Key, write),
    Records.

%%------------------------------------------------------------------------------
%% @private
%% Similar to {@link r_and_w/3} but operates on an input list.
%%------------------------------------------------------------------------------
-spec r_and_w(table(), [{key(), value()}]) -> [#lbm_kv{}].
r_and_w(Tab, KeyValues) ->
    lists:append([r_and_w(Tab, Key, Val) || {Key, Val} <- KeyValues]).

%%------------------------------------------------------------------------------
%% @private
%% Read all mappings for `Key' in `Tab', establish a new mapping from `Key' to
%% `Val' and return the previous mappings. Only allowed within transaction
%% context.
%%------------------------------------------------------------------------------
-spec r_and_w(table(), key(), value()) -> [#lbm_kv{}].
r_and_w(Tab, Key, Val) ->
    case r(Tab, Key, write) of
        Records = [#lbm_kv{key = Key, val = Val} | _] ->
            ok; %% no change, no write
        Records = [#lbm_kv{key = Key, ver = Ver} | _] ->
            ok = w(Tab, Key, Val, Ver);
        Records = [] ->
            ok = w(Tab, Key, Val, lbm_kv_vclock:fresh())
    end,
    Records.

%%------------------------------------------------------------------------------
%% @private
%% Update the mappings for all `Key's in a table. Only allowed within
%% transaction context.
%%------------------------------------------------------------------------------
-spec u(table(), update_fun()) -> [#lbm_kv{}].
u(Tab, Fun) -> lists:append([u(Tab, Fun, Key) || Key <- mnesia:all_keys(Tab)]).

%%------------------------------------------------------------------------------
%% @private
%% Update the mapping for `Key'. Only allowed within transaction context.
%%------------------------------------------------------------------------------
-spec u(table(), update_fun(), key()) -> [#lbm_kv{}].
u(Tab, Fun, Key) ->
    case r(Tab, Key, write) of
        Records = [#lbm_kv{key = Key, val = Val, ver = Ver} | _] ->
            case Fun(Key, {value, Val}) of
                {value, Val} ->
                    ok; %% no change, no write
                {value, NewVal} ->
                    ok = d(Tab, Key, write),
                    ok = w(Tab, Key, NewVal, Ver);
                _ ->
                    ok = d(Tab, Key, write)
            end;
        Records = [] ->
            case Fun(Key, undefined) of
                {value, Val} ->
                    ok = w(Tab, Key, Val, lbm_kv_vclock:fresh());
                _ ->
                    ok
            end
    end,
    Records.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec strip([#lbm_kv{}]) -> [{key(), value()}].
strip(Records) -> [{K, V} || #lbm_kv{key = K, val = V} <- Records].
