%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @copyright (C) 2016, Lindenbaum GmbH
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
%%% A very simple application providing distributed sequence numbers.
%%%
%%% Keep calm, after all this is only a sample application and does not handle
%%% duplicates which will of course occur in case of network splits (or on
%%% startup before nodes get connected).
%%% @end
%%%=============================================================================

-module(sequencer).

-behaviour(lbm_kv).
-behaviour(application).
-behaviour(supervisor).

%% API
-export([next/0]).

%% lbm_kv callbacks
-export([handle_conflict/3]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-define(KEY, sequence_number).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Returns the next sequence number. Returns an error if the database is not yet
%% initialized.
%% @end
%%------------------------------------------------------------------------------
-spec next() -> non_neg_integer().
next() ->
    {ok, [{?KEY, Value}]} = lbm_kv:update(?MODULE, ?KEY, fun update_db/2),
    Value + 1.

%%%=============================================================================
%%% lbm_kv callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Just use the highest sequence number.
%%------------------------------------------------------------------------------
-spec handle_conflict(?KEY, non_neg_integer(), non_neg_integer()) ->
                             {value, non_neg_integer()}.
handle_conflict(?KEY, Local, Remote) -> {value, max(Local, Remote)}.

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
init([]) ->
    ok = lbm_kv:create(?MODULE),
    {ok, _} = lbm_kv:update(?MODULE, ?KEY, fun init_db/2),
    {ok, {{one_for_one, 5, 1}, [spec(sequencer_logger)]}}.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
spec(M) -> {M, {M, start_link, []}, permanent, 1000, worker, [M]}.

%%------------------------------------------------------------------------------
%% @private
%% Initialize the DB with sequence number `0', if not already initialized by
%% merge.
%%------------------------------------------------------------------------------
init_db(?KEY, undefined) -> {value, 0};
init_db(?KEY, Other)     -> Other.

%%------------------------------------------------------------------------------
%% @private
%% Update the sequence number by one.
%%------------------------------------------------------------------------------
update_db(?KEY, {value, Value}) -> {value, Value + 1}.
