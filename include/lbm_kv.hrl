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

-ifndef(lbm_kv_hrl_).
-define(lbm_kv_hrl_, 1).

%% All lbm_kv have a version field in addition to the default attributes.
-define(LBM_KV_ATTRS, [key, val, version]).

%% A special define using a `hidden' mnesia feature to set the `cookie' of a
%% table (at creation time only). This is needed to be able to merge schemas
%% of nodes. That created the same table independently (while not yet
%% connected). Please note that this bypasses a mnesia-builtin security
%% mechanism that classifies tables with the same name and different cookie as
%% incompatible by default. If two nodes have at least one table with the same
%% name and differing cookie a schema merge and thus a mnesia-connection between
%% these nodes will be refused by mnesia.
-define(LBM_KV_COOKIE, {{0,0,0}, lbm_kv}).

%% The options used in `mnesia:create_table/2'.
-define(LBM_KV_TABLE_OPTS(), [{attributes, ?LBM_KV_ATTRS},
                              {cookie, ?LBM_KV_COOKIE},
                              {ram_copies, [node() | nodes()]}]).

%% A matcher for preprocessed lbm_kv table entries.
-define(LBM_KV_SHORT(Key, Value, Version), {Key, Value, Version}).

%% A matcher for a raw lbm_kv table entry.
-define(LBM_KV_LONG(Table, Key, Value, Version), {Table, Key, Value, Version}).

%% Default timeout for RPC calls.
-define(LBM_KV_RPC_TIMEOUT, 2000).

%% Simple debug macro.
-ifdef(DEBUG).
-define(LBM_KV_DBG(Fmt, Args), io:format(Fmt, Args)).
-else.
-define(LBM_KV_DBG(Fmt, Args), begin _ = Fmt, _ = Args, ok end).
-endif.

-endif. %% lbm_kv_hrl_
