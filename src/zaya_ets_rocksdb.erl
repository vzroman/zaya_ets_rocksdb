
-module(zaya_ets_rocksdb).

-include("zaya_ets_rocksdb.hrl").
%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([
  copy/3,
  dump_batch/2
]).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

%%=================================================================
%%	POOL API
%%=================================================================
-export([
  pool_batch/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref, {
  ets,
  rocksdb,
  pool
}).

-define(LOAD_BATCH_SIZE, 1000).

%%=================================================================
%%	SERVICE
%%=================================================================
create(Params)->
  case pipe(#ref{}, [
    fun(Ref)-> Ref#ref{ets = zaya_ets:create(type_params(ets, Params))} end,
    fun(Ref)-> Ref#ref{rocksdb = zaya_rocksdb:create(type_params(rocksdb, Params))} end,
    fun(#ref{ets = EtsRef, rocksdb = RocksdbRef} = Ref)->
      Ref#ref{pool = open_pool(EtsRef, RocksdbRef, Params)}
    end
  ]) of
    {ok, Ref}-> Ref;
    {error, Error, Ref}->
      catch close(Ref),
      catch remove(Params),
      throw(Error)
  end.

open(Params)->
  case pipe(#ref{}, [
    fun(Ref)-> Ref#ref{rocksdb = zaya_rocksdb:open(type_params(rocksdb, Params))} end,
    fun(Ref)-> Ref#ref{ets = zaya_ets:open(type_params(ets, Params))} end,
    fun(#ref{ets = EtsRef, rocksdb = RocksdbRef} = Ref)->
      Ref#ref{pool = open_pool(EtsRef, RocksdbRef, Params)}
    end,
    fun(#ref{ets = EtsRef, rocksdb = RocksdbRef} = Ref)->
      load_data(RocksdbRef, EtsRef),
      Ref
    end
  ]) of
    {ok, Ref}-> Ref;
    {error, Error, Ref}->
      catch close(Ref),
      throw(Error)
  end.

close(#ref{
  ets = EtsRef,
  rocksdb = RocksdbRef,
  pool = Pool
})->
  catch close_pool(Pool),
  catch zaya_ets:close(EtsRef),
  catch zaya_rocksdb:close(RocksdbRef),
  ok.

remove(Params)->
  zaya_rocksdb:remove(type_params(rocksdb, Params)),
  ok.

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{ets = EtsRef}, Keys)->
  zaya_ets:read(EtsRef, Keys).

write(#ref{ets = EtsRef, rocksdb = RocksdbRef, pool = disabled}, KVs)->
  zaya_rocksdb:write(RocksdbRef, KVs),
  zaya_ets:write(EtsRef, KVs);
write(#ref{pool = Pool}, KVs)->
  zaya_pool:call(Pool, [{write, KVs}]).

delete(#ref{ets = EtsRef, rocksdb = RocksdbRef, pool = disabled}, Keys)->
  zaya_rocksdb:delete(RocksdbRef, Keys),
  zaya_ets:delete(EtsRef, Keys);
delete(#ref{pool = Pool}, Keys)->
  zaya_pool:call(Pool, [{delete, Keys}]).

%%=================================================================
%%	ITERATOR
%%=================================================================
first(#ref{ets = EtsRef})->
  zaya_ets:first(EtsRef).

last(#ref{ets = EtsRef})->
  zaya_ets:last(EtsRef).

next(#ref{ets = EtsRef}, Key)->
  zaya_ets:next(EtsRef, Key).

prev(#ref{ets = EtsRef}, Key)->
  zaya_ets:prev(EtsRef, Key).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
find(#ref{ets = EtsRef}, Query)->
  zaya_ets:find(EtsRef, Query).

foldl(#ref{ets = EtsRef}, Query, Fun, InAcc)->
  zaya_ets:foldl(EtsRef, Query, Fun, InAcc).

foldr(#ref{ets = EtsRef}, Query, Fun, InAcc)->
  zaya_ets:foldr(EtsRef, Query, Fun, InAcc).

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(#ref{ets = EtsRef, rocksdb = RocksdbRef}, KVs)->
  zaya_rocksdb:write(RocksdbRef, KVs),
  zaya_ets:write(EtsRef, KVs).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit(#ref{ets = EtsRef, rocksdb = RocksdbRef, pool = disabled}, Write, Delete)->
  zaya_rocksdb:commit(RocksdbRef, Write, Delete),
  zaya_ets:commit(EtsRef, Write, Delete),
  ok;
commit(#ref{pool = Pool}, Write, Delete)->
  zaya_pool:call(Pool, [{write, Write}, {delete, Delete}]).

prepare_rollback(#ref{ets = EtsRef}, Write, Delete)->
  zaya_ets:prepare_rollback(EtsRef, Write, Delete).

is_persistent()->
  true.

%%=================================================================
%%	POOL API
%%=================================================================
pool_batch({EtsRef, RocksdbRef}, Requests)->
  do_pool_batch(Requests, EtsRef, RocksdbRef, []).

do_pool_batch([{write, KVs} | Rest], EtsRef, RocksdbRef, Writes)->
  do_pool_batch(Rest, EtsRef, RocksdbRef, [KVs | Writes]);
do_pool_batch(Requests, EtsRef, RocksdbRef, [_ | _] = Writes)->
  KVs = lists:append(lists:reverse(Writes)),
  zaya_rocksdb:write(RocksdbRef, KVs),
  zaya_ets:write(EtsRef, KVs),
  do_pool_batch(Requests, EtsRef, RocksdbRef, []);
do_pool_batch([{delete, Keys} | Rest], EtsRef, RocksdbRef, Writes)->
  zaya_rocksdb:delete(RocksdbRef, Keys),
  zaya_ets:delete(EtsRef, Keys),
  do_pool_batch(Rest, EtsRef, RocksdbRef, Writes);
do_pool_batch([], _EtsRef, _RocksdbRef, [])->
  ok.

%%=================================================================
%%	INFO
%%=================================================================
get_size(#ref{ets = EtsRef})->
  zaya_ets:get_size(EtsRef).

%%=================================================================
%%	UTILITIES
%%=================================================================
load_data(RocksdbRef, EtsRef)->
  Tail = zaya_rocksdb:foldl(RocksdbRef, #{}, fun(Rec, {Batch, Count})->
    Batch1 = [Rec | Batch],
    case Count + 1 of
      ?LOAD_BATCH_SIZE ->
        zaya_ets:dump_batch(EtsRef, Batch1),
        {[], 0};
      Count1 ->
        {Batch1, Count1}
    end
  end, {[], 0}),
  case Tail of
    {[_ | _] = Rest, _} -> zaya_ets:dump_batch(EtsRef, Rest);
    _ -> ok
  end.


open_pool(_EtsRef, _RocksdbRef, #{pool := disabled})->
  disabled;
open_pool(EtsRef, RocksdbRef, Params) when is_map(Params)->
  {ok, Pool} = zaya_pool:start_link(pool_params(EtsRef, RocksdbRef, Params)),
  Pool.

close_pool(disabled)->
  ok;
close_pool(Pool)->
  zaya_pool:stop(Pool).

pool_params(EtsRef, RocksdbRef, Params)->
  maps:merge(
    maps:get(pool, Params, #{}),
    #{
      ref => {EtsRef, RocksdbRef},
      module => ?MODULE
    }
  ).

pipe(Ref, [Step | Rest])->
  try Step(Ref) of
    Ref1 -> pipe(Ref1, Rest)
  catch _:Error ->
    {error, Error, Ref}
  end;
pipe(Ref, [])->
  {ok, Ref}.

type_params(Type, Params)->
  TypeParams = maps:with([Type], Params),
  OtherParams = maps:without([ets, rocksdb, pool], Params),
  maps:merge(OtherParams#{pool => disabled}, TypeParams).
