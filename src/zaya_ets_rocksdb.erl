
-module(zaya_ets_rocksdb).

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
  transaction/1,
  t_write/3,
  commit/2,
  rollback/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{ets,rocksdb}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  EtsParams = type_params(ets,Params),
  EtsRef = zaya_ets:create( EtsParams ),
  try
    RocksdbRef = zaya_rocksdb:create( type_params(rocksdb, Params) ),
    #ref{ ets = EtsRef, rocksdb = RocksdbRef }
  catch
    _:E->
      catch zaya_ets:close(EtsRef),
      catch zaya_ets:remove(EtsParams),
      throw(E)
  end.

open( Params )->
  RocksdbRef = zaya_rocksdb:open( type_params(rocksdb, Params ) ),
  EtsRef = zaya_ets:open( type_params(ets,Params) ),

  zaya_rocksdb:foldl(RocksdbRef,#{},fun(Rec,Acc)->
    zaya_ets:write( EtsRef, [Rec] ),
    Acc
  end,[]),

  #ref{ ets = EtsRef, rocksdb = RocksdbRef }.

close( #ref{ets = EtsRef, rocksdb = RocksdbRef} )->
  catch zaya_ets:close( EtsRef ),
  zaya_rocksdb:close( RocksdbRef ).

remove( Params )->
  zaya_rocksdb:remove( type_params(rocksdb, Params) ).

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{ets = EtsRef}, Keys)->
  zaya_ets:read( EtsRef, Keys ).

write(#ref{ets = EtsRef, rocksdb = RocksdbRef}, KVs)->
  zaya_rocksdb:write( RocksdbRef, KVs ),
  zaya_ets:write( EtsRef, KVs ).

delete(#ref{ets = EtsRef, rocksdb = RocksdbRef}, Keys)->
  zaya_rocksdb:delete( RocksdbRef, Keys ),
  zaya_ets:delete( EtsRef, Keys ).

%%=================================================================
%%	ITERATOR
%%=================================================================
first( #ref{ets = EtsRef} )->
  zaya_ets:first( EtsRef ).

last( #ref{ets = EtsRef} )->
  zaya_ets:last( EtsRef ).

next( #ref{ets = EtsRef}, Key )->
  zaya_ets:next( EtsRef, Key ).

prev( #ref{ets = EtsRef}, Key )->
  zaya_ets:prev( EtsRef, Key ).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{ets = EtsRef}, Query)->
  zaya_ets:find( EtsRef, Query ).

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{ets = EtsRef}, Query, Fun, InAcc )->
  zaya_ets:foldl( EtsRef, Query, Fun, InAcc ).

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{ets = EtsRef}, Query, Fun, InAcc )->
  zaya_ets:foldr( EtsRef, Query, Fun, InAcc ).

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(Ref, KVs)->
  write(Ref, KVs).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
transaction( _Ref )->
  ets:new(?MODULE,[
    private,
    ordered_set,
    {read_concurrency, true},
    {write_concurrency, auto}
  ]).

t_write( _Ref, TRef, KVs )->
  ets:insert( TRef, KVs ),
  ok.

commit(#ref{ ets = EtsRef, rocksdb = RocksdbRef }, TRef)->
  KVs = ets:tab2list( TRef ),
  zaya_rocksdb:write( RocksdbRef, KVs ),
  zaya_ets:write( EtsRef, KVs ),
  ok.


rollback(_Ref, TRef )->
  ets:delete( TRef ),
  ok.

%%=================================================================
%%	INFO
%%=================================================================
get_size( #ref{ets = EtsRef})->
  zaya_ets:get_size( EtsRef ).

type_params( Type, Params )->
  TypeParams = maps:with([Type],Params),
  OtherParams = maps:without([ets,rocksdb], Params),
  maps:merge( OtherParams, TypeParams ).