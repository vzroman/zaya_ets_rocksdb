
-module(zaya_ets_rocksdb).

-include("zaya_ets_rocksdb.hrl").

-define(DEFAULT_OPEN_ATTEMPTS, 5).
-define(RETRY_TIMEOUT, 1000).
-define(LOCK(P), P ++ "/LOCK").

-define(DEFAULT_ROCKSDB_OPTIONS, #{
  open_options => #{
    create_if_missing => false,
    paranoid_checks => false,
    compression => lz4,
    compaction_style => level,
    max_background_jobs => 16,
    max_background_compactions => 8,
    compaction_readahead_size => 2 * 1024 * 1024,
    db_write_buffer_size => 128 * 1024 * 1024,
    max_write_buffer_number => 4,
    min_write_buffer_number_to_merge => 2,
    target_file_size_base => 256 * 1024 * 1024,
    level0_file_num_compaction_trigger => 4,
    level0_slowdown_writes_trigger => 20,
    level0_stop_writes_trigger => 36,
    max_bytes_for_level_base => 1024 * 1024 * 1024,
    max_bytes_for_level_multiplier => 8,
    block_size => 32 * 1024,
    max_subcompactions => 8
  },
  read => #{
    verify_checksums => false,
    fill_cache => true
  },
  write => #{
    sync => false
  }
}).

-define(DEFAULT_OPTIONS, #{
  dir => ".",
  open_attempts => ?DEFAULT_OPEN_ATTEMPTS,
  rocksdb => ?DEFAULT_ROCKSDB_OPTIONS
}).

-define(env, maps_merge(?DEFAULT_OPTIONS, maps:from_list(application:get_all_env(zaya_ets_rocksdb)))).
-define(OPTIONS(O), maps_merge(?env, O)).

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
  commit1/3,
  commit2/2,
  rollback/2
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
  log,
  read,
  write,
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
    fun(Ref)->
      {Log, ReadParams, WriteParams} = create_log(Params),
      Ref#ref{log = Log, read = ReadParams, write = WriteParams}
    end,
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
    fun(#ref{rocksdb = RocksdbRef} = Ref)->
      {Log, ReadParams, WriteParams} = open_log(Params),
      rollback_log(Log, ReadParams, WriteParams, RocksdbRef),
      Ref#ref{log = Log, read = ReadParams, write = WriteParams}
    end,
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
  log = Log,
  pool = Pool
})->
  catch close_pool(Pool),
  catch zaya_ets:close(EtsRef),
  catch zaya_rocksdb:close(RocksdbRef),
  catch rocksdb:close(Log),
  ok.

remove(Params)->
  zaya_rocksdb:remove(type_params(rocksdb, Params)),
  catch remove_log(Params),
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

commit1(#ref{ets = EtsRef, log = Log, write = WriteParams} = Ref, Write, Delete)->
  {WriteBack, DeleteBack} = prepare_rollback(EtsRef, Write, Delete),
  case WriteBack =:= [] andalso DeleteBack =:= [] of
    true -> ignore;
    false ->
      TRef = term_to_binary(make_ref()),
      try
        ok = rocksdb:write(Log, [{put, TRef, term_to_binary({WriteBack, DeleteBack})}], WriteParams),
        commit(Ref, Write, Delete),
        TRef
      catch
        _:E ->
          rollback(Ref, TRef),
          throw(E)
      end
  end.

commit2(#ref{log = Log, write = WriteParams}, TRef)->
  case TRef of
    ignore -> ok;
    _ -> ok = rocksdb:write(Log, [{delete, TRef}], WriteParams)
  end.

rollback(#ref{log = Log, read = ReadParams, write = WriteParams} = Ref, TRef)->
  case rocksdb:get(Log, TRef, ReadParams) of
    {ok, Value} ->
      {WriteBack, DeleteBack} = binary_to_term(Value),
      commit(Ref, WriteBack, DeleteBack),
      ok = rocksdb:write(Log, [{delete, TRef}], WriteParams);
    _ ->
      ok
  end.

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
%%	LOG
%%=================================================================
create_log(Params)->
  Options = ?OPTIONS(maps_merge(Params, #{rocksdb => #{open_options => #{create_if_missing => true}}})),
  open_log_db(Options, create).

open_log(Params)->
  Options = ?OPTIONS(Params),
  #{dir := Dir} = Options,
  LogDir = Dir ++ "/TLOG",
  case filelib:is_dir(LogDir) of
    true -> ok;
    false ->
      ?LOGERROR("~s doesn't exist", [LogDir]),
      throw(not_exists)
  end,
  open_log_db(Options, open).

open_log_db(#{
  dir := Dir,
  rocksdb := #{
    read := Read,
    write := Write
  }
} = Options, Mode)->
  LogDir = Dir ++ "/TLOG",
  case Mode of
    create -> ensure_dir(LogDir);
    open -> ok
  end,
  Log = try_open(LogDir, Options),
  {Log, maps:to_list(Read), maps:to_list(Write)}.

remove_log(Params)->
  Dir = maps:get(dir, Params, "."),
  LogDir = Dir ++ "/TLOG",
  remove_recursive(LogDir).

rollback_log(Log, ReadParams, WriteParams, RocksdbRef)->
  Entries = rocksdb:fold(Log, fun({TRef, Value}, Acc)->
    [{TRef, binary_to_term(Value)} | Acc]
  end, [], ReadParams),
  lists:foreach(fun({TRef, {WriteBack, DeleteBack}})->
    zaya_rocksdb:commit(RocksdbRef, WriteBack, DeleteBack),
    rocksdb:write(Log, [{delete, TRef}], WriteParams)
  end, Entries).

%%=================================================================
%%	DATA LOADING
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

%%=================================================================
%%	ROLLBACK PREPARATION
%%=================================================================
prepare_rollback(EtsRef, Write, Delete)->
  WriteKeys = [K || {K, _} <- Write],
  DeleteKeys = Delete,
  CurrentForWrites = zaya_ets:read(EtsRef, WriteKeys),
  CurrentForDeletes = zaya_ets:read(EtsRef, DeleteKeys),
  WriteMap = maps:from_list(CurrentForWrites),
  WriteBack =
    [{K, V} || {K, V} <- CurrentForWrites, maps:get(K, maps:from_list(Write), undefined) =/= V] ++
    CurrentForDeletes,
  DeleteBack = [K || {K, _} <- Write, not maps:is_key(K, WriteMap)],
  {WriteBack, DeleteBack}.

%%=================================================================
%%	POOL UTILITIES
%%=================================================================
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

%%=================================================================
%%	PIPE UTILITY
%%=================================================================
pipe(Ref, [Step | Rest])->
  try Step(Ref) of
    Ref1 -> pipe(Ref1, Rest)
  catch _:Error ->
    {error, Error, Ref}
  end;
pipe(Ref, [])->
  {ok, Ref}.

%%=================================================================
%%	GENERAL UTILITIES
%%=================================================================
type_params(Type, Params)->
  TypeParams = maps:with([Type], Params),
  OtherParams = maps:without([ets, rocksdb, pool], Params),
  maps:merge(OtherParams#{pool => disabled}, TypeParams).

try_open(Dir, #{
  rocksdb := #{
    open_options := Params
  },
  open_attempts := Attempts
} = Options) when Attempts > 0->
  ?LOGINFO("~s try open with params ~p", [Dir, Params]),
  case rocksdb:open(Dir, maps:to_list(Params)) of
    {ok, Ref} -> Ref;
    {error, {db_open, Error}} ->
      case lists:prefix("IO error: lock ", Error) of
        true ->
          ?LOGWARNING("~s unable to open, hanging lock, trying to unlock", [Dir]),
          case file:delete(?LOCK(Dir)) of
            ok ->
              ?LOGINFO("~s lock removed, trying open", [Dir]),
              timer:sleep(?RETRY_TIMEOUT),
              try_open(Dir, Options);
            {error, UnlockError} ->
              ?LOGERROR("~s lock remove error ~p, try to remove it manually", [?LOCK(Dir), UnlockError]),
              throw(locked)
          end;
        false ->
          ?LOGWARNING("~s open error ~p, try to repair left attempts ~p", [Dir, Error, Attempts - 1]),
          try rocksdb:repair(Dir, [])
          catch
            _:E:S ->
              ?LOGWARNING("~s repair attempt failed error ~p stack ~p, left attempts ~p", [Dir, E, S, Attempts - 1])
          end,
          timer:sleep(?RETRY_TIMEOUT),
          try_open(Dir, Options#{open_attempts => Attempts - 1})
      end;
    {error, Other} ->
      ?LOGERROR("~s open error ~p, left attempts ~p", [Dir, Other, Attempts - 1]),
      timer:sleep(?RETRY_TIMEOUT),
      try_open(Dir, Options#{open_attempts => Attempts - 1})
  end;
try_open(Dir, #{rocksdb := Params})->
  ?LOGERROR("~s OPEN ERROR: params ~p", [Dir, Params]),
  throw(open_error).

ensure_dir(Path)->
  case filelib:is_file(Path) of
    false ->
      case filelib:ensure_dir(Path ++ "/") of
        ok -> ok;
        {error, CreateError} ->
          ?LOGERROR("~s create error ~p", [Path, CreateError]),
          throw({create_dir_error, CreateError})
      end;
    true ->
      remove_recursive(Path),
      ensure_dir(Path)
  end.

remove_recursive(Path)->
  case filelib:is_dir(Path) of
    false ->
      case filelib:is_file(Path) of
        true -> file:delete(Path);
        _ -> ok
      end;
    true ->
      {ok, Files} = file:list_dir(Path),
      [remove_recursive(filename:join(Path, F)) || F <- Files],
      file:del_dir(Path)
  end.

maps_merge(Map1, Map2)->
  maps:fold(fun(K, V2, Acc)->
    case Map1 of
      #{K := V1} when is_map(V1), is_map(V2)->
        Acc#{K => maps_merge(V1, V2)};
      _->
        Acc#{K => V2}
    end
  end, Map1, Map2).
