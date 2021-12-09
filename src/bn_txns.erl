-module(bn_txns).

-include("bn_jsonrpc.hrl").

% -behaviour(blockchain_follower).

-behavior(bn_jsonrpc_handler).

%% blockchain_follower
-export([
    requires_sync/0,
    requires_ledger/0,
    init/1,
    follower_height/1,
    load_chain/2,
    load_block/5,
    terminate/2,
    maybe_fn/2,
    maybe_h3/1,
    to_json/2,
    to_json/3
]).

%% jsonrpc_handler
-export([handle_rpc/2]).
%% api
-export([follower_height/0]).

-define(DB_FILE, "transactions.db").

-record(state, {
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    heights :: rocksdb:cf_handle(),
    transactions :: rocksdb:cf_handle(),
    json :: rocksdb:cf_handle()
}).

requires_ledger() -> false.

requires_sync() -> false.

init(Args) ->
    Dir = filename:join(proplists:get_value(base_dir, Args, "data"), ?DB_FILE),
    case load_db(Dir) of
        {error, {db_open, "Corruption:" ++ _Reason}} ->
            lager:error("DB could not be opened corrupted ~p, cleaning up", [_Reason]),
            ok = bn_db:clean_db(Dir),
            init(Args);
        {ok, State} ->
            persistent_term:put(?MODULE, State),
            {ok, State#state{}}
    end.

follower_height(#state{db = DB, default = DefaultCF}) ->
    case bn_db:get_follower_height(DB, DefaultCF) of
        {ok, Height} -> Height;
        {error, _} = Error -> ?jsonrpc_error(Error)
    end.

load_chain(Chain, State = #state{}) ->
    maybe_load_genesis(Chain, State).

maybe_load_genesis(Chain, State = #state{}) ->
    case blockchain:get_block(1, Chain) of
        {ok, Block} ->
            Hash = blockchain_txn:hash(lists:last(blockchain_block:transactions(Block))),
            case get_transaction(Hash, State) of
                % already loaded
                {ok, _} ->
                    {ok, State};
                % attempt to load
                _ ->
                    load_block([], Block, [], [], State)
            end;
        Error ->
            Error
    end.

load_block(_Hash, Block, _Sync, _Ledger, State = #state{}) ->
    BlockHeight = blockchain_block_v1:height(Block),
    Transactions = blockchain_block:transactions(Block),
    lager:info("Loading Block ~p (~p transactions)", [BlockHeight, length(Transactions)]),
    Chain = blockchain_worker:blockchain(),
    ok = save_transactions(
        BlockHeight,
        Transactions,
        blockchain:ledger(Chain),
        Chain,
        State
    ),
    lager:info("Saved ~p transactions at height ~p", [length(Transactions), BlockHeight]),
    {ok, State}.

terminate(_Reason, #state{db = DB}) ->
    rocksdb:close(DB).

%%
%% jsonrpc_handler
%%

handle_rpc(<<"transaction_get">>, {Param}) ->
    Hash = ?jsonrpc_b64_to_bin(<<"hash">>, Param),
    {ok, State} = get_state(),
    case get_transaction_json(Hash, State) of
        {ok, Json} ->
            Json;
        {error, not_found} ->
            ?jsonrpc_error({not_found, "No transaction: ~p", [?BIN_TO_B64(Hash)]});
        {error, _} = Error ->
            ?jsonrpc_error(Error)
    end;
handle_rpc(_, _) ->
    ?jsonrpc_error(method_not_found).

%%
%% api
%%

follower_height() ->
    {ok, State} = get_state(),
    follower_height(State).

%%
%% Internal
%%

get_state() ->
    bn_db:get_state(?MODULE).

-spec get_transaction(Hash :: binary(), #state{}) ->
    {ok, {Height :: pos_integer() | undefined, blockchain_txn:txn()}} | {error, term()}.
get_transaction(Hash, #state{db = DB, heights = HeightsCF, transactions = TransactionsCF}) ->
    case rocksdb:get(DB, TransactionsCF, Hash, []) of
        {ok, BinTxn} ->
            Height =
                case rocksdb:get(DB, HeightsCF, Hash, []) of
                    not_found -> undefined;
                    {ok, <<H:64/integer-unsigned-little>>} -> H
                end,
            {ok, {Height, blockchain_txn:deserialize(BinTxn)}};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec get_transaction_json(Hash :: binary(), #state{}) ->
    {ok, blockchain_json:json_object()} | {error, term()}.
get_transaction_json(Hash, State = #state{db = DB, json = JsonCF}) ->
    Chain = blockchain_worker:blockchain(),
    case rocksdb:get(DB, JsonCF, Hash, []) of
        {ok, BinJson} ->
            Json = jsone:decode(BinJson, []),
            case blockchain:get_implicit_burn(Hash, Chain) of
                {ok, ImplicitBurn} ->
                    {ok, Json#{implicit_burn => blockchain_implicit_burn:to_json(ImplicitBurn, [])}};
                {error, _} ->
                    {ok, Json}
            end;
        not_found ->
            case get_transaction(Hash, State) of
                {ok, {Height, Txn}} ->
                    Json = blockchain_txn:to_json(Txn, []),
                    case blockchain:get_implicit_burn(Hash, Chain) of
                        {ok, ImplicitBurn} ->
                            {ok, Json#{
                                block => Height,
                                implicit_burn => blockchain_implicit_burn:to_json(ImplicitBurn, [])
                            }};
                        {error, _} ->
                            {ok, Json#{block => Height}}
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

save_transactions(Height, Transactions, Ledger, Chain, #state{
    db = DB,
    default = DefaultCF,
    heights = HeightsCF,
    transactions = TransactionsCF,
    json = JsonCF
}) ->
    {ok, Batch} = rocksdb:batch(),
    HeightBin = <<Height:64/integer-unsigned-little>>,
    JsonOpts = [{ledger, Ledger}, {chain, Chain}],
    lists:foreach(
        fun(Txn) ->
            Hash = blockchain_txn:hash(Txn),
            case application:get_env(blockchain, store_json, false) of
                true ->
                    Json =
                        try
                            to_json(Txn, JsonOpts)
                        catch
                            _:_ ->
                                lager:info("blockchain_txn:to_json catch"),
                                blockchain_txn:to_json(Txn, [])
                        end,
                    ok = rocksdb:batch_put(
                        Batch,
                        JsonCF,
                        Hash,
                        jsone:encode(Json#{block => Height}, [undefined_as_null])
                    );
                _ ->
                    ok
            end,

            ok = rocksdb:batch_put(
                Batch,
                TransactionsCF,
                Hash,
                blockchain_txn:serialize(Txn)
            ),
            ok = rocksdb:batch_put(Batch, HeightsCF, Hash, HeightBin)
        end,
        Transactions
    ),
    bn_db:batch_put_follower_height(Batch, DefaultCF, Height),
    rocksdb:write_batch(DB, Batch, [{sync, true}]).

-spec maybe_fn(fun((any()) -> any()), undefined | null | any()) -> undefined | any().
maybe_fn(_Fun, undefined) ->
    undefined;
maybe_fn(_Fun, null) ->
    undefined;
maybe_fn(Fun, V) ->
    Fun(V).

-spec maybe_h3(undefined | h3:h3index()) -> undefined | binary().
maybe_h3(V) ->
    maybe_fn(fun(I) -> list_to_binary(h3:to_string(I)) end, V).

to_json(T, Opts) ->
    Type = blockchain_txn:json_type(T),
    to_json(Type, T, Opts).

to_json(<<"poc_request_v1">>, T, Opts) ->
    {ledger, Ledger} = lists:keyfind(ledger, 1, Opts),
    Json = #{challenger := Challenger} = blockchain_txn:to_json(T, Opts),
    {ok, ChallengerInfo} = blockchain_ledger_v1:find_gateway_info(?B58_TO_BIN(Challenger), Ledger),
    ChallengerLoc = blockchain_ledger_gateway_v2:location(ChallengerInfo),
    Json#{
        challenger_owner => ?BIN_TO_B58(blockchain_ledger_gateway_v2:owner_address(ChallengerInfo)),
        challenger_location => maybe_h3(ChallengerLoc)
    };

to_json(<<"poc_receipts_v1">>, T, Opts) ->
    {ledger, Ledger} = lists:keyfind(ledger, 1, Opts),
    Json = #{challenger := Challenger, path := Path} = blockchain_txn:to_json(T, Opts),
    UpdateWitness = fun(WitnessJson = #{gateway := Witness}) ->
        {ok, WitnessInfo} = blockchain_ledger_v1:find_gateway_info(?B58_TO_BIN(Witness), Ledger),
        WitnessLoc = blockchain_ledger_gateway_v2:location(WitnessInfo),
        WitnessJson#{
            owner => ?BIN_TO_B58(blockchain_ledger_gateway_v2:owner_address(WitnessInfo)),
            location => maybe_h3(WitnessLoc)
        }
    end,

    UpdatePath = fun(PathJson = #{challengee := Challengee, witnesses := Witnesses}) ->
        {ok, ChallengeeInfo} = blockchain_ledger_v1:find_gateway_info(
            ?B58_TO_BIN(Challengee),
            Ledger
        ),
        ChallengeeLoc = blockchain_ledger_gateway_v2:location(ChallengeeInfo),
        PathJson#{
            challengee_owner => ?BIN_TO_B58(
                blockchain_ledger_gateway_v2:owner_address(ChallengeeInfo)
            ),
            challengee_location => maybe_h3(ChallengeeLoc),
            witnesses => [UpdateWitness(W) || W <- Witnesses]
        }
    end,

    {ok, ChallengerInfo} = blockchain_ledger_v1:find_gateway_info(?B58_TO_BIN(Challenger), Ledger),
    ChallengerLoc = blockchain_ledger_gateway_v2:location(ChallengerInfo),
    Json#{
        challenger_owner => ?BIN_TO_B58(blockchain_ledger_gateway_v2:owner_address(ChallengerInfo)),
        challenger_location => maybe_h3(ChallengerLoc),
        path => [UpdatePath(E) || E <- Path]
    };

to_json(<<"state_channel_close_v1">>, T, Opts) ->
    {ledger, Ledger} = lists:keyfind(ledger, 1, Opts),
    Json = #{state_channel := SCJson} = blockchain_txn:to_json(T, Opts),
    UpdateSummary = fun(Summary = #{client := Client}) ->
        case blockchain_ledger_v1:find_gateway_info(?B58_TO_BIN(Client), Ledger) of
            {error, _} ->
                Summary;
            {ok, ClientInfo} ->
                blockchain_ledger_v1:find_gateway_info(?B58_TO_BIN(Client), Ledger),
                ClientLoc = blockchain_ledger_gateway_v2:location(ClientInfo),
                Summary#{
                    owner => ?BIN_TO_B58(blockchain_ledger_gateway_v2:owner_address(ClientInfo)),
                    location => maybe_h3(ClientLoc)
                }
        end
    end,

    Json#{
        state_channel => SCJson#{
            summaries => [UpdateSummary(S) || S <- maps:get(summaries, SCJson)]
        }
    };

to_json(<<"rewards_v2">>, T, Opts) ->
    {chain, Chain} = lists:keyfind(chain, 1, Opts),
    Start = blockchain_txn_rewards_v2:start_epoch(T),
    End = blockchain_txn_rewards_v2:end_epoch(T),
    StartTime = erlang:monotonic_time(millisecond),
    {ok, Metadata} = blockchain_txn_rewards_v2:calculate_rewards_metadata(
        Start,
        End,
        Chain
    ),
    EndTime = erlang:monotonic_time(millisecond),
    lager:info("Calculated rewards metadata took: ~p ms", [EndTime - StartTime]),
    blockchain_txn:to_json(T, Opts ++ [{rewards_metadata, Metadata}]);

to_json(_Type, T, Opts) ->
    blockchain_txn:to_json(T, Opts).

-spec load_db(file:filename_all()) -> {ok, #state{}} | {error, any()}.
load_db(Dir) ->
    case bn_db:open_db(Dir, ["default", "heights", "transactions", "json"]) of
        {error, _Reason} = Error ->
            Error;
        {ok, DB, [DefaultCF, HeightsCF, TransactionsCF, JsonCF]} ->
            State = #state{
                db = DB,
                default = DefaultCF,
                heights = HeightsCF,
                transactions = TransactionsCF,
                json = JsonCF
            },
            compact_db(State),
            {ok, State}
    end.

compact_db(#state{db = DB, default = Default, transactions = TransactionsCF, json = JsonCF}) ->
    rocksdb:compact_range(DB, Default, undefined, undefined, []),
    rocksdb:compact_range(DB, TransactionsCF, undefined, undefined, []),
    rocksdb:compact_range(DB, JsonCF, undefined, undefined, []),
    ok.
