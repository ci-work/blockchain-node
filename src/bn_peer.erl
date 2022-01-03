-module(bn_peer).

-include("bn_jsonrpc.hrl").
-behavior(bn_jsonrpc_handler).

-export([handle_rpc/2]).

%%
%% jsonrpc_handler
%%
handle_rpc(<<"peer_book_self">>, []) ->
    peer_book_response_self(blockchain_swarm:pubkey_bin());
handle_rpc(<<"peer_book_address">>, {Param}) ->
    BinAddress = ?jsonrpc_b58_to_bin(<<"address">>, Param),
    peer_book_response(BinAddress);
handle_rpc(<<"peer_connect">>, {Param}) ->
    BinAddress = ?jsonrpc_b58_to_bin(<<"address">>, Param),
    peer_connect(BinAddress);
handle_rpc(<<"peer_ping">>, {Param}) ->
    BinAddress = ?jsonrpc_b58_to_bin(<<"address">>, Param),
    peer_ping(BinAddress);
handle_rpc(<<"peer_refresh">>, {Param}) ->
    BinAddress = ?jsonrpc_b58_to_bin(<<"address">>, Param),
    peer_refresh(BinAddress);
handle_rpc(_, _) ->
    ?jsonrpc_error(method_not_found).

%%
%% Internal
%%
peer_connect(PubKeyBin) ->
    SwarmTID = blockchain_swarm:tid(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    case libp2p_swarm:connect(SwarmTID, P2PAddr) of
        {ok, _} ->
            #{success => true, success_string => ?TO_VALUE("connected"), address => ?TO_VALUE(P2PAddr)};
        {error, Reason} ->
            ?jsonrpc_error({not_found, "Failed to connect to ~p: ~p~n", [P2PAddr, Reason]})
    end.

peer_ping(PubKeyBin) ->
    SwarmTID = blockchain_swarm:tid(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    case libp2p_swarm:connect(SwarmTID, P2PAddr) of
        {ok, Session} ->
            case libp2p_session:ping(Session) of
                {ok, RTT} ->
                    #{success => true, success_string => ?TO_VALUE("pinged"), rtt => RTT, address => ?TO_VALUE(P2PAddr)};
                {error, Reason} ->
                    ?jsonrpc_error({not_found, "Failed to connect to ~p: ~p~n", [P2PAddr, Reason]})
            end;
        {error, Reason} ->
            ?jsonrpc_error({not_found, "Failed to connect to ~p: ~p~n", [P2PAddr, Reason]})
    end.

peer_refresh(PubKeyBin) ->
    SwarmTID = blockchain_swarm:tid(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    Peerbook = libp2p_swarm:peerbook(SwarmTID),
    libp2p_peerbook:refresh(Peerbook, PubKeyBin),
    #{success => true, success_string => ?TO_VALUE("refreshed"), address => ?TO_VALUE(P2PAddr)}.

peer_book_response_self(PubKeyBin) ->
    TID = blockchain_swarm:tid(),
    Peerbook = libp2p_swarm:peerbook(TID),

    case libp2p_peerbook:get(Peerbook, PubKeyBin) of
        {ok, Peer} ->
            [ lists:foldl(fun(M, Acc) -> maps:merge(Acc, M) end,
                format_peer(Peer),
                [format_listen_addrs(TID, libp2p_peer:listen_addrs(Peer)),
                    format_peer_sessions(TID)]
                ) ];
        {error, not_found} ->
            ?jsonrpc_error({not_found, "Address not found: ~p", [libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin)]});
        {error, _}=Error ->
            ?jsonrpc_error(Error)
    end.

peer_book_response(PubKeyBin) ->
    TID = blockchain_swarm:tid(),
    Peerbook = libp2p_swarm:peerbook(TID),

    case libp2p_peerbook:get(Peerbook, PubKeyBin) of
        {ok, Peer} ->
            [ lists:foldl(fun(M, Acc) -> maps:merge(Acc, M) end,
                format_peer(Peer),
                [format_listen_addrs(TID, libp2p_peer:listen_addrs(Peer)),
                    format_peer_connections(Peer)]
                ) ];
        {error, not_found} ->
            ?jsonrpc_error({not_found, "Address not found: ~p", [libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin)]});
        {error, _}=Error ->
            ?jsonrpc_error(Error)
    end.

format_peer(Peer) ->
    ListenAddrs = libp2p_peer:listen_addrs(Peer),
    ConnectedTo = libp2p_peer:connected_peers(Peer),
    NatType = libp2p_peer:nat_type(Peer),
    Timestamp = libp2p_peer:timestamp(Peer),
    PeerHeight = peer_height(Peer),
    PeerLastBlockTime = last_block_add_time(Peer),
    Bin = libp2p_peer:pubkey_bin(Peer),
    M = #{
        <<"address">> => libp2p_crypto:pubkey_bin_to_p2p(Bin),
        <<"name">> => ?BIN_TO_ANIMAL(Bin),
        <<"height">> => PeerHeight,
        <<"last_block_add_time">> => PeerLastBlockTime,
        <<"listen_addr_count">> => length(ListenAddrs),
        <<"connection_count">> => length(ConnectedTo),
        <<"nat">> => NatType,
        <<"last_updated">> => (erlang:system_time(millisecond) - Timestamp) / 1000
    },
    maps:map(fun(_K, V) -> ?TO_VALUE(V) end, M).

format_listen_addrs(TID, Addrs) ->
    libp2p_transport:sort_addrs(TID, Addrs),
    #{<<"listen_addresses">> => [?TO_VALUE(A) || A <- Addrs]}.

format_peer_connections(Peer) ->
    Connections = [ libp2p_crypto:pubkey_bin_to_p2p(P) || P <- libp2p_peer:connected_peers(Peer)],
    #{ <<"connections">> => [?TO_VALUE(C) || C <- Connections] }.

format_peer_sessions(Swarm) ->
    SessionInfos = libp2p_swarm:sessions(Swarm),
    Rs = lists:filtermap(
        fun({A, S}) ->
            case multiaddr:protocols(A) of
                [{"p2p", B58}] ->
                    {true, {A, libp2p_session:addr_info(libp2p_swarm:tid(Swarm), S), B58}};
                _ ->
                    false
            end
        end,
        SessionInfos
    ),

    FormatEntry = fun({MA, {SockAddr, PeerAddr}, B58}) ->
        M = #{
            <<"local">> => SockAddr,
            <<"remote">> => PeerAddr,
            <<"p2p">> => MA,
            <<"name">> => ?B58_TO_ANIMAL(B58)
        },
        maps:map(fun(_K, V) -> ?TO_VALUE(V) end, M)
    end,
    #{ <<"sessions">> => [FormatEntry(E) || E <- Rs] }.

peer_metadata(Key, Peer) ->
    libp2p_peer:signed_metadata_get(Peer, Key, undefined).

peer_height(Peer) ->
    case peer_metadata(<<"height">>, Peer) of
        undefined ->
            undefined;
        Height when is_integer(Height) ->
            Height;
        Other ->
            lager:warning("Invalid block height for gateway ~s: ~p", [
                libp2p_crypto:pubkey_bin_to_p2p(libp2p_peer:pubkey_bin(Peer)),
                Other
            ]),
            undefined
    end.

last_block_add_time(Peer) ->
    case peer_metadata(<<"last_block_add_time">>, Peer) of
        undefined ->
            undefined;
        LBAT when is_integer(LBAT) ->
            LBAT;
        Other ->
            lager:warning("last_block_add_time", [
                libp2p_crypto:pubkey_bin_to_p2p(libp2p_peer:pubkey_bin(Peer)),
                Other
            ]),
            undefined
    end.
