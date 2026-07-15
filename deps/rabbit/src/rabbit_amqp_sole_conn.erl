%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqp_sole_conn).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("include/rabbit_khepri.hrl").
-include_lib("amqp10_common/include/amqp10_sole_conn.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(RA_CLUSTER_NAME, rabbitmq_amqp10_sole_conn).
-define(STORE_ID, ?RA_CLUSTER_NAME).
-define(RA_FRIENDLY_NAME, "AMQP Sole Conn Enforcement").
-define(RA_SYSTEM, coordination).
-define(TRIGGER_ID, amqp10_sole_conn_kill_connection).
-define(DEFAULT_COMMAND_OPTIONS, #{reply_from => local}).
-define(RPC_TIMEOUT, 30_000).
-define(ALIVENESS_RPC_TIMEOUT, 1_000).
-define(TICK_INTERVAL, 30_000).
-define(JOIN_MAX_ATTEMPTS, 5).
-define(JOIN_RETRY_BACKOFF, 1_000).

-rabbit_boot_step({?MODULE,
                   [{description, "AMQP 1.0 sole connection enforcement"},
                    {mfa,         {?MODULE, recover, []}},
                    {requires,    database},
                    {enables,     pre_flight}]}).

%% supervisor and gen_server callbacks
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% lifecycle and store management
-export([forget_node/1,
         recover/0,
         ensure_running/0,
         stop/0,
         get_ra_system/0,
         get_store_id/0]).

%% public API
-export([acquire/5,
         refuse_connection_error/0,
         close_existing_connection_error/0]).

%% CLI
-export([status/0,
         force_delete/2]).

%% for testing
-export([conn/2,
         try_put/3,
         conn_path/2]).

-type vhost() :: binary().
-type container_id() :: binary().
-type username() :: binary().

-record(conn, {pid :: pid(),
               username :: username()}).
%% gen_server state
-record(state, {resizer_pid :: pid() | undefined}).
%% --------------------------------------------------------------
%% gen_server callbacks
%% --------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:send_after(tick_interval(), self(), cluster_tick),
    {ok, #state{resizer_pid = undefined}}.

handle_info(cluster_tick, State = #state{resizer_pid = ResizerPid}) ->
    ?LOG_DEBUG("sole_conn cluster tick"),
    erlang:send_after(tick_interval(), self(), cluster_tick),
    case is_leader() of
        true when ResizerPid =:= undefined ->
            ?LOG_DEBUG("leader, spawning resizing process"),
            %% We are the leader and no resize is currently running. Start one.
            {Pid, _MonitorRef} = spawn_monitor(fun maybe_resize_cluster/0),
            {noreply, State#state{resizer_pid = Pid}};
        true ->
            %% We are the leader but a resize is already running. Skip this tick.
            ?LOG_DEBUG("Skipping sole_conn cluster resize tick, previous run still in progress"),
            {noreply, State};
        false ->
            ?LOG_DEBUG("not the leader, no resizing"),
            %% We are not the leader. Do nothing.
            {noreply, State}
    end;
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #state{resizer_pid = Pid}) ->
    %% The resizing process finished or crashed. Clear the tracker so the next tick can run.
    {noreply, State#state{resizer_pid = undefined}};
handle_info(Message, State) ->
    {stop, {unhandled_info, Message}, State}.

handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unhandled_cast, Request}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------
%% Lifecycle and store management
%% --------------------------------------------------------------

-spec forget_node(node()) -> ok | {error, term()}.
forget_node(Node) when is_atom(Node) ->
    %% Check if the store was ever bootstrapped locally
    case ra_directory:uid_of(get_ra_system(), get_store_id()) of
        undefined ->
            %% The store was never used on this node (lazy init), safe to skip
            ok;
        _ ->
            %% Evict the node using the unified logic
            evict_node(Node)
    end.

is_leader() ->
    case ra_leaderboard:lookup_leader(get_store_id()) of
        {_StoreId, Node} when Node =:= node() -> true;
        _ -> false
    end.

maybe_resize_cluster() ->
    case rabbit:is_running() of
        true ->
            StoreId = get_store_id(),
            case khepri_cluster:members(StoreId, #{favor => low_latency}) of
                {ok, Members} ->
                    %% Extract the nodes currently in the Khepri cluster
                    MemberNodes = [Node || {_, Node} <- Members],
                    %% Get the state of the broader RabbitMQ cluster
                    Present = rabbit_nodes:list_running(),
                    RabbitNodes = rabbit_nodes:list_members(),
                    %% Explicitly compute nodes that are allowed to be added
                    %% They are members of the cluster and running, which is
                    %% necessary to bootstrap the Khepri store
                    AddableNodes = [N || N <- RabbitNodes, lists:member(N, Present)],
                    %% Calculate nodes to add
                    case AddableNodes -- MemberNodes of
                        [] ->
                            ok;
                        [New | _] ->
                            ?LOG_INFO("~ts: Expanding sole_conn Khepri cluster to "
                                      "running node ~w", [?MODULE, New]),
                           try
                                erpc:cast(New, ?MODULE, ensure_running, [])
                            catch
                                Class:Reason ->
                                    ?LOG_WARNING("~ts: Failed to cast ensure_running to node ~w. "
                                                 "Error: ~p:~p",
                                                 [?MODULE, New, Class, Reason])
                            end
                    end,
                    %% Calculate nodes to add
                    case MemberNodes -- RabbitNodes of
                        [] ->
                            ok;
                        [Old | _] when length(RabbitNodes) > 0 ->
                            %% This should be rare, as forget_cluster_node shrinks
                            %% this cluster as well
                            ?LOG_INFO("~ts: RabbitMQ node ~w was formally removed from the cluster, "
                                      "evicting it from the sole_conn Khepri cluster",
                                      [?MODULE, Old]),
                            _ = evict_node(Old),
                            ok;
                        _ ->
                            ok
                    end;
                _ ->
                    %% Failed to read local members, retry next tick
                    ok
            end;
        false ->
            ok
    end.

evict_node(Node) ->
    StoreId = get_store_id(),
    %% Check if the node we want to evict is currently reachable
    case net_adm:ping(Node) of
        pong ->
            %% Node is online. Safely stop our gen_server first.
            try
                erpc:cast(Node, ?MODULE, stop, [])
            catch
                Class:Reason ->
                    ?LOG_DEBUG("~ts: Could not stop sole_conn gen_server on node ~w. "
                               "Error: ~p:~p",
                               [?MODULE, Node, Class, Reason])
            end,
            %% Ask Khepri to cleanly reset the store on that node via RPC.
            %% This removes it from the quorum, deletes RA data,
            %% and clears Khepri memory caches.
            ?LOG_INFO("~ts: Target node ~w is reachable, executing Khepri reset",
                      [?MODULE, Node]),
            try erpc:call(Node, khepri_cluster, reset, [StoreId], ?RPC_TIMEOUT) of
                ok ->
                    ok;
                {error, _} = Err ->
                    Err
            catch
                error:{erpc, timeout} ->
                    {error, timeout};
                error:{erpc, RpcReason} ->
                    {error, RpcReason}
            end;
        pang ->
            %% Node is offline, we can ask it to reset itself.
            %% We must forcibly shrink the quorum via the Raft leader.
            ?LOG_INFO("~ts: Target node ~w is unreachable, forcefully removing "
                      "from Raft quorum",
                      [?MODULE, Node]),
            ExpectedMembers = [{StoreId, N} || N <- rabbit_nodes:list_members()],
            ToRemove = {StoreId, Node},
            case ra:members(ExpectedMembers) of
                {ok, Members, Leader} ->
                    case lists:member(ToRemove, Members) of
                        true ->
                            %% ra:remove_member safely evicts the dead node
                            %% from the consensus group
                            case ra:remove_member(Leader, ToRemove) of
                                {ok, _, _} ->
                                    ok;
                                {timeout, _} ->
                                    {error, timeout};
                                {error, _} = Err ->
                                    Err
                            end;
                        false ->
                            %% The node is already gone from the Raft quorum
                            ok
                    end;
                {timeout, _} ->
                    {error, timeout};
                {error, _} = Err ->
                    Err
            end
    end.

stop() ->
    ?LOG_DEBUG("Stopping sole_conn gen_server and "
               "removing from supervision tree on ~p", [node()]),
    _ = rabbit_sup:stop_child(?MODULE),
    ok.

init_schema() ->
    _ = khepri_adv:put(get_store_id(),
                       kill_connection_sproc_path(),
                       fun kill_connection_sproc/1,
                       ?DEFAULT_COMMAND_OPTIONS),

    EventFilter = khepri_evf:tree(kill_connection_sproc_trigger_pattern(),
                                  #{on_actions => [update]}),

    Opts = #{where => all_members},
    ok = khepri:register_trigger(
           get_store_id(),
           ?TRIGGER_ID,
           EventFilter,
           kill_connection_sproc_path(),
           Opts).

%% --------------------------------------------------------------
%% CLI
%% --------------------------------------------------------------

-spec status() -> [[{binary(), term()}]] | {error, term()}.
status() ->
    case members() of
        {ok, Members} ->
            [begin
                 %% Securely call ra:key_metrics/1 on the remote node
                 MetricsResult = try
                                     erpc:call(N, ra, key_metrics, [ServerId], ?RPC_TIMEOUT)
                                 catch
                                     _:Err -> {error, Err}
                                 end,
                 case MetricsResult of
                     #{state := RaftState,
                       membership := Membership,
                       commit_index := Commit,
                       term := Term,
                       last_index := Last,
                       last_applied := LastApplied,
                       last_written_index := LastWritten,
                       snapshot_index := SnapIdx} ->
                         %% Optionally fetch the Khepri machine version, failing gracefully to 0
                         MacVer = try
                                      erpc:call(N, khepri_machine, version, [], 1000)
                                  catch _:_ ->
                                            0
                                  end,
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, RaftState},
                          {<<"Membership">>, Membership},
                          {<<"Last Log Index">>, Last},
                          {<<"Last Written">>, LastWritten},
                          {<<"Last Applied">>, LastApplied},
                          {<<"Commit Index">>, Commit},
                          {<<"Snapshot Index">>, SnapIdx},
                          {<<"Term">>, Term},
                          {<<"Machine Version">>, MacVer}];
                     {error, ErrReason} ->
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, rabbit_misc:format("~p", [ErrReason])},
                          {<<"Membership">>, <<>>},
                          {<<"Last Log Index">>, <<>>},
                          {<<"Last Written">>, <<>>},
                          {<<"Last Applied">>, <<>>},
                          {<<"Commit Index">>, <<>>},
                          {<<"Snapshot Index">>, <<>>},
                          {<<"Term">>, <<>>},
                          {<<"Machine Version">>, <<>>}]
                 end
             end || {_, N} = ServerId <- Members];
        {error, {no_more_servers_to_try, _}} ->
            {error, sole_conn_not_started_or_available};
        {error, _} = Err ->
            Err
    end.

-spec force_delete(vhost(), container_id()) ->
    ok | {error, any()}.
force_delete(VHost, ContainerId) ->
    case whereis(?MODULE) of
        undefined ->
            {error, sole_conn_not_started_or_available};
        _Pid ->
            Path = conn_path(VHost, ContainerId),
            case khepri_adv:delete(get_store_id(), Path) of
                {ok, Map} when map_size(Map) =:= 0 ->
                    %% The path matched no tree node, there was nothing to delete.
                    {error, not_found};
                {ok, _Map} ->
                    ok;
                {error, _} = Err ->
                    Err
            end
    end.

%% --------------------------------------------------------------
%% Public API
%% --------------------------------------------------------------

recover() ->
    LocalServerId = {get_store_id(), node()},
    %% We ask RA to passively check the disk and restart the Khepri state machine
    ?LOG_DEBUG("Trying to restart local sole_conn RA server on ~p", [node()]),

    case ra:restart_server(get_ra_system(), LocalServerId) of
        {error, Reason} when Reason == not_started;
                             Reason == name_not_registered ->
            ?LOG_DEBUG("~p, will start on demand", [Reason]),
            %% First boot, do nothing and wait until the first `acquire`
            ok;
        _ ->
            ?LOG_DEBUG("Restarted local sole_conn RA server on ~p", [node()]),
            %% Khepri instance restarted
            %% We can now safely start our gen_server to manage it.
            rabbit_sup:start_child(?MODULE)
    end.

ensure_running() ->
    case whereis(?MODULE) of
        undefined ->
            ?LOG_DEBUG("sole_conn not running on ~p, "
                       "trying to acquire bootstrap lock", [node()]),
            global:set_lock({?MODULE, bootstrap}),
            try
                case whereis(?MODULE) of
                    undefined ->
                        start_local_store(); 
                    _Pid ->
                        ?LOG_DEBUG("sole_conn has started on ~p, skipping bootstrap sequence",
                                   [node()]),
                        ok
                end
            after
                %% Lock is released even if an exception occurs
                global:del_lock({?MODULE, bootstrap})
            end;
        _ ->
            ok
    end.

start_local_store() ->
    ?LOG_DEBUG("Starting sole_conn bootstrap sequence on ~p",
               [node()]),
    StoreId = get_store_id(),

    %% TODO get settings from global Khepri configuration
    %% TODO see also rabbit_stream_coordinator:make_ra_conf/3 for RA settings
    SnapshotInterval = 50000,
    RetryTimeout = 300_000,
    MachineConfig = #{snapshot_interval => SnapshotInterval},
    RaServerConfig = #{cluster_name => StoreId,
                       friendly_name => ?RA_FRIENDLY_NAME,
                       min_recovery_checkpoint_interval => 4096,
                       machine_config => MachineConfig},

    ?LOG_DEBUG("Starting ~ts Khepri store", [?RA_FRIENDLY_NAME]),
    {ok, _} = khepri:start(?RA_SYSTEM, RaServerConfig),

    %% Check if we just booted a virgin node or recovered data
    case khepri:is_empty(StoreId) of
        true ->
            %% Virgin bootstrap
            OtherNodes = rabbit_nodes:list_running() -- [node()],
            ?LOG_DEBUG("Other nodes in cluster: ~p", [OtherNodes]),
            case find_active_peer(OtherNodes) of
                undefined ->
                    %% Virgin Cluster
                    ?LOG_DEBUG("No active peer, starting new cluster"),
                    ok = khepri_cluster:wait_for_leader(StoreId, RetryTimeout),
                    ?LOG_DEBUG("Started new cluster, initializing schema"),
                    init_schema(),
                    ?LOG_DEBUG("Schema initialized");
                PeerNode ->
                    %% Existing Cluster
                    ?LOG_DEBUG("Trying to join active peer: ~p", [PeerNode]),
                    ok = join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout),

                    ?LOG_DEBUG("Joined existing cluster, waiting for effective behaviour"),
                    ok = khepri_cluster:wait_for_effective_behaviour(
                           StoreId, process_based_keep_while, RetryTimeout),
                    ?LOG_DEBUG("Local store ready")
            end;
        false ->
            %% Recovery
            %% The node already has data, meaning it was part of a cluster.
            %% It natively rejoins the Raft consensus group.
            ?LOG_DEBUG("sole_conn store recovered from disk. Skipping discovery. "
                       "Waiting for effective behaviour."),
            ok = khepri_cluster:wait_for_effective_behaviour(
                   StoreId, process_based_keep_while, RetryTimeout),
            ?LOG_DEBUG("Local store ready")
    end,

    %% Start the gen_server. This registers the local process
    %% name, which allows subsequent calls to bypass this setup, and
    %% lets other nodes discover us via find_active_peer/1.
    ok = rabbit_sup:start_child(?MODULE),
    ok.

join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout) ->
    join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, ?JOIN_MAX_ATTEMPTS).

join_active_peer(_StoreId, PeerNode, _RaServerConfig, _RetryTimeout, 0) ->
    ?LOG_ERROR("Giving up joining active peer ~p after repeated failures", [PeerNode]),
    erlang:error({failed_to_join_peer, PeerNode});
join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, AttemptsLeft) ->
    try join_or_evict_ghost_and_retry(StoreId, PeerNode, RetryTimeout) of
        ok ->
            ok;
        {error, Reason} ->
            retry_join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout,
                                   AttemptsLeft, Reason)
    catch
        %% The local Ra server can end up stopped (while its Ra system and
        %% server config are still known) if it crashed between being
        %% restarted (as part of a failed join attempt) and the eviction of
        %% our stale ghost identity from the remote peer: until that ghost is
        %% evicted, the remote cluster may still address Raft messages to it.
        error:?khepri_exception(ra_server_not_running_but_props_available, _) = Reason ->
            retry_join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout,
                                    AttemptsLeft, Reason)
    end.

retry_join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, AttemptsLeft, Reason) ->
    ?LOG_WARNING("Failed to join active peer ~p (~p), restarting local store "
                 "and retrying (~b attempt(s) left)",
                 [PeerNode, Reason, AttemptsLeft - 1]),
    timer:sleep(?JOIN_RETRY_BACKOFF),
    %% Bring the local Ra server back up (it is a no-op if it is already
    %% running) before retrying the join.
    {ok, _} = khepri:start(?RA_SYSTEM, RaServerConfig),
    join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, AttemptsLeft - 1).

join_or_evict_ghost_and_retry(StoreId, PeerNode, RetryTimeout) ->
    case khepri_cluster:join(StoreId, PeerNode) of
        ok ->
            ok;
        {error, _Reason} ->
            %% A violent crash may have wiped our local metadata,
            %% but the remote cluster still remembers our old ghost identity.
            %% We must forcibly evict our ghost from the active peer and retry.
            ?LOG_DEBUG("Join failed, attempting to evict ghost "
                       "identity from ~p",
                       [PeerNode]),
            TargetRaftNode = {StoreId, PeerNode},
            GhostIdentity = {StoreId, node()},

            %% Ask the active peer's RA server to remove our old identity
            _ = erpc:call(PeerNode, ra, remove_member,
                          [TargetRaftNode, GhostIdentity, RetryTimeout]),

            %% Retry the join now that the cluster views us as a clean slate
            ?LOG_DEBUG("Ghost evicted. Retrying join..."),
            khepri_cluster:join(StoreId, PeerNode)
    end.

get_ra_system() ->
    ?RA_SYSTEM.

get_store_id() ->
    ?STORE_ID.

%% --------------------------------------------------------------
%% Public API
%% --------------------------------------------------------------

-spec acquire(none | enforcement_policy(), vhost(), container_id(), username(), pid()) ->
    ok | {error, refuse_connection | close_existing}.
acquire(none, _, _, _, _) ->
    ok;
acquire(Plcy, VHost, ContainerId, Username, ConnPid) ->
    ensure_running(),
    do_acquire(Plcy, VHost, ContainerId, Username, ConnPid).

refuse_connection_error() ->
    %% the error field of close MUST have an error with the condition field
    %% of error being invalid-field and the info field of error having
    %% the symbol key invalid-field taking the symbol value container-id.
    %% [sole conn 3.2.1]
    amqp_error(
      ?V_1_0_AMQP_ERROR_INVALID_FIELD,
      <<"The container-id is already bound to an "
        "active exclusive connection.">>,
      {?V_1_0_AMQP_ERROR_INVALID_FIELD, {symbol, <<"container-id">>}}).

close_existing_connection_error() ->
    %% "The existing connection MUST be closed with the error field of
    %% close having the condition field of error being resource-locked.
    %% Further the info field of error MUST contain the symbol key
    %% sole-connection-enforcement taking the boolean value true"
    %% [sole conn 3.2.1]
    amqp_error(?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
               <<"Connection closed because another "
                 "connection with the same container-id "
                 "was established (sole connection "
                 "enforcement).">>,
               {?SOLE_CONN_ENFORCEMENT, {boolean, true}}).

do_acquire(refuse_connection = Plcy, VHost, ContainerId, Username, ConnPid) ->
    Path = conn_path(VHost, ContainerId),

    Opts = default_options(ConnPid),
    Payload = #conn{pid = ConnPid, username = Username},
    case khepri_adv:create(get_store_id(), Path, Payload, Opts) of
        {ok, _} ->
            %% no node yet, accept
            %% node should clean itself when the connection is closed
            ok;
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingConn}}}} ->
            %% Only the same user may take over a dead connection's lease;
            %% a different user is refused outright, aliveness notwithstanding.
            case same_user(ExistingConn, Username) of
                true ->
                    case check_conn(ExistingConn) of
                        true ->
                            {error, refuse_connection};
                        _ ->
                            case try_put(Path, ExistingConn, Payload) of
                                ok ->
                                    ok;
                                _ ->
                                    {error, refuse_connection}
                            end
                    end;
                false ->
                    {error, refuse_connection}
            end;
        {error, Reason} ->
            ?LOG_INFO("Unexpected Khepri error for connection '~ts' "
                      "in vhost ~ts (policy ~ts): ~p. Refusing connection.",
                      [ContainerId, VHost, Plcy, Reason]),
            {error, refuse_connection}
    end;
do_acquire(close_existing = Plcy, VHost, ContainerId, Username, ConnPid) ->
    Path = conn_path(VHost, ContainerId),
    Opts = default_options(ConnPid),
    Payload = #conn{pid = ConnPid, username = Username},
    case khepri_adv:create(get_store_id(), Path, Payload, Opts) of
        {ok, _} ->
            ok;
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingConn}}}} ->
            %% A different user may not close and replace someone else's
            %% connection: that would be a container ID hijack.
            case same_user(ExistingConn, Username) of
                true ->
                    case try_put(Path, ExistingConn, Payload) of
                        ok ->
                            ok;
                        _ ->
                            {error, refuse_connection}
                    end;
                false ->
                    {error, refuse_connection}
            end;
        {error, Reason} ->
            ?LOG_INFO("Unexpected Khepri error for connection '~ts' "
                      "in vhost ~ts (policy ~ts): ~p. Refusing connection.",
                      [ContainerId, VHost, Plcy, Reason]),
            {error, refuse_connection}
    end.

%% --------------------------------------------------------------
%% Internals
%% --------------------------------------------------------------

%% Iterates through peer nodes and checks if the amqp10_sole_conn process is alive.
find_active_peer([]) ->
    undefined;
find_active_peer([Node | Rest]) ->
    %% Use a fast RPC call with a 1-second timeout to avoid hanging the client
    %% if a peer is unresponsive.
    try erpc:call(Node, erlang, whereis, [?MODULE], 1000) of
        Pid when is_pid(Pid) ->
            Node;
        _ ->
            find_active_peer(Rest)
    catch
        error:{erpc, _Reason} ->
            %% Node is unreachable or timed out, move on to the next one
            find_active_peer(Rest)
    end.

default_options(Pid) ->
    maps:merge(?DEFAULT_COMMAND_OPTIONS, #{keep_while => Pid}).

same_user(#conn{username = ExistingUsername}, Username) ->
    ExistingUsername =:= Username.

check_conn(#conn{pid = Pid}) ->
    Node = node(Pid),
    case Node =:= node() of
        true ->
            is_process_alive(Pid);
        false ->
            try erpc:call(Node, erlang, is_process_alive, [Pid],
                          ?ALIVENESS_RPC_TIMEOUT) of
                Result ->
                    Result
            catch
                error:{erpc, _Reason} ->
                    %% If the RPC times out, the node is down, or unreachable,
                    %% we assume the process is dead to allow the new connection.
                    false
            end
    end.

try_put(Path,
        #conn{pid = ExistingPid} = ExistingConn,
        #conn{pid = NewPid} = NewConn) ->
    Opts = default_options(NewPid),
    case khepri:compare_and_swap(get_store_id(), Path, ExistingConn, NewConn,
                                 Opts) of
        ok ->
            ok;
        {error, Error} ->
            ?LOG_WARNING("Unexpected Khepri error for connection '~p', "
                         "old conn ~p, new conn ~p. Error is ~p.",
                         [Path, ExistingPid, NewPid, Error]),
            error
    end.

kill_connection_sproc(#khepri_trigger{type = tree,
                                      event = #{change := update,
                                                old_node_props := #{data := #conn{pid = Pid}}}}) ->
    exit(Pid, sole_conn_enforcement),
    ok;
kill_connection_sproc(Props) ->
    ?LOG_WARNING("Unexpected event for sole_conn stored procedure, "
                 "connection will not be instructed to close. Event: ~p",
                 Props),
    ok.

amqp_error(Cond, Desc, Info) ->
    #'v1_0.error'{
       condition = Cond,
       description = {utf8, Desc},
       info = {map, [Info]}}.

tick_interval() ->
    application:get_env(rabbit, amqp10_sole_conn_tick_interval,
                        ?TICK_INTERVAL).

%% Retrieves the Khepri members safely, even if the local store is offline
members() ->
    StoreId = get_store_id(),
    LocalServerId = {StoreId, node()},
    case whereis(?MODULE) of
        undefined ->
            %% The local store is not running (lazy init hasn't occurred).
            %% Query the other reachable RabbitMQ nodes to find the Raft leader.
            ExpectedMembers = [{StoreId, N} || N <- rabbit_nodes:list_reachable()],
            OtherMembers = lists:delete(LocalServerId, ExpectedMembers),
            case ra:members(OtherMembers) of
                {ok, Members, _Leader} ->
                    {ok, Members};
                Err ->
                    Err
            end;
        _Pid ->
            %% The local store is running, we can use the Khepri API directly
            khepri_cluster:members(StoreId)
    end.


%% for testing
conn(Pid, Username) ->
    #conn{pid = Pid, username = Username}.

%% --------------------------------------------------------------
%% Khepri paths
%% --------------------------------------------------------------

conn_path(VHost, ContainerId)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(ContainerId) ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [amqp10_sole_conn, ContainerId]).

kill_connection_sproc_path() ->
    ?RABBITMQ_KHEPRI_ROOT_PATH([amqp10_sole_conn, kill_connection]).

kill_connection_sproc_trigger_pattern() ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(?KHEPRI_WILDCARD_STAR_STAR,
                                [amqp10_sole_conn,
                                 ?KHEPRI_WILDCARD_STAR_STAR]).
