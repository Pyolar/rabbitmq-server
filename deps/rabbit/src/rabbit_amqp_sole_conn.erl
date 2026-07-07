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
-define(DEFAULT_COMMAND_OPTIONS, #{reply_from => local}).
-define(ALIVENESS_RPC_TIMEOUT, 1_000).

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

-export([recover/0,
         ensure_running/0,
         get_ra_system/0,
         get_store_id/0,
         init_schema/0,
         acquire/5,
         refuse_connection_error/0,
         close_existing_connection_error/0]).

%% for testing
-export([conn/2,
         try_put/3,
         conn_path/2]).

-type vhost() :: binary().
-type container_id() :: binary().
-type username() :: binary().

-record(conn, {pid :: pid(),
               username :: username()}).

%% --------------------------------------------------------------
%% gen_server callbacks
%% --------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:send_after(10_000, self(), cluster_tick),
    {ok, #{}}.

handle_info(cluster_tick, State) ->
    erlang:send_after(10_000, self(), cluster_tick),
    {noreply, State};
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
           amqp10_sole_conn_kill_connection,
           EventFilter,
           kill_connection_sproc_path(),
           Opts).

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
                        ?LOG_DEBUG("Starting sole_conn bootstrap sequence on ~p",
                                   [node()]),
                        StoreId = get_store_id(),

                        %% TODO get settings from global Khepri configuration
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
                                        ok = khepri_cluster:join(StoreId, PeerNode),
                                        ?LOG_DEBUG("Joined existing cluster, "
                                                   "waiting for effective behaviour"),
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
                        ok;
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

get_ra_system() ->
    ?RA_SYSTEM.

get_store_id() ->
    ?STORE_ID.

-spec acquire(none | enforcement_policy(), vhost(), container_id(), username(), pid()) ->
    ok | {error, refuse_connection | close_existing}.
acquire(none, _, _, _, _) ->
    ok;
acquire(Plcy, VHost, ContainerId, Username, ConnPid) ->
    ensure_running(),
    do_acquire(Plcy, VHost, ContainerId, Username, ConnPid).

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
