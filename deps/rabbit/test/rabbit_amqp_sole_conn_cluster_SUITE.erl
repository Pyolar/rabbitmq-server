-module(rabbit_amqp_sole_conn_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all, export_all]).

-define(SOLE_CONN_MOD, rabbit_amqp_sole_conn).
-define(STORE_ID, rabbit_amqp_sole_conn:get_store_id()).
-define(VH, <<"/">>).
-define(CID1, <<"id-1">>).
-define(CID2, <<"id-2">>).
-define(CID3, <<"id-3">>).
-define(USER, <<"user-1">>).

-define(LOGFMT_CONFIG, #{legacy_header => false,
                         single_line => false,
                         template => [time, " ", pid, ": ", msg, "\n"]}).

all() ->
    [{group, default_group}].

groups() ->
    [{default_group, [], [
        lazy_cluster_formation,
        restarted_node_rejoins_cluster
    ]}].

init_per_suite(Config) ->
    basic_logger_config(),
    ok = start_epmd(),
    case net_kernel:start([?MODULE, shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    Config.

end_per_suite(Config) ->
    net_kernel:stop(),
    Config.

init_per_group(default_group, Config) ->
    Config.

end_per_group(default_group, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    Nodes = start_n_nodes(Testcase, 3, Config0),
    NodeNames = [Node || {Node, _Peer} <- Nodes],
    ct:pal("Started peer nodes ~p", [Nodes]),
    
    Config1 = [{peer_nodes, Nodes} | Config0],
    
    lists:foreach(
        fun({Node, _Peer}) ->
            %% Mock rabbit_sup to bypass RabbitMQ boot
            call(Config1, Node, meck, new, [rabbit_sup, [passthrough, no_link]]),
            call(Config1, Node, meck, expect, [rabbit_sup, start_child, 
                fun(?SOLE_CONN_MOD) ->
                    gen_server:start({local, ?SOLE_CONN_MOD}, ?SOLE_CONN_MOD, [], []),
                    ok
                end]),
                
            %% Mock rabbit_nodes to return our 3 peers
            call(Config1, Node, meck, new, [rabbit_nodes, [passthrough, no_link]]),
            call(Config1, Node, meck, expect, [rabbit_nodes, list_running, 
                fun() -> NodeNames end])
        end, Nodes),
    Config1.

end_per_testcase(_Testcase, Config) ->
    Nodes = ?config(peer_nodes, Config),
    lists:foreach(
        fun({Node, _Peer}) ->
            %% Stop the gen_server safely
            call(Config, Node, erlang, apply, [fun() -> 
                case whereis(?SOLE_CONN_MOD) of
                    undefined -> ok;
                    Pid -> gen_server:stop(Pid)
                end
            end, []]),
            
            call(Config, Node, khepri, stop, [?STORE_ID]),
            
            try 
                call(Config, Node, meck, unload, [rabbit_nodes]) 
            catch 
                _:_ -> ok 
            end,
            
            try 
                call(Config, Node, meck, unload, [rabbit_sup]) 
            catch 
                _:_ -> ok 
            end
        end, Nodes),
    lists:foreach(
        fun({Node, _Peer}) ->
            call(Config, Node, application, stop, [khepri]),
            call(Config, Node, application, stop, [ra]),
            ok = stop_erlang_node(Config, Node)
        end, Nodes),
    Config.

%% -------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------

lazy_cluster_formation(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],
    
    ct:pal("Triggering acquire/4 on Node 1 (~p)", [Node1]),
    Pid1 = call(Config, Node1, erlang, spawn, [fun() -> receive die -> ok end end]),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),
    
    %% Verify Node 1 is the sole member
    {ok, Members1} = call(Config, Node1, khepri_cluster, members, [?STORE_ID]),
    ?assertEqual(1, length(Members1)),
    
    ct:pal("Triggering acquire/4 on Node 2 (~p)", [Node2]),
    Pid2 = call(Config, Node2, erlang, spawn, [fun() -> receive die -> ok end end]),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2),
    
    %% Verify Node 2 joined the cluster
    {ok, Members2} = call(Config, Node2, khepri_cluster, members, [?STORE_ID]),
    ?assertEqual(2, length(Members2)),
    
    ct:pal("Triggering acquire/4 on Node 3 (~p)", [Node3]),
    Pid3 = call(Config, Node3, erlang, spawn, [fun() -> receive die -> ok end end]),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3),
    
    %% Verify all 3 nodes are in the cluster
    {ok, Members3} = call(Config, Node3, khepri_cluster, members, [?STORE_ID]),
    ?assertEqual(3, length(Members3)),

    %% Simulate a conflict with a connection on node 1
    Pid4 = call(Config, Node3, erlang, spawn, [fun() -> receive die -> ok end end]),
    {error, refuse_connection} = acq_ref_conn(Config, Node3, ?VH, ?CID1, ?USER, Pid4),

    %% Cleanup the dummy processes
    call(Config, Node1, erlang, exit, [Pid1, kill]),
    call(Config, Node2, erlang, exit, [Pid2, kill]),
    call(Config, Node3, erlang, exit, [Pid3, kill]),
    call(Config, Node3, erlang, exit, [Pid4, kill]),
    ok.

restarted_node_rejoins_cluster(Config0) ->
    Nodes = ?config(peer_nodes, Config0),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],

    %% Form the initial cluster and write data
    Pid1 = call(Config0, Node1, erlang, spawn, [fun() -> receive die -> ok end end]),
    ok = acq_ref_conn(Config0, Node1, ?VH, ?CID1, ?USER, Pid1),
    Pid2 = call(Config0, Node2, erlang, spawn, [fun() -> receive die -> ok end end]),
    ok = acq_ref_conn(Config0, Node2, ?VH, ?CID2, ?USER, Pid2),
    Pid3 = call(Config0, Node3, erlang, spawn, [fun() -> receive die -> ok end end]),
    ok = acq_ref_conn(Config0, Node3, ?VH, ?CID3, ?USER, Pid3),

    ct:pal("Gracefully flushing Node 3 RA metadata before VM kill"),
    call(Config0, Node3, application, stop, [khepri]),
    call(Config0, Node3, application, stop, [ra]),

    %% Stop Node 3 to simulate a crash/shutdown
    ct:pal("Stopping Node 3 (~p)", [Node3]),
    stop_erlang_node(Config0, Node3),

    %% Restart Node 3 (which re-uses the same on-disk DataDir)
    ct:pal("Restarting Node 3 (~p)", [Node3]),
    NewPeer3 = restart_node(Node3, Config0),

    %% Update the Config so call/5 uses the new control PID for Node 3
    NewNodes = lists:keyreplace(Node3, 1, Nodes, {Node3, NewPeer3}),
    Config1 = lists:keyreplace(peer_nodes, 1, Config0, {peer_nodes, NewNodes}),

    %% Execute the boot step recovery natively
    ct:pal("Executing ensure_running/0 on Node 3"),
    ok = call(Config1, Node3, ?SOLE_CONN_MOD, ensure_running, []),

    %% Verify Node 3 is fully operational and rejoined
    %% Check that the gen_server successfully started
    RecoveredPid = call(Config1, Node3, erlang, whereis, [?SOLE_CONN_MOD]),
    ?assert(is_pid(RecoveredPid)),

    %% Query the cluster members from the recovered node to prove Raft synchronization
    {ok, RecoveredMembers} = call(Config1, Node3, khepri_cluster, members, [?STORE_ID]),
    ?assertEqual(3, length(RecoveredMembers)),

    %% Cleanup the dummy processes
    call(Config1, Node1, erlang, exit, [Pid1, kill]),
    call(Config1, Node2, erlang, exit, [Pid2, kill]),
    %% (Pid3 was naturally killed when Node3 was stopped)
    ok.

%% --------------------------------------------------------------
%% Internal Helpers
%% --------------------------------------------------------------

start_epmd() ->
    RootDir = code:root_dir(),
    ErtsVersion = erlang:system_info(version),
    ErtsDir = lists:flatten(io_lib:format("erts-~ts", [ErtsVersion])),
    EpmdPath0 = filename:join([RootDir, ErtsDir, "bin", "epmd"]),
    EpmdPath = case os:type() of
                   {win32, _} -> EpmdPath0 ++ ".exe";
                   _          -> EpmdPath0
               end,
    Port = erlang:open_port(
             {spawn_executable, EpmdPath},
             [{args, ["-daemon"]}]),
    erlang:port_close(Port),
    ok.

start_n_nodes(Prefix, Count, Config) ->
    PrivDir = ?config(priv_dir, Config),
    CodePath = code:get_path(),
    
    Nodes = [begin
                 Name = list_to_atom(lists:flatten(
                                       io_lib:format("~s-~s-~b",
                                                     [?MODULE, Prefix, I]))),
                 {ok, Peer, Node} = peer:start(#{name => Name, connection => standard_io}),
                 {Node, Peer}
             end || I <- lists:seq(1, Count)],

    lists:foreach(
        fun({Node, Peer}) ->
            peer:call(Peer, code, add_pathsz, [CodePath]),
            
            %% Setup a unique DataDir for this specific peer
            DataDir = filename:join(PrivDir, rabbit_misc:format("data-~ts", [Node])),
            filelib:ensure_dir(filename:join(DataDir, "dummy")),
            
            %% Setup correct log routing for CT reports natively in the peer
            peer:call(Peer, ?MODULE, setup_node, [], infinity),
            
            %% Load the rabbit application and set the env variables
            case peer:call(Peer, application, load, [rabbit]) of
                ok                           -> ok;
                {error, {already_loaded, _}} -> ok
            end,
            ct:pal("Using data_dir ~p for node ~p", [DataDir, Node]),
            ok = peer:call(Peer, application, set_env, [rabbit, data_dir, DataDir]),
            
            %% Start the dependencies
            {ok, _} = peer:call(Peer, application, ensure_all_started, [khepri]),
            ok = peer:call(Peer, rabbit_ra_systems, ensure_ra_system_started, [coordination])
        end, Nodes),
    Nodes.

%% Restarts an existing node using its exact previous short-name and DataDir,
%% and re-establishes the test mocks for the new VM.
restart_node(Node, Config) ->
    PrivDir = ?config(priv_dir, Config),
    CodePath = code:get_path(),
    %% peer:start/1 expects a short name (e.g. 'node_1'), not the full node address.
    %% We split the existing atom 'node_1@hostname' to extract just the short name.
    [ShortName, _Host] = string:split(atom_to_list(Node), "@"),
    Name = list_to_atom(ShortName),

    {ok, Peer, Node} = peer:start(#{name => Name, connection => standard_io}),

    %% Re-inject paths and setup logging
    peer:call(Peer, code, add_pathsz, [CodePath]),
    peer:call(Peer, ?MODULE, setup_node, [], infinity),
    %% Re-bind the exact same DataDir to trigger RA disk recovery
    DataDir = filename:join(PrivDir, rabbit_misc:format("data-~ts", [Node])),
    case peer:call(Peer, application, load, [rabbit]) of
        ok                           -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ct:pal("Using data_dir ~p for node ~p", [DataDir, Node]),
    ok = peer:call(Peer, application, set_env, [rabbit, data_dir, DataDir]),
    %% Start dependencies
    {ok, _} = peer:call(Peer, application, ensure_all_started, [khepri]),
    ok = peer:call(Peer, rabbit_ra_systems, ensure_ra_system_started, [coordination]),
    %% Re-establish all mocks on the fresh VM
    Nodes = ?config(peer_nodes, Config),
    NodeNames = [N || {N, _P} <- Nodes],
    peer:call(Peer, meck, new, [rabbit_sup, [passthrough, no_link]]),
    peer:call(Peer, meck, expect, [rabbit_sup, start_child,
        fun(?SOLE_CONN_MOD) ->
            gen_server:start({local, ?SOLE_CONN_MOD}, ?SOLE_CONN_MOD, [], []),
            ok
        end]),
    peer:call(Peer, meck, new, [rabbit_nodes, [passthrough, no_link]]),
    peer:call(Peer, meck, expect, [rabbit_nodes, list_running,
        fun() -> NodeNames end]),
    Peer.

stop_erlang_node(Config, Node) ->
    Nodes = ?config(peer_nodes, Config),
    case proplists:get_value(Node, Nodes) of
        undefined -> ok;
        Peer ->
            case is_process_alive(Peer) of
                true ->
                    peer:stop(Peer);
                false ->
                    try
                        erpc:cast(Node, erlang, halt, [])
                    catch
                        _:_ -> ok
                    end
            end
    end.

acq_ref_conn(Config, Node, VH, CID, Username, Pid) ->
    call(Config, Node, ?SOLE_CONN_MOD, acquire,
         [refuse_connection, VH, CID, Username, Pid]).

call(Config, Node, Module, Func, Args) ->
    Nodes = ?config(peer_nodes, Config),
    case proplists:get_value(Node, Nodes) of
        undefined ->
            case Node =:= node() of
                true -> erlang:apply(Module, Func, Args);
                false -> erlang:error({unknown_node, Node})
            end;
        Peer ->
            case is_process_alive(Peer) of
                true ->
                    peer:call(Peer, Module, Func, Args, infinity);
                false ->
                    %% Fallback control channel (TCP Distribution)
                    %% Used when a test dynamically restarts a node.
                    erpc:call(Node, Module, Func, Args, infinity)
            end
    end.

%% Forces logger to debug level, disables burst limits, and applies a clean single-line
%% formatting template to make Common Test HTML reports readable and verbose.
basic_logger_config() ->
    _ = logger:set_primary_config(level, debug),
    HandlerIds = [HandlerId ||
                  HandlerId <- logger:get_handler_ids(),
                  HandlerId =:= default orelse
                  HandlerId =:= cth_log_redirect],
    lists:foreach(
      fun(HandlerId) ->
              ok = logger:set_handler_config(
                    HandlerId, formatter,
                    {logger_formatter, ?LOGFMT_CONFIG}),
              ok = logger:update_handler_config(
                    HandlerId, config, #{burst_limit_enable => false}),
              _ = logger:add_handler_filter(
                    HandlerId, progress,
                    {fun logger_filters:progress/2,stop}),
              _ = logger:remove_handler_filter(
                    HandlerId, remote_gl)
      end, HandlerIds),
    ok.

%% Configures peer nodes to format logs cleanly for standard_io capture,
%% and sets strict Khepri timeouts so cluster tests fail fast instead of hanging.
setup_node() ->
    basic_logger_config(),
    
    %% Set strict timeouts for Khepri so we don't wait forever during network splits
    ok = application:set_env(
           khepri, default_timeout, 5000, [{persistent, true}]),
    ok.
