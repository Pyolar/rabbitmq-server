%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_sole_conn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_amqp_sole_conn,
        [acquire/4]).

-import(rabbit_ct_helpers,
        [eventually/1, eventually/3]).

-import(rabbit_amqp_sole_conn,
        [get_store_id/0,
         get_ra_system/0]).

-define(VH, <<"/">>).
-define(CID1, <<"id-1">>).

all() ->
    [
      {group, default_group}
    ].

groups() ->
    [
     {default_group, [shuffle],
      [
        refuse_connection_should_refuse_new_connection_if_conflict,
        refuse_connection_let_new_through_if_previous_died,
        close_existing_should_close_existing_connection,
        try_put,
        khepri_put_should_override_keep_while_monitor,
        khepri_triggers,
        khepri_cas
      ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    ok = meck:new(rabbit_nodes, [passthrough, no_link]),
    ok = meck:expect(rabbit_nodes, list_running, fun() -> [node()] end),
    ok = meck:new(rabbit_sup, [passthrough, no_link]),
    ok = meck:expect(rabbit_sup, start_child,
                     fun(rabbit_amqp_sole_conn) ->
                             %% We use gen_server:start instead of start_link
                             %% so it survives the death of the init_per_group process
                             gen_server:start({local, rabbit_amqp_sole_conn},
                                              rabbit_amqp_sole_conn, [], []),
                             ok
                     end),
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join(
                PrivDir,
                rabbit_misc:format("data-~ts", [node()])),
    case application:load(rabbit) of
        ok                           -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ok = application:set_env(rabbit, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(khepri),
    ok = rabbit_ra_systems:ensure_ra_system_started(get_ra_system()),
    rabbit_amqp_sole_conn:ensure_running(),
    Config.

end_per_group(_, Config) ->
    case whereis(rabbit_amqp_sole_conn) of
        undefined ->
            ok;
        Pid ->
            gen_server:stop(Pid)
    end,
    ok = khepri:stop(get_store_id()),
    ok = application:stop(khepri),
    ok = ra_system:stop(get_ra_system()),
    ok = application:stop(ra),
    ok = meck:unload(rabbit_sup),
    ok = meck:unload(rabbit_nodes),
    Config.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    clean_store_from_connections(),
    Config.

refuse_connection_should_refuse_new_connection_if_conflict(_) ->
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    Pid2 = spawn_disposable(),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, Pid2)),
    Pid2 ! die,
    Pid1 ! die,
    ok.

refuse_connection_let_new_through_if_previous_died(_) ->
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, self())),
    Pid1 ! die,
    eventually(?_assertNot(is_process_alive(Pid1))),
    Pid2 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid2)),
    Pid2 ! die,
    ok.

close_existing_should_close_existing_connection(_) ->
    TestPid = self(),
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn(fun() ->
                         process_flag(trap_exit, true),
                         receive
                             {'EXIT', _, sole_conn_enforcement} ->
                                 TestPid ! close_sole_conn_enforcement_received
                         after 5000 ->
                                   ok
                         end
                 end),
    Pid2 = spawn_disposable(),
    %% 1 takes the lease
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, Pid1)),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid1)},
                 khepri:get(get_store_id(), Path)),
    %% 2 takes the lease from 1
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, Pid2)),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid2)},
                 khepri:get(get_store_id(), Path)),
    %% 1 must have received the enforcement message
    ?assertEqual(ok, receive close_sole_conn_enforcement_received -> ok
                     after 5000 -> timeout end),
    %% 1 should have stopped
    eventually(?_assertNot(is_process_alive(Pid1))),

    Pid2 ! die,

    eventually(?_assertMatch({error, _}, khepri:get(get_store_id(), Path))),
    ok.

try_put(_) ->
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn_disposable(),
    Conn1 = rabbit_amqp_sole_conn:conn(Pid1),
    %% acquire lease
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    ?assertEqual({ok, Conn1}, khepri:get(get_store_id(), Path)),
    %% simulating new incoming connection
    Pid2 = spawn_disposable(),
    Conn2 = rabbit_amqp_sole_conn:conn(Pid2),
    %% new connection manages to replace old connection
    ?assertEqual(ok, rabbit_amqp_sole_conn:try_put(Path, Conn1, Conn2)),
    ?assertEqual({ok, Conn2}, khepri:get(get_store_id(), Path)),
    %% new connection arrives, but a bit slower than the second one,
    %% it still sees Conn1 in the datastore
    Pid3 = spawn_disposable(),
    Conn3 = rabbit_amqp_sole_conn:conn(Pid3),
    ?assertEqual(error, rabbit_amqp_sole_conn:try_put(Path, Conn1, Conn3)),
    ?assertEqual({ok, Conn2}, khepri:get(get_store_id(), Path)),

    %% can't take the lease, conn2 has it
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    %% conn2 dies, it should release the lease
    Pid2 ! die,
    %% we try to take the lease with conn3 (the other 2 connections are dead)
    eventually(?_assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid3))),

    Pid3 ! die,
    ok.

khepri_put_should_override_keep_while_monitor(_) ->
    Pid1 = spawn_disposable(),
    Opts1 = #{keep_while => Pid1},
    Path1 = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    ?assertMatch({ok, _}, khepri_adv:create(get_store_id(), Path1, Pid1, Opts1)),
    ?assertEqual({ok, Pid1}, khepri:get(get_store_id(), Path1)),

    Pid2 = spawn_disposable(),
    Opts2 = #{keep_while => Pid2},
    ?assertMatch(ok, khepri:put(get_store_id(), Path1, Pid2, Opts2)),
    ?assertEqual({ok, Pid2}, khepri:get(get_store_id(), Path1)),

    %% making sure that the node does not monitor the first PID anymore
    Pid1 ! die,
    timer:sleep(500),
    ?assertEqual({ok, Pid2}, khepri:get(get_store_id(), Path1)),
    Pid2 ! die,
    eventually(?_assertMatch({error, {khepri, node_not_found, _}},
                             khepri:get(get_store_id(), Path1))),
    ok.

khepri_triggers(_) ->
    Key = ?FUNCTION_NAME,
    StoredProcPath = [rmq, sole_conn, proc],
    Pid = self(),
    Proc = fun(Props) ->
                   #khepri_trigger{type = tree,
                                   event = #{path := Path, change := Change}} = Props,
                   Pid ! {sproc, Key, {Change, Path}}
           end,

    khepri_adv:put(get_store_id(), StoredProcPath, Proc),

    EventFilter = khepri_evf:tree([rmq, vhosts,
                                   ?KHEPRI_WILDCARD_STAR_STAR,
                                   sole_conn,
                                   ?KHEPRI_WILDCARD_STAR_STAR],
                                  #{on_actions => [update, delete]}),

    %% we want to try closing the connection on all members
    %% this way a connection on an isolated node will get the message
    %% when the partition ends.
    Opts = #{where => all_members},
    ok = khepri:register_trigger(
           get_store_id(),
           sole_conn,
           EventFilter,
           StoredProcPath,
           Opts),

    Path1 = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    ?assertMatch({ok, _}, khepri_adv:create(get_store_id(), Path1, <<"1">>)),

    ?assertMatch({ok, _}, khepri_adv:put(get_store_id(), Path1, <<"2">>)),
    ?assertEqual(executed, receive_sproc_msg(Key, {update, Path1})),

    ?assertMatch({ok, _}, khepri_adv:delete(get_store_id(), Path1)),
    ?assertEqual(executed, receive_sproc_msg(Key, {delete, Path1})),
    %% the tree nodes created implictly are deleted automatically
    eventually(?_assertMatch({error, {khepri, node_not_found, _}},
                             khepri:get(get_store_id(), Path1))),
    ok.

khepri_cas(_) ->
    StoreId = get_store_id(),
    Path = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    Pid1 = spawn_disposable(),
    Pid2 = spawn_disposable(),
    Pid3 = spawn_disposable(),
    V1 = rabbit_amqp_sole_conn:conn(Pid1),
    V2 = rabbit_amqp_sole_conn:conn(Pid2),
    V3 = rabbit_amqp_sole_conn:conn(Pid3),

    ?assertMatch(ok, khepri:create(get_store_id(), Path, V1)),
    ?assertEqual({ok, V1}, khepri:get(StoreId, Path)),

    ?assertMatch(ok,
                 khepri:compare_and_swap(StoreId, Path, V1, V2)),
    ?assertEqual({ok, V2}, khepri:get(StoreId, Path)),
    ?assertMatch({error, _},
                 khepri:compare_and_swap(StoreId, Path, V1, V3)),
    ?assertEqual({ok, V2}, khepri:get(StoreId, Path)),

    ?assertMatch(ok, khepri:delete(StoreId, Path)),
    ?assertMatch({error, _}, khepri:get(StoreId, Path)),
    ?assertMatch({error, _},
                 khepri:compare_and_swap(StoreId, Path, V1, V2)),
    ok.

%% --------------------------------------------------------------
%% Internal Helpers
%% --------------------------------------------------------------

clean_store_from_connections() ->
    ok = khepri:delete(get_store_id(), [rabbitmq, vhosts]).

spawn_disposable() ->
    spawn(fun() -> receive die -> ok end end).


receive_sproc_msg(Key, V) ->
    receive {sproc, Key, V} -> executed
    after 1000              -> timeout
    end.
