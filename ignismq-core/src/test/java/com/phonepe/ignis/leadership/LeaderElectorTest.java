/**
 * Copyright (c) 2026 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.ignis.leadership;

import com.phonepe.ignis.common.LoadBalancer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class LeaderElectorTest {

    private CuratorFramework curatorFramework;
    private LoadBalancer loadBalancer;

    @Before
    public void setUp() {
        curatorFramework = Mockito.mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        loadBalancer = Mockito.mock(LoadBalancer.class);
    }

    private LeaderElector createElector(Map<Integer, Set<LoadBalancer>> workers) {
        return new LeaderElector("CLIENT_ID", curatorFramework, workers);
    }

    private LeaderElector createDefaultElector() {
        return createElector(ImmutableMap.of(1, ImmutableSet.of(loadBalancer)));
    }

    private void setField(Object obj, String name, Object value) throws Exception {
        Field f = obj.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(obj, value);
    }

    private Object getField(Object obj, String name) throws Exception {
        Field f = obj.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(obj);
    }

    private void invokeUpdateState(LeaderElector le, boolean force) throws Exception {
        Method m = LeaderElector.class.getDeclaredMethod("updateState", boolean.class);
        m.setAccessible(true);
        m.invoke(le, force);
    }

    private void setupIsRunning(LeaderElector le, Map<Integer, AtomicBoolean> map) throws Exception {
        setField(le, "isRunning", map);
    }

    private void setupLeaderSelector(LeaderElector le, boolean hasLeadership) throws Exception {
        LeaderSelector ls = mock(LeaderSelector.class);
        when(ls.hasLeadership()).thenReturn(hasLeadership);
        setField(le, "leaderSelector", ls);
    }

    /**
     * Sets up the deep-stub curatorFramework so that:
     * - getChildren().usingWatcher(any).forPath(any) returns the given members
     * - checkExists().creatingParentContainersIfNeeded().forPath(any) returns a Stat (path exists)
     * - setData().forPath(any, any) returns a Stat
     * - getData().forPath(any) returns the given readerBytes
     */
    private void setupZkMocks(List<String> members, byte[] readerBytes) throws Exception {
        // getChildren chain - usingWatcher returns something with forPath
        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(members);

        // checkExists chain
        when(curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(anyString()))
                .thenReturn(new Stat());

        // setData chain
        when(curatorFramework.setData().forPath(anyString(), any(byte[].class)))
                .thenReturn(new Stat());

        // getData chain
        when(curatorFramework.getData().forPath(anyString()))
                .thenReturn(readerBytes);
    }

    // --- Basic state changed tests ---

    @Test
    public void testStateChangedLostConnection() {
        createDefaultElector().stateChanged(curatorFramework, ConnectionState.LOST);
    }

    @Test
    public void testStateChangedConnected() {
        createDefaultElector().stateChanged(curatorFramework, ConnectionState.CONNECTED);
    }

    @Test
    public void testStateChangedReconnected() {
        createDefaultElector().stateChanged(curatorFramework, ConnectionState.RECONNECTED);
    }

    @Test
    public void testStateChangedSuspended() {
        createDefaultElector().stateChanged(curatorFramework, ConnectionState.SUSPENDED);
    }

    @Test
    public void testStateChangedLostWhileLeader() throws Exception {
        LeaderElector le = createDefaultElector();
        ((AtomicBoolean) getField(le, "leader")).set(true);
        le.stateChanged(curatorFramework, ConnectionState.LOST);
        assertFalse(((AtomicBoolean) getField(le, "leader")).get());
    }

    // --- Stop tests ---

    @Test
    public void testStopSetsStopFlag() throws Exception {
        LeaderElector le = createDefaultElector();
        le.stop();
        assertTrue(((AtomicBoolean) getField(le, "stop")).get());
    }

    // --- Start test ---

    @Test
    public void testStartCreatesPathAndSelector() throws Exception {
        LeaderElector le = createDefaultElector();
        when(curatorFramework.create().creatingParentContainersIfNeeded()
                .withMode(any(CreateMode.class)).forPath(anyString())).thenReturn("");
        le.start();
        verify(curatorFramework, atLeastOnce()).create();
    }

    // --- TakeLeadership tests ---

    @Test
    public void testTakeLeadershipAndStop() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);
        setupZkMocks(List.of("member1"), "unknown".getBytes());

        Thread thread = new Thread(() -> {
            try {
                le.takeLeadership(curatorFramework);
            } catch (Exception ignored) {
            }
        });
        thread.start();
        Thread.sleep(200);
        le.stop();
        thread.join(5000);
        assertFalse(((AtomicBoolean) getField(le, "leader")).get());
    }

    @Test
    public void testTakeLeadershipAndLoseLeadership() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);
        setupZkMocks(List.of("member1"), "other".getBytes());

        Thread thread = new Thread(() -> {
            try {
                le.takeLeadership(curatorFramework);
            } catch (Exception ignored) {
            }
        });
        thread.start();
        Thread.sleep(200);
        le.stateChanged(curatorFramework, ConnectionState.LOST);
        thread.join(5000);
        assertFalse(((AtomicBoolean) getField(le, "leader")).get());
    }

    // --- updateState / peerCountChange tests ---

    @Test
    public void testUpdateStateWithNodeExistsException() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        ((AtomicBoolean) getField(le, "leader")).set(true);

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenThrow(new KeeperException.NodeExistsException());

        invokeUpdateState(le, true);
        // Graceful handling, no exception
    }

    @Test
    public void testUpdateStateWithGenericException() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenThrow(new RuntimeException("ZK error"));

        invokeUpdateState(le, true);
    }

    @Test
    public void testUpdateStateNotLeader() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, false);

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(List.of("member1"));

        // getData for updatePartitionWorkerState - will hit NoNodeException path
        when(curatorFramework.getData().forPath(anyString()))
                .thenThrow(new KeeperException.NoNodeException());

        invokeUpdateState(le, true);
    }

    @Test
    public void testPeerCountChangeNoMembershipChanges() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);
        setField(le, "knownMembers", Sets.newHashSet("member1"));

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(List.of("member1"));
        when(curatorFramework.getData().forPath(anyString()))
                .thenThrow(new KeeperException.NoNodeException());

        // force=false, same members => no reassignment
        invokeUpdateState(le, false);
    }

    // --- updatePartitionWorkerState tests ---

    @Test
    public void testUpdatePartitionWorkerStateActivate() throws Exception {
        LeaderElector le = createDefaultElector();
        String balancerId = (String) getField(le, "balancerId");
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);
        setupZkMocks(List.of(balancerId), balancerId.getBytes());

        invokeUpdateState(le, true);
        verify(loadBalancer).activate();
    }

    @Test
    public void testUpdatePartitionWorkerStateDeactivate() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(true))));
        setupLeaderSelector(le, true);
        setupZkMocks(List.of("other-member"), "other-member".getBytes());

        invokeUpdateState(le, true);
        verify(loadBalancer).deactivate();
    }

    @Test
    public void testUpdatePartitionWorkerStateNoNodeException() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(List.of("member1"));
        when(curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(anyString()))
                .thenReturn(new Stat());
        when(curatorFramework.setData().forPath(anyString(), any(byte[].class)))
                .thenReturn(new Stat());
        when(curatorFramework.getData().forPath(anyString()))
                .thenThrow(new KeeperException.NoNodeException());

        invokeUpdateState(le, true);
        verify(loadBalancer, never()).activate();
        verify(loadBalancer, never()).deactivate();
    }

    @Test
    public void testUpdatePartitionWorkerStateGenericException() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(List.of("member1"));
        when(curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(anyString()))
                .thenReturn(new Stat());
        when(curatorFramework.setData().forPath(anyString(), any(byte[].class)))
                .thenReturn(new Stat());
        when(curatorFramework.getData().forPath(anyString()))
                .thenThrow(new RuntimeException("ZK read error"));

        // updateState catches the exception from updatePartitionWorkerState
        invokeUpdateState(le, true);
    }

    @Test
    public void testPeerCountChangeSetDataException() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(List.of("member1"));
        when(curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(anyString()))
                .thenReturn(new Stat());
        when(curatorFramework.setData().forPath(anyString(), any(byte[].class)))
                .thenThrow(new RuntimeException("set error"));
        when(curatorFramework.getData().forPath(anyString()))
                .thenThrow(new KeeperException.NoNodeException());

        invokeUpdateState(le, true);
    }

    @Test
    public void testPeerCountChangeCreateCommunicatorPath() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(List.of("member1"));
        // checkExists returns null => needs to create communicator path
        when(curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(anyString()))
                .thenReturn(null);
        when(curatorFramework.create().creatingParentContainersIfNeeded().forPath(anyString()))
                .thenReturn("");
        when(curatorFramework.setData().forPath(anyString(), any(byte[].class)))
                .thenReturn(new Stat());
        when(curatorFramework.getData().forPath(anyString()))
                .thenThrow(new KeeperException.NoNodeException());

        invokeUpdateState(le, true);
    }

    @Test
    public void testMultipleWorkers() throws Exception {
        LoadBalancer lb2 = mock(LoadBalancer.class);
        Map<Integer, Set<LoadBalancer>> workers = ImmutableMap.of(
                1, ImmutableSet.of(loadBalancer),
                2, ImmutableSet.of(lb2));
        LeaderElector le = createElector(workers);
        String balancerId = (String) getField(le, "balancerId");

        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false), 2, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);
        setupZkMocks(List.of(balancerId, "other"), balancerId.getBytes());

        invokeUpdateState(le, true);
    }

    @Test
    public void testRescindMembershipWithNullCurator() throws Exception {
        LeaderElector le = createDefaultElector();
        // Just call stop which internally calls rescindMembership
        // curatorFramework is not null but delete might fail - that's ok
        when(curatorFramework.delete().forPath(anyString())).thenThrow(new RuntimeException("delete failed"));
        le.stop();
        assertTrue(((AtomicBoolean) getField(le, "stop")).get());
    }

    @Test
    public void testAlreadyRunningActivateNoOp() throws Exception {
        LeaderElector le = createDefaultElector();
        String balancerId = (String) getField(le, "balancerId");
        // Already running = true, same reader => no activate call
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(true))));
        setupLeaderSelector(le, true);
        setupZkMocks(List.of(balancerId), balancerId.getBytes());

        invokeUpdateState(le, true);
        // Already running, same reader => compareAndSet(false, true) fails => no activate
        verify(loadBalancer, never()).activate();
    }

    @Test
    public void testAlreadyStoppedDeactivateNoOp() throws Exception {
        LeaderElector le = createDefaultElector();
        // Already stopped = false, different reader => no deactivate call
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        setupLeaderSelector(le, true);
        setupZkMocks(List.of("other"), "other".getBytes());

        invokeUpdateState(le, true);
        // Already not running, different reader => compareAndSet(true, false) fails => no deactivate
        verify(loadBalancer, never()).deactivate();
    }

    @Test
    public void testNullLeaderSelector() throws Exception {
        LeaderElector le = createDefaultElector();
        setupIsRunning(le, new HashMap<>(Map.of(1, new AtomicBoolean(false))));
        // leaderSelector is null by default

        when(curatorFramework.getChildren().usingWatcher(any(Watcher.class)).forPath(anyString()))
                .thenReturn(List.of("member1"));
        when(curatorFramework.getData().forPath(anyString()))
                .thenThrow(new KeeperException.NoNodeException());

        invokeUpdateState(le, true);
    }
}
