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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author shantanu.tiwari
 * Created on 14/03/22
 */
@Slf4j
public class LeaderElector implements LeaderSelectorListener {
    private static final int INITIAL_DELAY_IN_SEC = 2;
    private static final int DELAY_IN_SEC = 30;
    private final String clientId;
    private final String balancerId;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicBoolean leader = new AtomicBoolean();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final CuratorFramework curatorFramework;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<Integer, Set<LoadBalancer>> workers;
    private Map<Integer, AtomicBoolean> isRunning = Maps.newHashMap();
    private Set<String> knownMembers = Sets.newHashSet();
    private LeaderSelector leaderSelector;

    private final Watcher memberWatcher = event -> {
        log.debug("[{}] Got watcher event on path = {}, type = {}",
                Thread.currentThread().getId(),
                event.getPath(),
                event.getType());
        switch (event.getType()) {
            case None:
            case NodeDataChanged:
            case NodeCreated:
            case NodeDeleted:
                break;
            case NodeChildrenChanged:
                if (leader.get()) {
                    updateState(false);
                }
                break;
        }
    };

    public LeaderElector(final String clientId, final CuratorFramework curatorFramework,
                         final Map<Integer, Set<LoadBalancer>> workers) {
        this.clientId = clientId;
        this.curatorFramework = curatorFramework;
        this.workers = workers;
        this.balancerId = UUID.randomUUID().toString();
    }


    @Override
    public void takeLeadership(final CuratorFramework curatorFramework) throws Exception {
        leader.set(true);
        log.info("[{}:{}] This coordinator became the leader.", clientId, balancerId);
        updateState(true);
        while (true) {
            lock.lock();
            try {
                while (!stop.get() && leader.get()) {
                    condition.await();
                }
                boolean breakLoop = false;
                if (stop.get()) {
                    leader.compareAndSet(true, false);
                    log.info("[{}:{}] Exiting coordinator.", clientId, balancerId);
                    breakLoop = true;
                }
                if (!leader.get()) {
                    log.info("[{}:{}] This coordinator is not a leader anymore, giving up leadership",
                            clientId, balancerId);
                    breakLoop = true;
                }
                if (breakLoop) {
                    break;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void stateChanged(final CuratorFramework curatorFramework, final ConnectionState newState) {
        if (!newState.isConnected()) {
            log.debug("Lost connection to zk");
            lock.lock();
            try {
                if (leader.get()) {
                    log.debug("[{}:{}] Giving away leadership", clientId, balancerId);
                    leader.set(false);
                    condition.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void start() throws Exception {
        workers.keySet().forEach(worker -> isRunning.put(worker, new AtomicBoolean(false)));
        scheduler.scheduleWithFixedDelay(() -> updateState(false), INITIAL_DELAY_IN_SEC, DELAY_IN_SEC, TimeUnit.SECONDS);
        log.info("[{}] Watching reader path: {}", clientId, memberPathPrefix());
        final String leaderPath = String.format("/%s-ignis-workers/%s/loadbalancer-leader", clientId, clientId);
        this.leaderSelector = new LeaderSelector(curatorFramework, leaderPath, this);
        log.info("Starting leader selector at: " + leaderPath);
        curatorFramework.create().creatingParentContainersIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(memberPath());
        log.info("Member path created at: " + memberPath());
        leaderSelector.start();
    }

    public void stop() {
        lock.lock();
        try {
            stop.set(true);
            condition.signal();
            rescindMembership();
        } finally {
            lock.unlock();
        }
    }

    private synchronized void updateState(boolean force) {
        log.debug("Checking zookeeper for topology changes");
        try {
            // do a reassignment of workers to partitions if necessary
            peerCountChange(force);
            // update state of partitions assigned to this worker (turn on/off processing)
            updatePartitionWorkerState();
        } catch (Exception t) {
            log.info("Error detecting membership changes.", t);
        }
    }

    private void peerCountChange(boolean force) {
        final String memberPath = memberPathPrefix();
        List<String> members = null;
        try {
            members = curatorFramework.getChildren().usingWatcher(memberWatcher).forPath(memberPath);
            log.debug("Members: " + members);
            if (Sets.symmetricDifference(knownMembers, Sets.newHashSet(members)).isEmpty() && !force) {
                log.debug("No membership changes detected");
                return;
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Looks like this client combination is being used for the first time");
        } catch (Exception e) {
            log.error("Error checking for node on ZK: ", e);
        }
        if (Objects.isNull(members)) {
            log.error("No members found... how did i come here? ZK issue?");
            return;
        }
        if (Objects.isNull(leaderSelector) || !leaderSelector.hasLeadership()) {
            log.debug("I'm not the leader coordinator");
            return;
        }

        knownMembers = Sets.newHashSet(members);
        final List<String> finalMembers = members;
        AtomicInteger counter = new AtomicInteger(0);
        workers.keySet().forEach(partition -> {
            String selectedReader = finalMembers.get(counter.getAndIncrement() % finalMembers.size());
            log.info("[{}:{}] Selected reader: {}", clientId, partition, selectedReader);
            final String communicatorPath = communicatorPath(partition);
            try {
                if (null == curatorFramework.checkExists().creatingParentContainersIfNeeded()
                        .forPath(communicatorPath)) {
                    curatorFramework.create().creatingParentContainersIfNeeded().forPath(communicatorPath);
                    log.info("[{}:{}] Created communicator", clientId, partition);
                }
                curatorFramework.setData().forPath(communicatorPath, selectedReader.getBytes());
                log.error("Set reader at {} to {}", communicatorPath, selectedReader);
            } catch (Exception e) {
                log.error("Error setting reader value at " + communicatorPath + " to " + selectedReader, e);
            }
        });
    }

    private void updatePartitionWorkerState() {
        workers.keySet().forEach(partition -> {
            try {
                final String communicatorPath = communicatorPath(partition);
                byte[] data = curatorFramework.getData().forPath(communicatorPath);
                final String selectedReader = new String(data);
                if (balancerId.equals(selectedReader)) {
                    if (isRunning.get(partition).compareAndSet(false, true)) {
                        log.info("[{}:{}] Got activate()", clientId, partition);
                        workers.get(partition).forEach(LoadBalancer::activate);
                    }
                } else {
                    if (isRunning.get(partition).compareAndSet(true, false)) {
                        log.info("[{}:{}] Got deactivate()", clientId, partition);
                        workers.get(partition).forEach(LoadBalancer::deactivate);
                    }
                }
            } catch (KeeperException.NoNodeException e) {
                log.info("[{}:{}] Communicator not yet initialized", clientId, partition);
            } catch (Exception e) {
                log.error("[{}:{}] Error reading ZK node for reader id", clientId, partition);
                throw new IllegalStateException(e);
            }
        });
    }

    private void rescindMembership() {
        String memberPath = memberPath();
        log.info("Deleting membership at {}", memberPath);
        try {
            if (Objects.nonNull(curatorFramework)) {
                curatorFramework.delete().forPath(memberPath);
            }
        } catch (Exception e) {
            log.error("Error deleting membership at {}", memberPath, e);
        }
    }

    private String communicatorPath(int partition) {
        return String.format("/%s-ignis-workers/%s/readers/%d", clientId, clientId, partition);
    }

    private String memberPath() {
        return String.format("/%s-ignis-workers/%s/loadbalancer/%s", clientId, clientId, balancerId);
    }

    private String memberPathPrefix() {
        return String.format("/%s-ignis-workers/%s/loadbalancer", clientId, clientId);
    }
}
