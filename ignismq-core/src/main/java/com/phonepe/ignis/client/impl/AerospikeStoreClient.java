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

package com.phonepe.ignis.client.impl;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.*;
import com.google.common.base.Strings;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.platform.aerospike.config.AerospikeConfiguration;
import com.phonepe.platform.aerospike.config.AerospikeHost;
import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class AerospikeStoreClient implements StorageClient<IAerospikeClient>, Managed {
    private final AerospikeConfiguration config;
    private IAerospikeClient client;

    public AerospikeStoreClient(final AerospikeConfiguration aerospikeConfiguration) {
        this.config = aerospikeConfiguration;
        this.start();
    }

    @Override
    public IAerospikeClient getClient() {
        return client;
    }

    @Override
    public void start() {
        if (Objects.nonNull(this.client)) {
            return;
        }

        val writePolicy = configureWritePolicy(config);
        val readPolicy = configureReadPolicy(config);
        val scanPolicy = configureScanPolicy(config);
        val batchPolicy = configureBatchPolicy(config);
        val clientPolicy = configureClientPolicy(config, readPolicy, writePolicy, scanPolicy, batchPolicy);


        log.info("Connecting to remote aerospike: hosts = {}", config.getHosts());
        val aerospikeHosts = hosts(config.getHosts());
        client = new AerospikeClient(clientPolicy, aerospikeHosts.toArray(new Host[0]));
        client.getNodeNames().forEach(n -> log.info("Node: {}", n));
        log.info("Aerospike connection response: " + client.isConnected());
    }

    @Override
    public void stop() {
        log.info("Killing aerospike connection");
        if (Objects.nonNull(client)) {
            client.close();
        }
    }

    private ClientPolicy configureClientPolicy(AerospikeConfiguration configuration,
                                               Policy readPolicy,
                                               WritePolicy writePolicy,
                                               ScanPolicy scanPolicy,
                                               BatchPolicy batchPolicy) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = configuration.getUser();
        clientPolicy.password = configuration.getPassword();
        clientPolicy.maxConnsPerNode = configuration.getMaxConnectionsPerNode();
        clientPolicy.failIfNotConnected = true;
        clientPolicy.threadPool = configuration.getThreadPoolSize() > 0
                ? Executors.newFixedThreadPool(configuration.getThreadPoolSize())
                : Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);

        clientPolicy.readPolicyDefault = readPolicy;
        clientPolicy.writePolicyDefault = writePolicy;
        clientPolicy.scanPolicyDefault = scanPolicy;
        clientPolicy.batchPolicyDefault = batchPolicy;
        final Set<String> tlsProtocols = configuration.getTlsProtocols();
        if (!Strings.isNullOrEmpty(configuration.getUser())
                && !Strings.isNullOrEmpty(configuration.getPassword())) {
            clientPolicy.tlsPolicy = new TlsPolicy();
        }
        if (tlsProtocols != null && !tlsProtocols.isEmpty()) {
            clientPolicy.tlsPolicy.protocols = tlsProtocols.toArray(new String[0]);
        }
        return clientPolicy;
    }

    private WritePolicy configureWritePolicy(final AerospikeConfiguration aerospikeConfiguration) {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.maxRetries = aerospikeConfiguration.getRetries();
        writePolicy.replica = Replica.MASTER_PROLES;
        writePolicy.sleepBetweenRetries = aerospikeConfiguration.getSleepBetweenRetries();
        writePolicy.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicy.socketTimeout = aerospikeConfiguration.getSocketTimeout();
        writePolicy.totalTimeout = aerospikeConfiguration.getTotalTimeout();
        writePolicy.sendKey = true;
        return writePolicy;
    }

    private Policy configureReadPolicy(final AerospikeConfiguration aerospikeConfiguration) {
        Policy readPolicy = new Policy();
        readPolicy.maxRetries = aerospikeConfiguration.getRetries();
        readPolicy.replica = Replica.MASTER_PROLES;
        readPolicy.sleepBetweenRetries = aerospikeConfiguration.getSleepBetweenRetries();
        readPolicy.socketTimeout = aerospikeConfiguration.getSocketTimeout();
        readPolicy.totalTimeout = aerospikeConfiguration.getTotalTimeout();
        readPolicy.sendKey = true;
        readPolicy.readModeSC = aerospikeConfiguration.getReadModeSC();
        return readPolicy;
    }

    private ScanPolicy configureScanPolicy(final AerospikeConfiguration aerospikeConfiguration) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.maxConcurrentNodes = aerospikeConfiguration.getScanMaxConcurrentNodes();
        scanPolicy.includeBinData = true;
        return scanPolicy;
    }

    private BatchPolicy configureBatchPolicy(final AerospikeConfiguration aerospikeConfiguration) {
        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.maxConcurrentThreads = aerospikeConfiguration.getBatchMaxConcurrentThreads();
        batchPolicy.allowInline = true;
        batchPolicy.readModeSC = aerospikeConfiguration.getReadModeSC();
        return batchPolicy;
    }

    private List<Host> hosts(final List<AerospikeHost> connections) {
        return connections.stream()
                .map(connection -> new Host(connection.getHost(), connection.getTlsName(), connection.getPort()))
                .collect(Collectors.toList());
    }

}