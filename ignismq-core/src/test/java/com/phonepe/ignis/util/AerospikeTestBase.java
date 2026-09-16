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

package com.phonepe.ignis.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.aerospike.config.AerospikeHost;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.storage.BaseStorage;
import io.appform.testcontainers.aerospike.AerospikeContainerConfiguration;
import io.appform.testcontainers.aerospike.container.AerospikeContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;

/**
 * Base class for all tests that require a real Aerospike instance via testcontainer.
 */
@Slf4j
public abstract class AerospikeTestBase {
    public static final String AEROSPIKE_HOST = "localhost";
    public static final String AEROSPIKE_DOCKER_IMAGE = "aerospike/aerospike-server:6.2.0.7";
    public static final String AEROSPIKE_NAMESPACE = "ignismq";
    public static final int AEROSPIKE_PORT = 3000;
    public static final String CLIENT_ID = "CLIENT_ID";
    public static final String FARM_ID = "AB1";

    private static final AerospikeContainer AEROSPIKE_DOCKER_CONTAINER;

    static {
        AerospikeContainerConfiguration config = new AerospikeContainerConfiguration(
                true, AEROSPIKE_DOCKER_IMAGE, AEROSPIKE_NAMESPACE, AEROSPIKE_HOST, AEROSPIKE_PORT);
        config.setWaitTimeoutInSeconds(300L);
        AEROSPIKE_DOCKER_CONTAINER = new AerospikeContainer(config);
        AEROSPIKE_DOCKER_CONTAINER.start();
    }

    protected IAerospikeClient aerospikeClient;

    @BeforeEach
    public void setUpAerospikeClient() {
        aerospikeClient = new AerospikeClient(new ClientPolicy(),
                new Host(AEROSPIKE_DOCKER_CONTAINER.getHost(), AEROSPIKE_DOCKER_CONTAINER.getConnectionPort()));
        truncateNamespace();
    }

    @AfterEach
    public void tearDownAerospikeClient() {
        if (aerospikeClient != null) {
            truncateNamespace();
            aerospikeClient.close();
            aerospikeClient = null;
        }
    }

    protected static String getContainerHost() {
        return AEROSPIKE_DOCKER_CONTAINER.getHost();
    }

    protected static int getContainerPort() {
        return AEROSPIKE_DOCKER_CONTAINER.getConnectionPort();
    }

    protected void truncateNamespace() {
        aerospikeClient.truncate(
                ((AerospikeClient) aerospikeClient).getInfoPolicyDefault(),
                AEROSPIKE_NAMESPACE, null, null);
    }

    protected AerospikeConfiguration getAerospikeConfiguration() {
        return AerospikeConfiguration.builder()
                .hosts(List.of(AerospikeHost.builder()
                        .host(getContainerHost())
                        .port(getContainerPort())
                        .build()))
                .retries(3)
                .sleepBetweenRetries(100)
                .socketTimeout(3000)
                .totalTimeout(5000)
                .maxConnectionsPerNode(100)
                .threadPoolSize(4)
                .scanMaxConcurrentNodes(5)
                .batchMaxConcurrentThreads(5)
                .build();
    }

    protected BaseStorage createBaseStorage() {
        return new AerospikeStorage(getAerospikeConfiguration(), AEROSPIKE_NAMESPACE);
    }

    protected AerospikeQueueService createQueueService() {
        return new AerospikeQueueService(
                aerospikeClient, getAerospikeConfiguration(),
                AEROSPIKE_NAMESPACE, CLIENT_ID, FARM_ID);
    }
}
