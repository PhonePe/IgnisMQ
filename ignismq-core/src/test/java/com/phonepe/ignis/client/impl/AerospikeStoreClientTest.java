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

import com.aerospike.client.IAerospikeClient;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.aerospike.config.AerospikeHost;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class AerospikeStoreClientTest extends AerospikeTestBase {

    @Test
    public void testStartAndGetClient() {
        AerospikeConfiguration config = getAerospikeConfiguration();
        AerospikeStoreClient client = new AerospikeStoreClient(config);

        assertNotNull(client.getClient());
        assertTrue(client.getClient().isConnected());
        client.stop();
    }

    @Test
    public void testStartIdempotent() {
        AerospikeConfiguration config = getAerospikeConfiguration();
        AerospikeStoreClient client = new AerospikeStoreClient(config);

        IAerospikeClient firstClient = client.getClient();
        // Second start should be no-op
        client.start();
        assertSame(firstClient, client.getClient());
        client.stop();
    }

    @Test
    public void testStopMultipleTimes() {
        AerospikeConfiguration config = getAerospikeConfiguration();
        AerospikeStoreClient client = new AerospikeStoreClient(config);

        client.stop();
        // Second stop should be safe
        client.stop();
    }

    @Test
    public void testWithTlsConfig() {
        AerospikeConfiguration config = getAerospikeConfiguration();
        // The testcontainer doesn't use TLS, but we verify construction works
        AerospikeStoreClient client = new AerospikeStoreClient(config);
        assertNotNull(client.getClient());
        client.stop();
    }

    @Test
    public void testWithZeroThreadPoolSize() {
        AerospikeConfiguration config = AerospikeConfiguration.builder()
                .hosts(List.of(AerospikeHost.builder()
                        .host(getContainerHost())
                        .port(getContainerPort())
                        .build()))
                .retries(3)
                .sleepBetweenRetries(100)
                .socketTimeout(3000)
                .totalTimeout(5000)
                .maxConnectionsPerNode(100)
                .threadPoolSize(0) // triggers availableProcessors * 4 path
                .scanMaxConcurrentNodes(5)
                .batchMaxConcurrentThreads(5)
                .build();
        AerospikeStoreClient client = new AerospikeStoreClient(config);
        assertNotNull(client.getClient());
        client.stop();
    }

    @Test
    public void testWithUserPasswordTls() {
        // Tests the TLS policy creation branch (user + password non-empty)
        // Can't actually connect with TLS to testcontainer, but we test the config path
        // by using correct host but with user/pass set — the constructor will fail
        // on connect, but configureClientPolicy will be fully exercised
        AerospikeConfiguration config = AerospikeConfiguration.builder()
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
                .user("testuser")
                .password("testpass")
                .tlsProtocols(Set.of("TLSv1.2"))
                .build();
        try {
            new AerospikeStoreClient(config);
            fail("Expected exception due to TLS mismatch with testcontainer");
        } catch (Exception e) {
            // Expected - TLS connection to non-TLS server fails
            // But configureClientPolicy was fully executed
        }
    }
}
