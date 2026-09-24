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

package com.phonepe.ignis.storage;

import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.magazine.entity.StorageType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AerospikeStorageTest {

    @Test
    void testGetters() {
        AerospikeConfiguration config = AerospikeConfiguration.builder()
                .hosts(Collections.emptyList())
                .build();
        AerospikeStorage storage = new AerospikeStorage(config, "test-namespace");

        assertEquals("test-namespace", storage.getNamespace());
        assertEquals(config, storage.getConfiguration());
        assertEquals(StorageType.AEROSPIKE, storage.getStorageType());
    }

    @Test
    void testAcceptVisitor() {
        AerospikeConfiguration config = AerospikeConfiguration.builder()
                .hosts(Collections.emptyList())
                .build();
        AerospikeStorage storage = new AerospikeStorage(config, "test-namespace");

        String result = storage.accept(new StorageVisitor<String>() {
            @Override
            public String visit(AerospikeStorage aerospikeStorage) {
                return "visited:" + aerospikeStorage.getNamespace();
            }
        });

        assertEquals("visited:test-namespace", result);
    }
}
