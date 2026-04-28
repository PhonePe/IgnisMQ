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

import com.phonepe.magazine.core.StorageType;
import lombok.Getter;

import javax.validation.constraints.NotNull;

/**
 * @author shantanu.tiwari
 */
@Getter
public abstract sealed class BaseStorage permits AerospikeStorage {
    @NotNull
    private final StorageType storageType;

    public BaseStorage(final StorageType storageType) {
        this.storageType = storageType;
    }

    public abstract <T> T accept(StorageVisitor<T> visitor);
}
