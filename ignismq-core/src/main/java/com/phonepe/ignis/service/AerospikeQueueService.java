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

package com.phonepe.ignis.service;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.github.rholder.retry.*;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class AerospikeQueueService implements QueueService {
    private static final int DO_NOT_UPDATE_TTL = -2;
    private static final String ACTIVE_BIN = "active";
    private static final String MESSAGE_EXPIRY_BIN = "messageExpiry";
    private static final String SHARDS_BIN = "shards";
    private static final String QUEUE_EXPIRY_BIN = "queueExpiry";
    private static final String CONCURRENCY_BIN = "concurrency";
    private static final String MESSAGE_HANDLER_TYPE_BIN = "handlerType";
    private static final String SHOVEL_TIME_INTERVAL_IN_SECS_BIN = "shovelInterval";
    private static final String SHOVEL_CONCURRENCY_BIN = "shovelConcur";
    private static final String CREATED_AT_BIN = "createdAt";
    private static final String SWEEP_POINTERS_BIN = "sweepPointers";
    private static final String SWEPT_COUNTER_BIN = "sweptCounter";
    private static final String SIDELINE_SWEEP_POINTERS_BIN = "sidelineSweep";
    private static final String SIDELINE_SWEPT_COUNTER_BIN = "sidelineSwept";
    private static final String SWEEP_DURATION_BIN = "sweepDuration";
    private static final String HANDLER_TIMEOUT_BIN = "handlerTimeout";
    private static final String MAX_BATCH_SIZE = "maxBatchSize";
    private static final String MAX_WAIT_TIME = "maxWaitTime";
    private static final String SET_FORMAT = "%s_%s_ignis_queues";

    private final IAerospikeClient client;
    private final String namespace;
    private final String setName;
    private final Retryer<Object> retryer;

    public AerospikeQueueService(final IAerospikeClient client,
                                 final AerospikeConfiguration configuration,
                                 final String namespace,
                                 final String clientId,
                                 final String farmId) {
        this.client = client;
        this.namespace = namespace;
        this.setName = getSetName(farmId, clientId);
        this.retryer = RetryerBuilder.newBuilder()
                .retryIfExceptionOfType(AerospikeException.class)
                .withStopStrategy(StopStrategies.stopAfterAttempt(configuration.getRetries()))
                .withWaitStrategy(WaitStrategies.fixedWait(configuration.getSleepBetweenRetries(), TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
        createIndex(String.format("%s_%s", setName, ACTIVE_BIN), ACTIVE_BIN, IndexType.STRING);
    }

    @Override
    public boolean exists(final String name) {
        try {
            return (Boolean) retryer.call(() ->
                    client.exists(client.getReadPolicyDefault(), new Key(namespace, setName, name)));
        } catch (Exception e) {
            log.error("Error getting queue record from AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    @Override
    public Optional<QueueEntity> get(final String name) {
        try {
            final Record record = (Record) retryer.call(() ->
                    client.get(client.getReadPolicyDefault(), new Key(namespace, setName, name)));
            return Objects.nonNull(record) ?
                    Optional.of(buildQueueEntity(record))
                    : Optional.empty();
        } catch (Exception e) {
            log.error("Error getting queue record from AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    @Override
    public void store(final String name, final QueueEntity entity, final int ttl) {
        try {
            retryer.call(() -> {
                final List<Bin> binList = new ArrayList<>();
                binList.add(new Bin(ACTIVE_BIN, Boolean.toString(entity.isActive())));
                binList.add(new Bin(MESSAGE_EXPIRY_BIN, entity.getMessageExpiry()));
                binList.add(new Bin(SHARDS_BIN, entity.getShards()));
                binList.add(new Bin(QUEUE_EXPIRY_BIN, entity.getQueueExpiry()));
                binList.add(new Bin(CONCURRENCY_BIN, entity.getConcurrency()));
                binList.add(new Bin(MESSAGE_HANDLER_TYPE_BIN, entity.getMessageHandlerType()));
                binList.add(new Bin(SHOVEL_CONCURRENCY_BIN, entity.getShovelConcurrency()));
                binList.add(new Bin(SHOVEL_TIME_INTERVAL_IN_SECS_BIN, entity.getShovelTimeIntervalInSecs()));
                binList.add(new Bin(CREATED_AT_BIN, entity.getCreatedAt()));
                binList.add(new Bin(SWEEP_DURATION_BIN, entity.getSweepDuration()));
                binList.add(new Bin(HANDLER_TIMEOUT_BIN, entity.getHandlerTimeout()));
                if (Objects.nonNull(entity.getBatchingConfig())) {
                    binList.add(new Bin(MAX_BATCH_SIZE, entity.getBatchingConfig().getMaxBatchSize()));
                    binList.add(new Bin(MAX_WAIT_TIME, entity.getBatchingConfig().getMaxWaitTimeInSecs()));
                }
                client.put(createWritePolicy(ttl), new Key(namespace, setName, name), binList.toArray(new Bin[0]));
                return true;
            });
        } catch (Exception e) {
            log.error("Error storing queue record in AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    @Override
    public void updateState(final String name, final boolean active) {
        try {
            retryer.call(() -> {
                final List<Bin> binList = new ArrayList<>();
                binList.add(new Bin(ACTIVE_BIN, Boolean.toString(active)));
                client.put(createWritePolicy(DO_NOT_UPDATE_TTL), new Key(namespace, setName, name), binList.toArray(new Bin[0]));
                return true;
            });
        } catch (Exception e) {
            log.error("Error updating queue state in AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    @Override
    public void updateConcurrency(final String name, final int concurrency) {
        try {
            retryer.call(() -> {
                final List<Bin> binList = new ArrayList<>();
                binList.add(new Bin(CONCURRENCY_BIN, concurrency));
                client.put(createWritePolicy(DO_NOT_UPDATE_TTL), new Key(namespace, setName, name), binList.toArray(new Bin[0]));
                return true;
            });
        } catch (Exception e) {
            log.error("Error updating concurrency in AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    @Override
    public void updateShovelConfig(final String name, final int shovelConcurrency, final int shovelTimeInterval) {
        try {
            retryer.call(() -> {
                final List<Bin> binList = new ArrayList<>();
                binList.add(new Bin(SHOVEL_CONCURRENCY_BIN, shovelConcurrency));
                binList.add(new Bin(SHOVEL_TIME_INTERVAL_IN_SECS_BIN, shovelTimeInterval));
                client.put(createWritePolicy(DO_NOT_UPDATE_TTL), new Key(namespace, setName, name), binList.toArray(new Bin[0]));
                return true;
            });
        } catch (Exception e) {
            log.error("Error updating concurrency in AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    @Override
    public Map<String, QueueEntity> getQueues(final boolean active) {
        final Map<String, QueueEntity> activeQueuesMap = new HashMap<>();
        final Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(setName);
        statement.setFilter(Filter.equal(ACTIVE_BIN, Boolean.toString(active)));
        try {
            final RecordSet rs = (RecordSet) retryer.call(() ->
                    client.query(client.getQueryPolicyDefault(), statement));
            rs.iterator().forEachRemaining(keyRecord ->
                    activeQueuesMap.put(
                            ((String) keyRecord.key.userKey.getObject()),
                            buildQueueEntity(keyRecord.record)
                    )
            );
            return activeQueuesMap;
        } catch (Exception e) {
            log.error("Error getting queues from AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    @Override
    public void updateSweepProgress(final String queueName, final boolean isSideline,
                                    final Map<String, Long> shardPointers, final long sweptCounter) {
        if (shardPointers.isEmpty()) {
            return;
        }
        final String sweepPointersBin = isSideline ? SIDELINE_SWEEP_POINTERS_BIN : SWEEP_POINTERS_BIN;
        final String sweptCounterBin = isSideline ? SIDELINE_SWEPT_COUNTER_BIN : SWEPT_COUNTER_BIN;
        try {
            retryer.call(() -> {
                final List<Operation> operations = new ArrayList<>();
                shardPointers.forEach((shardId, pointer) -> operations.add(
                        MapOperation.put(new MapPolicy(), sweepPointersBin,
                                new Value.StringValue(shardId), new Value.LongValue(pointer))));
                operations.add(Operation.put(new Bin(sweptCounterBin, sweptCounter)));
                client.operate(createWritePolicy(DO_NOT_UPDATE_TTL),
                        new Key(namespace, setName, queueName),
                        operations.toArray(new Operation[0]));
                return true;
            });
        } catch (Exception e) {
            log.error("Error updating sweep progress in AS", e);
            throw IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, e);
        }
    }

    private QueueEntity buildQueueEntity(final Record record) {
        final int maxBatchSize = record.getInt(MAX_BATCH_SIZE);
        final int maxWaitTime = record.getInt(MAX_WAIT_TIME);
        BatchingConfig batchingConfig = null;
        if (maxBatchSize > 0 && maxWaitTime > 0) {
            batchingConfig = BatchingConfig.builder()
                    .maxBatchSize(maxBatchSize)
                    .maxWaitTimeInSecs(maxWaitTime)
                    .build();
        }

        return QueueEntity.builder()
                .active(Boolean.parseBoolean(record.getString(ACTIVE_BIN)))
                .messageExpiry(record.getInt(MESSAGE_EXPIRY_BIN))
                .queueExpiry(record.getInt(QUEUE_EXPIRY_BIN))
                .concurrency(record.getInt(CONCURRENCY_BIN))
                .messageHandlerType(record.getString(MESSAGE_HANDLER_TYPE_BIN))
                .shovelConcurrency(record.getInt(SHOVEL_CONCURRENCY_BIN))
                .shovelTimeIntervalInSecs(record.getInt(SHOVEL_TIME_INTERVAL_IN_SECS_BIN))
                .createdAt(record.getLong(CREATED_AT_BIN))
                .shards(record.getInt(SHARDS_BIN))
                .sweepPointers((Map<String, Long>) record.getMap(SWEEP_POINTERS_BIN))
                .sidelineSweepPointers((Map<String, Long>) record.getMap(SIDELINE_SWEEP_POINTERS_BIN))
                .sweptCounter(record.getLong(SWEPT_COUNTER_BIN))
                .sidelineSweptCounter(record.getLong(SIDELINE_SWEPT_COUNTER_BIN))
                .sweepDuration(record.getLong(SWEEP_DURATION_BIN))
                .handlerTimeout(record.getLong(HANDLER_TIMEOUT_BIN))
                .batchingConfig(batchingConfig)
                .build();
    }

    private WritePolicy createWritePolicy(final int ttl) {
        final WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.expiration = ttl;
        writePolicy.sendKey = true;
        return writePolicy;
    }

    private void createIndex(final String indexName, final String bin, final IndexType indexType) {
        try {
            client.createIndex(null, namespace, setName, indexName, bin, indexType).waitTillComplete();
        } catch (AerospikeException e) {
            if (e.getResultCode() == 200) {
                log.info("Ignoring index creation. Index seems to be already present");
                return;
            }
            throw e;
        }
    }

    private String getSetName(final String farmId, final String clientId) {
        return String.format(SET_FORMAT, farmId, clientId);
    }
}
