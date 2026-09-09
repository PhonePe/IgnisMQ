# Configuration Reference

Complete reference for every configurable parameter in IgnisMQ.

---

## IgnisMQManager Constructor Parameters

The `IgnisMQManager` is the primary entry point. All parameters are required.

| Parameter          | Type               | Description                                                                                                        |
|--------------------|--------------------|--------------------------------------------------------------------------------------------------------------------|
| `clientId`         | `String`           | Unique identifier for this application. Used in Aerospike set names (`{farmId}_{clientId}_ignis_queues`), ZooKeeper paths (`/{clientId}-ignis-workers/`), and metric names. |
| `storage`          | `BaseStorage`      | Storage backend implementation. Currently only `AerospikeStorage` is supported.                                    |
| `mapper`           | `ObjectMapper`     | Jackson `ObjectMapper` used for message serialization and deserialization.                                          |
| `meterRegistry`    | `MeterRegistry`    | Micrometer registry for publishing ignisMQ and Magazine timers and counters.                                       |
| `curatorFramework` | `CuratorFramework` | Apache Curator client for ZooKeeper-based leader election.                                                         |
| `farmId`           | `String`           | Deployment or datacenter identifier. Used as a prefix in Aerospike set names and metric names.                     |

---

## CreateQueueRequest

Defines all properties of a queue at creation time.

| Field                | Type             | Default              | Constraints                  | Description                                                                                          |
|----------------------|------------------|----------------------|------------------------------|------------------------------------------------------------------------------------------------------|
| `name`               | `String`         | *(required)*         | `@NotBlank`                  | Unique queue name.                                                                                   |
| `shards`             | `Integer`        | `32`                 | `@Min(1)` `@Max(512)`       | Number of shards the queue is partitioned into.                                                      |
| `messageExpiry`      | `TimeToLive`     | 2 days               | Must convert to &le; 31,536,000 s | How long an individual message is retained before expiring.                                           |
| `queueExpiry`        | `TimeToLive`     | 2 days               | Must be &ge; `messageExpiry` | How long queue metadata is retained. Must be greater than or equal to `messageExpiry`.               |
| `concurrency`        | `int`            | *(required)*         | `@Min(1)` `@Max(100)`       | Number of concurrent consumers per shard.                                                            |
| `messageHandlerType` | `String`         | *(required)*         | `@NotNull`                   | Identifier used to look up the registered `MessageHandler` for this queue.                           |
| `shovelConfig`       | `ShovelConfig`   | `null` *(disabled)*  | —                            | If set, enables the shovelling process for retrying failed messages. See [ShovelConfig](#shovelconfig). |
| `sweepDurationInMins`| `int`            | `20`                 | `@Min(5)` `@Max(300)`       | Interval in minutes between sweep cycles that reclaim stuck messages.                                |
| `batchingConfig`     | `BatchingConfig` | `null` *(single-message mode)* | —                    | If set, enables batched delivery. See [BatchingConfig](#batchingconfig).                             |

### Validation Rules

- `queueExpiry` &ge; `messageExpiry` — the queue must live at least as long as its messages.
- Both `messageExpiry` and `queueExpiry` must convert to &le; **31,536,000 seconds** (1 year) via `toSeconds()`.

---

## ShovelConfig

Controls the background shovelling process that retries side-lined messages.

| Field                | Type  | Default | Constraints          | Description                                                    |
|----------------------|-------|---------|----------------------|----------------------------------------------------------------|
| `timeIntervalInSecs` | `int` | `600`   | `@Max(86400)`        | Interval in seconds between shovel runs.                       |
| `concurrency`        | `int` | `4`     | `@Min(1)` `@Max(50)` | Number of concurrent threads used during shovelling.           |

---

## BatchingConfig

Enables batched message delivery to consumers instead of one-at-a-time processing.

| Field               | Type  | Default | Constraints            | Description                                                          |
|---------------------|-------|---------|------------------------|----------------------------------------------------------------------|
| `maxBatchSize`      | `int` | `2`     | `@Min(2)` `@Max(200000)` | Maximum number of messages collected into a single batch.            |
| `maxWaitTimeInSecs` | `int` | `1`     | `@Min(1)` `@Max(300)`    | Maximum seconds to wait for a batch to fill before delivering as-is. |

---

## TimeToLive

Represents a duration used for message and queue expiry.

| Field      | Type       | Constraints | Description                                      |
|------------|------------|-------------|--------------------------------------------------|
| `timeUnit` | `TimeUnit` | —           | One of `MINUTE`, `HOUR`, or `DAY`.               |
| `duration` | `int`      | `@Min(1)`   | Number of time units.                            |

The maximum allowed value after conversion is **31,536,000 seconds** (1 year).

**Examples:**

```java
// 2 days
new TimeToLive(TimeUnit.DAY, 2);

// 6 hours
new TimeToLive(TimeUnit.HOUR, 6);

// 30 minutes
new TimeToLive(TimeUnit.MINUTE, 30);
```

---

## AerospikeStorage

The only storage backend currently supported.

| Parameter       | Type                       | Description                                              |
|-----------------|----------------------------|----------------------------------------------------------|
| `configuration` | `AerospikeConfiguration`   | Connection and client settings (from `aerospike-bundle`). |
| `namespace`     | `String`                   | Aerospike namespace where all queue data is stored.      |

---

## AerospikeConfiguration

Provided by `aerospike-bundle`. Common fields:

| Field                    | Type       | Description                                                 |
|--------------------------|------------|-------------------------------------------------------------|
| `hosts`                  | `String`   | Comma-separated list of Aerospike seed hosts.               |
| `retries`                | `int`      | Number of retry attempts for failed operations.             |
| `sleepBetweenRetries`    | `int`      | Milliseconds to wait between retries.                       |
| `socketTimeout`          | `int`      | Per-socket timeout in milliseconds.                         |
| `totalTimeout`           | `int`      | Total timeout for an operation including all retries (ms).  |
| `maxConnectionsPerNode`  | `int`      | Maximum connection pool size per Aerospike node.            |
| `threadPoolSize`         | `int`      | Size of the client-side thread pool for async operations.   |

---

## System Constants (Non-Configurable)

These values are defined in `Constants.java` and cannot be changed at runtime.

| Constant                      | Value       | Description                                                       |
|-------------------------------|-------------|-------------------------------------------------------------------|
| `INITIAL_DELAY_IN_MS`         | `120000`    | Delay (ms) before the first consumer poll cycle starts.           |
| `DELAY_PERIOD_IN_MS`          | `1000`      | Interval (ms) between successive consumer poll cycles.            |
| `WATCHER_INITIAL_DELAY_IN_MS` | `60000`     | Delay (ms) before the watcher thread begins its first run.        |
| `REFRESH_INTERVAL_IN_MS`      | `300000`    | Interval (ms) between queue metadata refresh cycles.              |
| `SHOVEL_DELAY_IN_MS`          | `10000`     | Initial delay (ms) before the first shovel cycle.                 |
| `MAX_CONSUMERS_ALLOWED`       | `100`       | Hard cap on the number of concurrent consumers per queue.         |
| `TTL_FACTOR_FOR_QUEUE_EXPIRY` | `2`         | Internal multiplier applied to queue expiry for safety margin.    |
| `PARALLEL_FACTOR`             | `64`        | Parallelism level used for internal concurrent operations.        |

---

## Configuration Examples

### Production Configuration

A typical production setup with shovelling and batching enabled:

```java
ObjectMapper mapper = new ObjectMapper();
MeterRegistry meterRegistry = new SimpleMeterRegistry();
CuratorFramework curator = CuratorFrameworkFactory.newClient(
    "zk1.prod:2181,zk2.prod:2181,zk3.prod:2181",
    new RetryNTimes(3, 1000)
);
curator.start();

AerospikeConfiguration aerospikeConfig = new AerospikeConfiguration();
aerospikeConfig.setHosts("aero1.prod:3000,aero2.prod:3000");
aerospikeConfig.setMaxConnectionsPerNode(300);
aerospikeConfig.setSocketTimeout(100);
aerospikeConfig.setTotalTimeout(1000);
aerospikeConfig.setRetries(2);

AerospikeStorage storage = new AerospikeStorage(aerospikeConfig, "ignismq");

IgnisMQManager manager = new IgnisMQManager(
    "order-service",   // clientId
    storage,
    mapper,
    meterRegistry,
    curator,
    "us-east-1"        // farmId
);

CreateQueueRequest request = CreateQueueRequest.builder()
    .name("order-events")
    .shards(64)
    .messageExpiry(new TimeToLive(TimeUnit.DAY, 7))
    .queueExpiry(new TimeToLive(TimeUnit.DAY, 14))
    .concurrency(16)
    .messageHandlerType("orderEventHandler")
    .sweepDurationInMins(30)
    .shovelConfig(ShovelConfig.builder()
        .timeIntervalInSecs(300)
        .concurrency(8)
        .build())
    .batchingConfig(BatchingConfig.builder()
        .maxBatchSize(50)
        .maxWaitTimeInSecs(5)
        .build())
    .build();

manager.createQueue(request);
```

### Minimal Configuration

The simplest possible setup using all defaults:

```java
IgnisMQManager manager = new IgnisMQManager(
    "my-app",
    storage,
    new ObjectMapper(),
    new SimpleMeterRegistry(),
    curator,
    "local"
);

CreateQueueRequest request = CreateQueueRequest.builder()
    .name("notifications")
    .concurrency(4)
    .messageHandlerType("notificationHandler")
    .build();
// Uses defaults: 32 shards, 2-day expiry, no shovel, no batching, 20-min sweep

manager.createQueue(request);
```

!!! warning "Expiry Constraint"
    `queueExpiry` must always be greater than or equal to `messageExpiry`. Violating this will cause a validation error at queue creation time.

!!! tip "Choosing Shard Count"
    The number of shards determines the maximum parallelism across your cluster. A good starting point is `number_of_instances * concurrency`. You can go higher but cannot change it after queue creation.

!!! note "Batching Trade-offs"
    Batching increases throughput but adds latency equal to `maxWaitTimeInSecs` in the worst case. Use it when your handler benefits from processing multiple messages together (e.g., bulk database writes).
