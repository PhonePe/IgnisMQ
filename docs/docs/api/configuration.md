# Configuration Reference

Complete reference for every configurable parameter in IgnisMQ.

---

## IgnisMQManager Constructor Parameters

The `IgnisMQManager` is the primary entry point. Every parameter except `settings` is required.

| Parameter          | Type               | Description                                                                                                        |
|--------------------|--------------------|--------------------------------------------------------------------------------------------------------------------|
| `clientId`         | `String`           | Unique identifier for this application. Used in Aerospike set names (`{farmId}_{clientId}_ignis_queues`) and ZooKeeper paths (`/{clientId}-ignis-workers/`). Not used in metric names. |
| `storage`          | `BaseStorage`      | Storage backend implementation. Currently only `AerospikeStorage` is supported.                                    |
| `mapper`           | `ObjectMapper`     | Jackson `ObjectMapper` used for message serialization and deserialization.                                          |
| `meterRegistry`    | `MeterRegistry`    | Micrometer registry for publishing ignisMQ and Magazine timers and counters.                                       |
| `curatorFramework` | `CuratorFramework` | Apache Curator client for ZooKeeper-based leader election.                                                         |
| `farmId`           | `String`           | Deployment or datacenter identifier. Used as a prefix in Aerospike set names. Not used in metric names - meters are tagged, and neither identifier is a tag. |
| `settings`         | `IgnisMQSettings`  | Worker-pool size and the metrics switch. **Optional** — `null` means `IgnisMQSettings.defaults()`.                 |

An eight-parameter form additionally takes a `StorageClient` before `curatorFramework`, for callers who want to share one database connection. The caller keeps ownership of a client passed this way, so `stop()` leaves it open.

---

## CreateQueueRequest

Defines all properties of a queue at creation time.

| Field                | Type             | Default              | Constraints                  | Description                                                                                          |
|----------------------|------------------|----------------------|------------------------------|------------------------------------------------------------------------------------------------------|
| `name`               | `String`         | *(required)*         | `@NotBlank`                  | Unique queue name.                                                                                   |
| `shards`             | `Integer`        | `8`                  | `@Min(1)` `@Max(512)`       | Shards the queue is partitioned into. Fixed once the queue exists.                                   |
| `messageExpiry`      | `TimeToLive`     | 2 days               | Must convert to &le; 31,536,000 s | How long an individual message is retained before expiring.                                           |
| `queueExpiry`        | `TimeToLive`     | 2 days               | Must be &ge; `messageExpiry` | How long queue metadata is retained. Must be greater than or equal to `messageExpiry`.               |
| `concurrency`        | `int`            | *(required)*         | `@Min(1)` `@Max(100)`       | Consumers for this queue, in **this** process. A target share of `workerThreads`, not dedicated threads. |
| `messageHandlerType` | `String`         | *(required)*         | `@NotNull`                   | Identifier used to look up the registered `MessageHandler` for this queue.                           |
| `shovelConfig`       | `ShovelConfig`   | `null` *(disabled)*  | —                            | If set, enables the shovelling process for retrying failed messages. See [ShovelConfig](#shovelconfig). |
| `sweepDurationInMins`| `int`            | `20`                 | `@Min(5)` `@Max(300)`       | **Age threshold, not an interval.** A message claimed longer ago than this is treated as abandoned. The sweep itself runs every 15 minutes. Capped at 12 hours. |
| `handlerTimeoutInMins`| `int`           | `10`                 | `@Min(1)` `@Max(30)`        | How long a handler may run before its message is sidelined. Clamped to at most half of `sweepDuration`. |
| `batchingConfig`     | `BatchingConfig` | `null` *(single-message mode)* | —                    | If set, enables batched delivery. See [BatchingConfig](#batchingconfig).                             |

## IgnisMQSettings

Process-wide tunables, passed as the manager's last constructor argument. `null` means
`IgnisMQSettings.defaults()`.

| Field | Type | Default | Description |
|---|---|---|---|
| `workerThreads` | `int` | `256` | Ceiling on threads running consumers and shovels, and on those running message handlers. Size it as **concurrently active queues x their concurrency**, not as total configured consumers. |
| `metricsEnabled` | `boolean` | `true` | When false, every `ignismq.*` meter becomes a no-op — and so do Magazine's `magazine.*` meters. |

```java
IgnisMQSettings.builder()
    .workerThreads(128)
    .metricsEnabled(true)
    .build();
```

!!! warning "`concurrency` changed meaning in 2.0"
    It used to mean one dedicated `Timer` thread per consumer. It now means a scheduled task
    competing for the shared worker pool. See the [upgrade notes](../upgrading.md).

---

### Validation Rules

- `queueExpiry` &ge; `messageExpiry` — the queue must live at least as long as its messages.
- Both `messageExpiry` and `queueExpiry` must convert to &le; **31,536,000 seconds** (1 year) via `toSeconds()`.

---

## ShovelConfig

Controls the background shovelling process that retries side-lined messages.

| Field                | Type  | Default | Constraints          | Description                                                    |
|----------------------|-------|---------|----------------------|----------------------------------------------------------------|
| `timeIntervalInSecs` | `int` | `600`   | `@Max(86400)`        | Interval in seconds between shovel runs.                       |
| `concurrency`        | `int` | `4`     | `@Min(1)` `@Max(50)` | Number of concurrent shovel tasks on the shared worker pool.   |

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
| `hosts`                  | `List<AerospikeHost>` | Aerospike seed nodes. Each `AerospikeHost` carries a `host` and a `port`. |
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
| `INITIAL_DELAY_IN_MS`         | `1000`      | Delay (ms) before the first consumer poll cycle starts.           |
| `DELAY_PERIOD_IN_MS`          | `1000`      | Interval (ms) between successive consumer poll cycles.            |
| `WATCHER_INITIAL_DELAY_IN_MS` | `60000`     | Delay (ms) before the watcher thread begins its first run.        |
| `REFRESH_INTERVAL_IN_MS`      | `300000`    | Interval (ms) between queue metadata refresh cycles.              |
| `SHOVEL_DELAY_IN_MS`          | `10000`     | Delay (ms) before retrying a one-shot shovel that failed. Not the first-run delay, which is `INITIAL_DELAY_IN_MS`. |
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

AerospikeConfiguration aerospikeConfig = AerospikeConfiguration.builder()
    .hosts(List.of(
        AerospikeHost.builder().host("aero1.prod").port(3000).build(),
        AerospikeHost.builder().host("aero2.prod").port(3000).build()))
    .maxConnectionsPerNode(300)
    .socketTimeout(100)
    .totalTimeout(1000)
    .retries(2)
    .build();

AerospikeStorage storage = new AerospikeStorage(aerospikeConfig, "ignismq");

IgnisMQManager manager = new IgnisMQManager(
    "order-service",   // clientId
    storage,
    mapper,
    meterRegistry,
    curator,
    "us-east-1",       // farmId
    IgnisMQSettings.builder().workerThreads(128).build()
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
    "local",
    null               // null means IgnisMQSettings.defaults()
);

CreateQueueRequest request = CreateQueueRequest.builder()
    .name("notifications")
    .concurrency(4)
    .messageHandlerType("notificationHandler")
    .build();
// Uses defaults: 8 shards, 2-day expiry, no shovel, no batching, 20-min sweep,
// 10-min handler timeout

manager.createQueue(request);
```

!!! warning "Expiry Constraint"
    `queueExpiry` must always be greater than or equal to `messageExpiry`. Violating this will cause a validation error at queue creation time.

!!! tip "Choosing Shard Count"
    The number of shards determines the maximum parallelism across your cluster. A good starting point is `number_of_instances * concurrency`. You can go higher but cannot change it after queue creation.

!!! note "Batching Trade-offs"
    Batching increases throughput but adds latency equal to `maxWaitTimeInSecs` in the worst case. Use it when your handler benefits from processing multiple messages together (e.g., bulk database writes).
