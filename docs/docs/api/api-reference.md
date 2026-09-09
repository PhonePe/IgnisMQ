# API Reference

Complete reference for all public classes, interfaces, methods, and fields in IgnisMQ.

---

## IQueue

The primary interface for interacting with a queue. Generic parameter `M` is the message type.

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `publish` | `boolean publish(M message) throws JsonProcessingException` | Serializes the message to JSON and loads it into the Magazine. Returns `true` on success. |
| `getUnconsumedCount` | `long getUnconsumedCount()` | Returns `publishedCount - consumedCount`, floored at `0`. |
| `getMetaData` | `QueueMetaData getMetaData()` | Returns a snapshot of published, consumed, sidelined, and shovelled counts. |
| `shovel` | `void shovel(int concurrency)` | Triggers a one-shot shovel operation with auto-delete enabled. |

---

## IgnisMQManager

```java
public final class IgnisMQManager
```

Central manager for all queue lifecycle operations. Framework integrations adapt its `start()` and
`stop()` methods to their own lifecycle APIs; `ignismq-dw-bundle` does this with a Dropwizard
`Managed`.

`stop()` cancels the queue-refresh watcher and, **only when the manager created the storage client
itself**, closes that client. If you passed your own `StorageClient` to the constructor, you retain
ownership of it and are responsible for closing it. `stop()` is safe to call more than once, and
safe to call when `start()` never ran.

### Constructors

```java
public IgnisMQManager(
    String clientId,
    BaseStorage storage,
    ObjectMapper mapper,
    MeterRegistry meterRegistry,
    CuratorFramework curatorFramework,
    String farmId
) throws Exception
```

Creates the manager without an externally provided `StorageClient`. The client is constructed internally from the storage configuration.

```java
public IgnisMQManager(
    String clientId,
    BaseStorage storage,
    ObjectMapper mapper,
    MeterRegistry meterRegistry,
    StorageClient storageClient,
    CuratorFramework curatorFramework,
    String farmId
) throws Exception
```

Creates the manager with an externally managed `StorageClient`, allowing the caller to share or reuse the underlying database connection.

### Methods

#### initialiseMessageHandlers

```java
void initialiseMessageHandlers(Map<String, Map.Entry<Class, MessageHandler>> messageHandlers)
```

Registers all message handler mappings. **Must be called exactly once** before any call to `createQueue`. Subsequent calls will fail. Internally calls `refreshQueues()` to sync state from the database. Framework integrations may expose `getQueueStats()` through their metrics system.

> [!WARNING]
> Calling this method more than once will throw an exception. Ensure all handlers are collected into a single map before invoking.

---

#### getQueue

```java
<M> IQueue<M> getQueue(String name)
```

Returns the in-memory `IQueue` instance for the given queue name.

**Throws:** `IgnisMQException` with `ErrorCode.QUEUE_NOT_FOUND` if no queue with that name exists.

---

#### getAllQueues

```java
Map<String, IQueue<?>> getAllQueues()
```

Returns the full in-memory map of queue name to `IQueue` instance.

---

#### getAllQueuesFromDB

```java
Set<String> getAllQueuesFromDB()
```

Queries Aerospike and returns the set of all active queue names persisted in the database.

---

#### createQueue

```java
void createQueue(CreateQueueRequest queueRequest) throws Exception
```

Validates the request, creates the underlying Magazine structure, and persists a `QueueEntity` to the database. Starts consumers and schedules sweeps/shovels as configured.

**Throws:** `IgnisMQException` with `ErrorCode.QUEUE_ALREADY_EXISTS` if a queue with the same name is already active.

---

#### deactivateQueue

```java
void deactivateQueue(String queueName)
```

Stops all consumers for the queue, removes it from the in-memory map, and sets `active=false` in the database.

> [!CAUTION]
> This operation is **irreversible**. The queue cannot be reactivated after deactivation.

---

#### increaseConsumers

```java
void increaseConsumers(String queueName, int count)
```

Adds `count` new consumer timers for the specified queue and updates the concurrency value in the database.

---

#### decreaseConsumers

```java
void decreaseConsumers(String queueName, int count)
```

Stops `count` consumer timers for the specified queue and updates the concurrency value in the database.

---

#### scheduleShoveling

```java
void scheduleShoveling(String queueName, ShovelConfig shovelConfig)
```

Schedules periodic shoveling for the queue using the interval and concurrency defined in `shovelConfig`.

---

#### sweepQueue

```java
void sweepQueue(String queueName)
```

Manually triggers a sweep operation for the specified queue.

---

#### refreshQueues

```java
void refreshQueues()
```

Synchronizes the in-memory queue map with the persisted state in the database. Creates or removes queue instances as needed.

---

#### getTaskInitializer

```java
TaskInitializer getTaskInitializer()
```

Returns the internal `TaskInitializer` instance used for scheduling consumer and sweep tasks.

---

## MessageHandler<M\>

```java
public interface MessageHandler<M>
```

User-implemented interface for processing messages consumed from a queue.

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `handle` (single) | `boolean handle(M message) throws Exception` | Processes a single message. Return `true` to mark as consumed, `false` to sideline the message. |
| `handle` (batch) | `boolean handle(List<M> messages) throws Exception` | Processes a batch of messages. Return `true` to mark all as consumed, `false` to sideline **all** messages in the batch. |
| `getIgnorableExceptions` | `Set<Class<?>> getIgnorableExceptions()` | Returns exception types that should **not** cause sidelining. When a thrown exception matches one of these types, the message is deleted instead of sidelined. |

---

## CreateQueueRequest

```java
@Data
public class CreateQueueRequest
```

Request object for creating a new queue via `IgnisMQManager.createQueue()`.

### Fields

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `name` | `String` | — | `@NotBlank` | Unique queue name. |
| `shards` | `Integer` | `32` | `@Min(1) @Max(512)` | Number of shards for the underlying Magazine. |
| `messageExpiry` | `TimeToLive` | 2 days | Must be valid | TTL for individual messages. |
| `queueExpiry` | `TimeToLive` | 2 days | Must be valid | TTL for the queue itself. |
| `concurrency` | `int` | — | `@Min(1) @Max(100)` | Number of consumer threads. |
| `messageHandlerType` | `String` | — | `@NotNull` | Key into the handler map registered via `initialiseMessageHandlers`. |
| `shovelConfig` | `ShovelConfig` | `null` | `@Valid`, optional | If provided, enables periodic shoveling. |
| `sweepDurationInMins` | `int` | `20` | `@Min(5) @Max(300)` | Interval in minutes between sweep runs. |
| `batchingConfig` | `BatchingConfig` | `null` | `@Valid`, optional | If `null`, single-message mode is used. If set, enables batch consumption. |

> [!IMPORTANT]
> Validation requires `queueExpiry >= messageExpiry`. Both `queueExpiry` and `messageExpiry` must resolve to valid durations within the allowed maximum.

---

## ShovelConfig

```java
@Data @Builder @AllArgsConstructor
public class ShovelConfig
```

Configuration for periodic shoveling of sidelined messages back into the queue.

### Fields

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `timeIntervalInSecs` | `int` | `600` (10 min) | `@Max(86400)` | Interval in seconds between shovel runs. |
| `concurrency` | `int` | `4` | `@Min(1) @Max(50)` | Number of parallel shovel threads. |

---

## BatchingConfig

```java
@Data @Builder @AllArgsConstructor
public class BatchingConfig
```

Configuration for batch message consumption.

### Fields

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `maxBatchSize` | `int` | `2` | `@Min(2) @Max(200000)` | Maximum number of messages per batch. |
| `maxWaitTimeInSecs` | `int` | `1` | `@Min(1) @Max(300)` | Maximum time in seconds to wait for a full batch before dispatching. |

---

## TimeToLive

```java
@Getter
public class TimeToLive
```

Represents a duration with a unit, used for message and queue expiry.

### Fields

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `timeUnit` | `TimeUnit` | `@NotNull` | The unit of time. |
| `duration` | `int` | `@Min(1)` | The numeric duration value. |

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `toSeconds` | `int toSeconds()` | Converts the duration to seconds based on `timeUnit`. |

> [!NOTE]
> Maximum allowed value after conversion is **31,536,000 seconds** (1 year).

---

## TimeUnit

```java
public enum TimeUnit {
    MINUTE,
    HOUR,
    DAY
}
```

| Value | Conversion |
|-------|------------|
| `MINUTE` | `duration * 60` |
| `HOUR` | `duration * 3600` |
| `DAY` | `duration * 86400` |

---

## QueueMetaData

```java
@Data @Builder @AllArgsConstructor @NoArgsConstructor
public class QueueMetaData
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `published` | `long` | Total messages published. |
| `consumed` | `long` | Total messages consumed. |
| `sidelined` | `long` | Total messages sidelined. |
| `shovelled` | `long` | Total messages shovelled back into the queue. |

---

## QueueEntity

```java
@Data @Builder @AllArgsConstructor @NoArgsConstructor
public class QueueEntity
```

Persistent representation of a queue stored in Aerospike.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `messageHandlerType` | `String` | Key for the registered message handler. |
| `shards` | `int` | Number of shards. |
| `queueExpiry` | `TimeToLive` | Queue-level TTL. |
| `messageExpiry` | `TimeToLive` | Message-level TTL. |
| `concurrency` | `int` | Number of consumer threads. |
| `shovelConcurrency` | `int` | Number of shovel threads. |
| `shovelTimeIntervalInSecs` | `int` | Shovel interval in seconds. |
| `createdAt` | `long` | Creation timestamp (epoch millis). |
| `active` | `boolean` | Whether the queue is active. |
| `sweepPointers` | `Map` | Current sweep cursor positions per shard. |
| `sidelineSweepPointers` | `Map` | Current sideline sweep cursor positions per shard. |
| `sweptCounter` | `long` | Total messages swept. |
| `sidelineSweptCounter` | `long` | Total sidelined messages swept. |
| `sweepDuration` | `int` | Sweep interval in minutes. |
| `batchingConfig` | `BatchingConfig` | Batching configuration, or `null` for single-message mode. |

---

## BaseStorage

```java
public abstract sealed class BaseStorage permits AerospikeStorage
```

Abstract base for storage backends.

### Fields

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `storageType` | `StorageType` | `@NotNull` | The type of storage backend. |

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `accept` | `abstract <T> T accept(StorageVisitor<T> visitor)` | Visitor pattern dispatch for storage-specific operations. |

---

## AerospikeStorage

```java
public final class AerospikeStorage extends BaseStorage
```

Aerospike-backed storage implementation.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `configuration` | `AerospikeConfiguration` | Aerospike cluster configuration. |
| `namespace` | `String` | Aerospike namespace to use. |

### Constructor

```java
public AerospikeStorage(AerospikeConfiguration configuration, String namespace)
```

---

## StorageClient\<T\>

```java
public interface StorageClient<T>
```

Generic interface for obtaining a database client handle.

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `getClient` | `T getClient()` | Returns the underlying client instance. |
| `stop` | `default void stop()` | Releases the client's resources. Idempotent, and a no-op by default so that externally owned clients are torn down by their owner. |

---

## AerospikeStoreClient

```java
public final class AerospikeStoreClient implements StorageClient<IAerospikeClient>
```

Aerospike client wrapper that auto-starts on construction.

### Constructor

```java
public AerospikeStoreClient(AerospikeConfiguration aerospikeConfiguration)
```

> [!NOTE]
> The client starts automatically on construction. Write policy is configured with `COMMIT_ALL` commit level and `MASTER_PROLES` replica policy.

---

## QueueService

```java
public sealed interface QueueService permits AerospikeQueueService
```

Internal service interface for queue persistence operations.

### Methods

| Method | Description |
|--------|-------------|
| `exists` | Checks if a queue exists in storage. |
| `get` | Retrieves a `QueueEntity` by name. |
| `store` | Persists a new `QueueEntity`. |
| `updateState` | Updates the active state of a queue. |
| `updateConcurrency` | Updates consumer concurrency in storage. |
| `updateShovelConfig` | Updates shovel configuration in storage. |
| `getQueues` | Returns all queue entities from storage. |
| `addFireTimestamp` | Records a fire timestamp for audit/tracking. |
| `sweep` | Executes a sweep operation in storage. |

---

## IgnisMQBundle\<T\>

```java
public abstract class IgnisMQBundle<T extends Configuration> implements ConfiguredBundle<T>
```

Dropwizard bundle for integrating IgnisMQ into a Dropwizard application.

### Abstract Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `getStorage` | `BaseStorage getStorage(T config)` | Provide the storage backend from app config. |
| `getClientId` | `String getClientId(T config)` | Provide the client identifier from app config. |
| `getFarmId` | `String getFarmId(T config)` | Provide the farm/cluster identifier from app config. |
| `getCuratorFramework` | `CuratorFramework getCuratorFramework()` | Provide the ZooKeeper CuratorFramework instance. |

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `getIgnisMQManager` | `IgnisMQManager getIgnisMQManager()` | Returns the initialized `IgnisMQManager` instance. |

---

## IgnisMQException

```java
public class IgnisMQException extends RuntimeException
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `errorCode` | `ErrorCode` | The specific error code for this exception. |

### Static Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `propagate` | `static IgnisMQException propagate(Throwable t)` | Wraps a throwable as an `IgnisMQException`. |
| `propagate` | `static IgnisMQException propagate(String message, Throwable t)` | Wraps with a custom message. |
| `propagate` | `static IgnisMQException propagate(ErrorCode errorCode, Throwable t)` | Wraps with a specific error code. |

---

## ErrorCode

```java
public enum ErrorCode
```

| Value | Description |
|-------|-------------|
| `QUEUE_ALREADY_EXISTS` | A queue with the given name is already active. |
| `QUEUE_NOT_FOUND` | No queue exists with the given name. |
| `NOT_IMPLEMENTED` | Operation is not implemented for the current storage backend. |
| `AEROSPIKE_ERROR` | An error occurred in the Aerospike layer. |
| `INVALID_REQUEST` | The request failed validation. |
| `MAX_ALLOWED_CONSUMERS_EXCEEDED` | Consumer count exceeds the maximum of 100. |
| `INVALID_SHOVEL_TIME_INTERVAL` | Shovel interval exceeds 86,400 seconds. |
| `INVALID_MESSAGE_HANDLER` | The specified message handler type is not registered. |
| `INTERNAL_ERROR` | An unexpected internal error occurred. |

---

## QueueStat

```java
@Data @Builder
public class QueueStat
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | `String` | Queue name. |
| `active` | `boolean` | Whether the queue is active. |
| `published` | `long` | Total published count. |
| `consumed` | `long` | Total consumed count. |
| `unConsumed` | `long` | Unconsumed count (`published - consumed`). |
| `sidelined` | `long` | Total sidelined count. |
| `shovelled` | `long` | Total shovelled count. |

---

## Constants

All constants defined in `Constants.java`:

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_SHARDS` | `32` | Default number of shards per queue. |
| `INITIAL_DELAY_IN_MS` | `120000` | Initial delay before consumers start (2 minutes). |
| `WATCHER_INITIAL_DELAY_IN_MS` | `60000` | Initial delay before the watcher starts (1 minute). |
| `DELAY_PERIOD_IN_MS` | `1000` | Consumer polling interval (1 second). |
| `SHOVEL_DELAY_IN_MS` | `10000` | Delay before first shovel run (10 seconds). |
| `DEFAULT_DURATION_DAY` | `2` | Default TTL duration in days. |
| `MAX_ALLOWED_SHOVEL_TIME_INTERVAL_IN_SECONDS` | `86400` | Maximum shovel interval (24 hours). |
| `MAX_DURATION_ALLOWED_IN_SECONDS` | `31536000` | Maximum TTL duration (1 year). |
| `REFRESH_INTERVAL_IN_MS` | `300000` | Queue refresh interval (5 minutes). |
| `MAX_CONSUMERS_ALLOWED` | `100` | Maximum consumer threads per queue. |
| `TTL_FACTOR_FOR_QUEUE_EXPIRY` | `2` | Multiplier applied to queue expiry for internal storage TTL. |
| `PARALLEL_FACTOR` | `64` | Parallelism factor for internal operations. |
