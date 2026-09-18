# Usage Guide

This guide covers every major use case for IgnisMQ with complete, working code examples.

---

## 1. Dropwizard Integration

The recommended way to use IgnisMQ in a Dropwizard application is via the `IgnisMQBundle`.

### Configuration Class

```java
@Data
public class MyAppConfiguration extends Configuration {
    @Valid
    @NotNull
    private AerospikeConfiguration aerospikeConfig;

    @NotBlank
    private String clientId;

    @NotBlank
    private String farmId;

    @NotBlank
    private String zookeeperConnectionString;
}
```

### Bundle Implementation

Subclass `IgnisMQBundle` and implement its single abstract method, which returns everything the bundle needs:

```java
public class MyIgnisMQBundle extends IgnisMQBundle<MyAppConfiguration> {

    @Getter
    private CuratorFramework curatorFramework;

    @Override
    protected IgnisMQContext context(MyAppConfiguration config) {
        // Built here, not in Application.run: a ConfiguredBundle's run() executes before the
        // application's, so anything handed in afterwards is still null at this point.
        this.curatorFramework = CuratorFrameworkFactory.newClient(
                config.getZookeeperConnectionString(), new ExponentialBackoffRetry(1000, 3));
        this.curatorFramework.start();

        return IgnisMQContext.builder()
                .clientId(config.getClientId())
                .farmId(config.getFarmId())
                .storage(new AerospikeStorage(config.getAerospikeConfig(), "my-namespace"))
                .curatorFramework(curatorFramework)
                .build();
    }
}
```

!!! danger "Build the CuratorFramework inside `context(config)`"
    Dropwizard runs every `ConfiguredBundle.run()` **before** `Application.run()`. `context(config)` is
    called from the bundle's `run()`, and `curatorFramework` is `@NonNull`, so a client injected from
    `Application.run()` arrives too late and startup fails with a `NullPointerException`.

| Field | Type | Purpose |
|-------|------|---------|
| `storage` | `BaseStorage` | The Aerospike storage backend. Required |
| `clientId` | `String` | Unique identifier for this application/service. Required |
| `farmId` | `String` | Identifier for the deployment region/datacenter. Required |
| `curatorFramework` | `CuratorFramework` | ZooKeeper client for leader election. Required |
| `settings` | `IgnisMQSettings` | Worker-pool size and the metrics switch. Optional; defaults to `IgnisMQSettings.defaults()` |

!!! note "One method, not five"
    The four required fields are `@NonNull` on the builder, so omitting one throws at startup rather
    than failing to compile. That is the trade a single extension point makes: a new input becomes a
    new field here instead of a new abstract method every existing subclass has to implement.

### Application Class

```java
public class MyApplication extends Application<MyAppConfiguration> {

    private final MyIgnisMQBundle ignisMQBundle = new MyIgnisMQBundle();

    @Override
    public void initialize(Bootstrap<MyAppConfiguration> bootstrap) {
        bootstrap.addBundle(ignisMQBundle);
    }

    @Override
    public void run(MyAppConfiguration config, Environment environment) throws Exception {
        // The bundle has already run by this point, so the manager exists and ZooKeeper is connected.
        IgnisMQManager manager = ignisMQBundle.getIgnisMQManager();

        // Register all message handlers BEFORE creating queues
        manager.initialiseMessageHandlers(Map.of(
                "order-handler", Map.entry(OrderEvent.class, new OrderEventHandler()),
                "notification-handler", Map.entry(Notification.class, new NotificationHandler())
        ));

        // Create queues
        manager.createQueue(CreateQueueRequest.builder()
                .name("order-events")
                .shards(32)
                .concurrency(8)
                .messageHandlerType("order-handler")
                .messageExpiry(new TimeToLive(TimeUnit.DAY, 2))
                .queueExpiry(new TimeToLive(TimeUnit.DAY, 7))
                .build());
    }
}
```

!!! warning "Handler registration order"
    You **must** call `initialiseMessageHandlers()` before `createQueue()`. The manager validates that the `messageHandlerType` references a registered handler. This method can only be called **once** — calling it again throws an exception.

---

## 2. Standalone Usage (Without Dropwizard)

If you're not using Dropwizard, create an `IgnisMQManager` directly.

=== "Manager-owned StorageClient"

    The manager builds the `StorageClient` internally from the provided `BaseStorage`.

    ```java
    // 1. Configure Aerospike storage
    AerospikeStorage storage = new AerospikeStorage(
            AerospikeConfiguration.builder()
                    .hosts(List.of(AerospikeHost.builder()
                            .host("localhost")
                            .port(3000)
                            .build()))
                    .retries(3)
                    .sleepBetweenRetries(100)
                    .socketTimeout(3000)
                    .totalTimeout(5000)
                    .maxConnectionsPerNode(100)
                    .threadPoolSize(4)
                    .build(),
            "my-namespace"
    );

    // 2. Set up ZooKeeper
    CuratorFramework curator = CuratorFrameworkFactory.newClient(
            "zk-host:2181",
            new ExponentialBackoffRetry(1000, 3)
    );
    curator.start();

    // 3. Create the manager
    IgnisMQManager manager = new IgnisMQManager(
            "my-service",           // clientId
            storage,                // BaseStorage
            new ObjectMapper(),     // Jackson ObjectMapper
            new SimpleMeterRegistry(), // Micrometer MeterRegistry
            curator,                // CuratorFramework
            "datacenter-1",         // farmId
            IgnisMQSettings.defaults()  // or null for the same
    );

    // 4. Register handlers and create queues
    manager.initialiseMessageHandlers(Map.of(
            "order-handler", Map.entry(OrderEvent.class, new OrderEventHandler())
    ));

    manager.createQueue(CreateQueueRequest.builder()
            .name("order-events")
            .concurrency(4)
            .messageHandlerType("order-handler")
            .build());
    ```

=== "With a pre-built StorageClient"

    Use this when you already have an Aerospike client and want to share it.

    ```java
    // Pre-build the StorageClient
    StorageClient storageClient = new AerospikeStoreClient(aerospikeConfiguration);

    IgnisMQManager manager = new IgnisMQManager(
            "my-service",           // clientId
            storage,                // BaseStorage
            new ObjectMapper(),     // Jackson ObjectMapper
            new SimpleMeterRegistry(), // Micrometer MeterRegistry
            storageClient,          // pre-built; stop() will not close it
            curator,                // CuratorFramework
            "datacenter-1",         // farmId
            IgnisMQSettings.defaults()  // or null for the same
    );
    ```

!!! note "Constructor side effects"
    The constructor builds the scheduler pools, the storage client (unless you supplied one) and the
    queue watcher, which refreshes queues every 5 minutes. Aerospike must be reachable.

    It does **not** start leader election or the sweeper: `getTaskInitializer().start()` does, and the
    Dropwizard bundle calls it for you on application start.

    Shutdown mirrors that, and **`stop()` alone is not enough**: it stops the scheduler pools and the
    storage client, but leader election and the sweeper belong to the `TaskInitializer`. Call
    `getTaskInitializer().stop()` first, then `stop()` - which is exactly what the bundle's `Managed`
    does. A `StorageClient` you supplied yourself is left open, since you own it.

---

## 3. Publishing Messages

Once you have a queue, publish messages using the `publish()` method on `IQueue`. Messages are serialized to JSON via Jackson's `ObjectMapper`.

### Basic Publishing

```java
IQueue<OrderEvent> queue = manager.getQueue("order-events");

// Publish a POJO
queue.publish(OrderEvent.builder()
        .orderId("ORD-12345")
        .amount(249.99)
        .status("CREATED")
        .build());
```

### Different Message Types

Any Jackson-serializable Java object works as a message:

=== "POJO"

    ```java
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public class OrderEvent {
        private String orderId;
        private double amount;
        private String status;
    }

    IQueue<OrderEvent> queue = manager.getQueue("order-events");
    queue.publish(new OrderEvent("ORD-001", 99.99, "CREATED"));
    ```

=== "String"

    ```java
    IQueue<String> queue = manager.getQueue("simple-queue");
    queue.publish("Hello, IgnisMQ!");
    queue.publish("{\"key\": \"raw-json-string\"}");
    ```

=== "Map"

    ```java
    IQueue<Map<String, Object>> queue = manager.getQueue("dynamic-queue");
    queue.publish(Map.of(
            "eventType", "USER_SIGNUP",
            "userId", "usr-42",
            "timestamp", System.currentTimeMillis()
    ));
    ```

!!! tip "Publish return value"
    `publish()` returns `true` if the message was successfully written to Magazine (Aerospike). It throws `JsonProcessingException` if serialization fails.

---

### Publishing now, consuming later

A queue created with `concurrency: 0` is **publish-only**: it accepts messages and starts nothing.
Consumption is turned on afterwards, without recreating the queue.

```java
manager.createQueue(CreateQueueRequest.builder()
        .name("order-events")
        .concurrency(0)                     // accepts publishes, consumes nothing
        .messageHandlerType("order-handler")
        .messageExpiry(new TimeToLive(TimeUnit.DAY, 2))
        .queueExpiry(new TimeToLive(TimeUnit.DAY, 7))
        .build());

manager.<OrderEvent>getQueue("order-events").publish(event);   // accumulates

manager.increaseConsumers("order-events", 4);   // consumption starts, here and everywhere
```

`increaseConsumers` **persists the new count**, and every other instance reconciles to it on its next
refresh pass - so this starts consumption across the deployment, not only in the process that made the
call. `decreaseConsumers(name, n)` is the reverse, and taking it back to zero pauses consumption
while publishes continue.

!!! warning "A paused queue still expires"
    Nothing about `concurrency: 0` suspends TTLs. Messages are deleted `messageExpiry` after they were
    published whether or not anything ever consumed them, and the sweeper still runs. Pausing for
    longer than `messageExpiry` loses messages.

!!! note "This is the only trigger that exists today"
    Starting consumption on a schedule, on a queue-depth threshold, or on an external event is not
    built in - `increaseConsumers` is the hook you would drive from your own scheduler or alert. See
    the roadmap in the project plan for where that may go.

---

## 4. Implementing MessageHandler

The `MessageHandler<M>` interface has three methods you must implement:

```java
public interface MessageHandler<M> {
    boolean handle(M message) throws Exception;
    boolean handle(List<M> messages) throws Exception;
    Set<Class<?>> getIgnorableExceptions();
}
```

| Method | Called When | Return `true` | Return `false` |
|--------|-----------|---------------|----------------|
| `handle(M)` | Each message, when the queue has **no** `batchingConfig` | Message acknowledged and deleted | Message sidelined for retry |
| `handle(List<M>)` | Each flushed batch, when the queue **has** a `batchingConfig` | All messages in the batch acknowledged and deleted | All messages in the batch sidelined for retry |
| `getIgnorableExceptions()` | Exception thrown during handling | — | Exceptions in this set are swallowed: the message is **deleted**, not sidelined |

### Simple Handler

```java
public class OrderEventHandler implements MessageHandler<OrderEvent> {

    @Override
    public boolean handle(OrderEvent message) throws Exception {
        log.info("Processing order: {} amount: {}",
                message.getOrderId(), message.getAmount());
        // Business logic here
        return true; // Acknowledge
    }

    @Override
    public boolean handle(List<OrderEvent> messages) throws Exception {
        for (OrderEvent msg : messages) {
            handle(msg);
        }
        return true;
    }

    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        return Set.of(); // No exceptions are ignorable
    }
}
```

### Handler with Retry Logic

Return `false` to sideline the message. Shoveling will re-queue it for another attempt later.

```java
public class PaymentHandler implements MessageHandler<PaymentEvent> {

    @Override
    public boolean handle(PaymentEvent payment) throws Exception {
        try {
            paymentGateway.process(payment);
            return true; // Success — message consumed
        } catch (GatewayTimeoutException e) {
            log.warn("Payment gateway timeout for {}, will retry via shovel",
                    payment.getPaymentId());
            return false; // Sidelined — will be retried when shovel runs
        }
    }

    @Override
    public boolean handle(List<PaymentEvent> messages) throws Exception {
        boolean allSucceeded = true;
        for (PaymentEvent msg : messages) {
            allSucceeded &= handle(msg);
        }
        return allSucceeded;
    }

    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        return Set.of(); // All exceptions cause sidelining
    }
}
```

### Handler with Ignorable Exceptions

Exceptions in the ignorable set are caught and swallowed — the message is **deleted** without being sidelined, and is never redelivered.

```java
public class EventSyncHandler implements MessageHandler<SyncEvent> {

    @Override
    public boolean handle(SyncEvent event) throws Exception {
        if (eventStore.exists(event.getEventId())) {
            throw new DuplicateEventException(event.getEventId());
            // This exception is ignorable - the message is deleted, not sidelined
        }
        eventStore.save(event);
        return true;
    }

    @Override
    public boolean handle(List<SyncEvent> messages) throws Exception {
        for (SyncEvent msg : messages) {
            handle(msg);
        }
        return true;
    }

    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        return Set.of(
                DuplicateEventException.class,  // Skip duplicates silently
                StaleEventException.class        // Skip outdated events
        );
    }
}
```

!!! info "Exception handling flow"
    When an exception is thrown during `handle()`:

    1. If the exception class is in `getIgnorableExceptions()` → message is **deleted** (not sidelined, and gone for good)
    2. Otherwise → message is **sidelined** to the sideline Magazine for later retry

---

## 5. Batch Consumption

Batch mode allows consuming multiple messages in a single `handle(List<M>)` call, which is useful for bulk database writes, batch API calls, or aggregation.

### Configuration

Set `batchingConfig` in the `CreateQueueRequest`:

```java
manager.createQueue(CreateQueueRequest.builder()
        .name("analytics-events")
        .concurrency(4)
        .messageHandlerType("analytics-handler")
        .batchingConfig(BatchingConfig.builder()
                .maxBatchSize(100)
                .maxWaitTimeInSecs(5)
                .build())
        .build());
```

### How Batching Works

The consumer accumulates messages until **either** condition is met:

| Parameter | Range | Default | Trigger |
|-----------|-------|---------|---------|
| `maxBatchSize` | 2 – 200,000 | 2 | Batch is full → deliver immediately |
| `maxWaitTimeInSecs` | 1 – 300 | 1 | Time elapsed → deliver whatever has accumulated |

```
Time ──────────────────────────────────────────────►
     │ msg1  msg2  msg3  ...  msg100               │
     │◄─────── batch fills up ──────►│              │
     │                               ▼              │
     │                    handler.handle(List<M>)   │
     │                                              │
     │ msg1  msg2  msg3                             │
     │◄──── only 3 msgs ────►│                      │
     │                        │ maxWaitTimeInSecs   │
     │                        ▼ elapsed             │
     │             handler.handle(List<M>)          │
```

### Batch Handler Example

```java
public class AnalyticsHandler implements MessageHandler<AnalyticsEvent> {

    private final JdbcTemplate jdbc;

    @Override
    public boolean handle(AnalyticsEvent message) throws Exception {
        return handle(List.of(message)); // Delegate to batch method
    }

    @Override
    public boolean handle(List<AnalyticsEvent> messages) throws Exception {
        // Bulk insert for efficiency
        jdbc.batchUpdate(
                "INSERT INTO analytics (event_id, event_type, payload, ts) VALUES (?, ?, ?, ?)",
                messages.stream()
                        .map(e -> new Object[]{e.getId(), e.getType(), e.getPayload(), e.getTimestamp()})
                        .collect(Collectors.toList())
        );
        log.info("Inserted {} analytics events in batch", messages.size());
        return true;
    }

    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        return Set.of();
    }
}
```

!!! tip "When to use batching"
    Batching is most beneficial when the per-message overhead is high (e.g., database round-trips, HTTP calls). For simple in-memory processing, single-message consumption is typically sufficient.

---

## 6. Shoveling Configuration

Shoveling moves messages from the **sideline queue** back to the **main queue** for re-consumption. This is the retry mechanism for messages that failed processing.

### Configure at Queue Creation

```java
manager.createQueue(CreateQueueRequest.builder()
        .name("payment-events")
        .concurrency(8)
        .messageHandlerType("payment-handler")
        .shovelConfig(ShovelConfig.builder()
                .concurrency(4)         // 4 parallel shovel tasks
                .timeIntervalInSecs(600) // Run every 10 minutes
                .build())
        .build());
```

| Parameter | Range | Default | Description |
|-----------|-------|---------|-------------|
| `concurrency` | 1 – 50 | 4 | Number of parallel shovel tasks |
| `timeIntervalInSecs` | 0 – 86,400 (1 day) | 600 (10 min) | Interval between shovel runs |

### Schedule Shoveling Later

If you didn't configure shoveling at queue creation, you can schedule it afterwards:

```java
manager.scheduleShoveling("payment-events", ShovelConfig.builder()
        .concurrency(2)
        .timeIntervalInSecs(300) // Every 5 minutes
        .build());
```

### Manual One-Shot Shovel

Trigger an immediate, one-time shovel that stops when the sideline is empty:

```java
IQueue<?> queue = manager.getQueue("payment-events");
queue.shovel(4); // 4 concurrent one-shot tasks on the shared worker pool
```

!!! warning "Shovel behavior"
    - **Scheduled shoveling** runs repeatedly at the configured interval
    - **Manual shovel** (`queue.shovel(concurrency)`) runs once and stops when sideline is drained
    - Shovels are capped separately from consumers, by the same check — up to 100 of each

---

## 7. Runtime Queue Management

`IgnisMQManager` provides methods to inspect and manage queues at runtime.

### Retrieving Queues

```java
// Get a specific queue (throws IgnisMQException if not found)
IQueue<OrderEvent> queue = manager.getQueue("order-events");

// Get all active queues on this instance
Map<String, IQueue<?>> allQueues = manager.getAllQueues();
for (Map.Entry<String, IQueue<?>> entry : allQueues.entrySet()) {
    log.info("Queue: {} unconsumed: {}", entry.getKey(), entry.getValue().getUnconsumedCount());
}

// Get all queue names from persistent storage (across all instances)
Set<String> allQueueNames = manager.getAllQueuesFromDB();
log.info("Total queues in cluster: {}", allQueueNames.size());
```

### Scaling Consumers

Adjust the number of consumer tasks for a queue at runtime:

```java
// Scale up: add 4 more consumer tasks
manager.increaseConsumers("order-events", 4);

// Scale down: stop 2 consumer tasks
manager.decreaseConsumers("order-events", 2);
```

!!! note "Consumer limits"
    The maximum is **100 consumers per queue** (`MAX_CONSUMERS_ALLOWED`). Exceeding it throws `IgnisMQException` with error code `MAX_ALLOWED_CONSUMERS_EXCEEDED`.

### Deactivating a Queue

```java
// Stop all consumers and shovel tasks, mark queue as inactive
manager.deactivateQueue("order-events");
```

!!! warning "Irreversible operation"
    `deactivateQueue()` is **permanent**. Once deactivated, the queue cannot be reactivated. The deactivation propagates to all instances on the next refresh cycle (every 5 minutes). Any unconsumed messages remain in Aerospike until their TTL expires.

### Manual Sweep

Trigger the sweep process for a specific queue. Normally one instance sweeps automatically - the leader assigns the sweep partition to a member, and the assigned instance runs it:

```java
manager.sweepQueue("order-events");
```

---

## 8. Queue Monitoring

### Unconsumed Count

```java
IQueue<?> queue = manager.getQueue("order-events");
long pending = queue.getUnconsumedCount();
log.info("Messages waiting to be consumed: {}", pending);
```

### Full Queue Metadata

```java
QueueMetaData meta = queue.getMetaData();
log.info("Published:  {}", meta.getPublished());
log.info("Consumed:   {}", meta.getConsumed());
log.info("Sidelined:  {}", meta.getSidelined());
log.info("Shovelled:  {}", meta.getShovelled());
```

| Field | Description |
|-------|-------------|
| `published` | Total messages ever published (load pointer sum across shards) |
| `consumed` | Total messages consumed/acknowledged (fire pointer sum across shards) |
| `sidelined` | Messages currently in the sideline queue (sideline load − sideline fire) |
| `shovelled` | Messages moved back from sideline to main queue (sideline fire pointer) |

### Automatic Metrics

Core records metrics through the supplied Micrometer `MeterRegistry`. The Dropwizard bundle bridges
those meters into `Environment.metrics()` and registers the cached queue-stat gauge:

| Metric | Type | Description |
|--------|------|-------------|
| `ignismq.publish` | Timer | Publish latency, tagged `queue` and `outcome` |
| `ignismq.consume` | Timer | Batch processing after the messages were claimed, tagged `queue` |
| `ignismq.handler.duration` | Timer | The handler call alone, tagged `queue` and `outcome` |
| `ignismq.queue.depth` | Gauge | Backlog per queue |
| `ignis.queue.stats` | Gauge | Bundle-only cached queue statistics, refreshed every 3 minutes |

The full set, with every tag and the scope of each meter, is in **[Metrics](api/metrics.md)**.

```java
// Access core metrics programmatically
Timer publishTimer = meterRegistry.find(IgnisMetrics.PUBLISH)
        .tag("queue", "order-events")
        .tag("outcome", "success")
        .timer();
log.info("Publish mean: {} ms", publishTimer.mean(TimeUnit.MILLISECONDS));
```

!!! warning "Renamed in 2.0"
    These replace `commands.{queueName}_publish.all` and `commands.{queueName}_consume.all`. The old
    names are not aliased — see the [upgrade notes](upgrading.md).

---

## 9. Queue Lifecycle & TTL

IgnisMQ uses time-to-live (TTL) values to manage the lifecycle of messages and queues.

### TTL Configuration

```java
manager.createQueue(CreateQueueRequest.builder()
        .name("transient-events")
        .concurrency(4)
        .messageHandlerType("transient-handler")
        .messageExpiry(new TimeToLive(TimeUnit.HOUR, 6))   // Messages expire after 6 hours
        .queueExpiry(new TimeToLive(TimeUnit.DAY, 7))      // Queue expires after 7 days
        .build());
```

| Parameter | Default | Max | Description |
|-----------|---------|-----|-------------|
| `messageExpiry` | 2 days | 1 year (365 days) | TTL for individual messages in Aerospike |
| `queueExpiry` | 2 days | 1 year (365 days) | TTL for the queue itself (from creation time) |
| `sweepDurationInMins` | 20 min | 300 min | How far back the sweeper looks for stuck messages |

### Available Time Units

| TimeUnit | Example | Seconds |
|----------|---------|---------|
| `MINUTE` | `new TimeToLive(TimeUnit.MINUTE, 30)` | 1,800 |
| `HOUR` | `new TimeToLive(TimeUnit.HOUR, 12)` | 43,200 |
| `DAY` | `new TimeToLive(TimeUnit.DAY, 7)` | 604,800 |

### TTL Rules and Behavior

- **Queue metadata** is stored with TTL = `queueExpiry × 2`. This ensures metadata outlives both the queue and its messages to prevent Magazine identifier clashes.
- **Validation**: `queueExpiry` must be ≥ `messageExpiry`. This is enforced at creation time.
- **Maximum TTL**: 1 year (31,536,000 seconds). Exceeding this fails validation.
- **Expired queues**: On each refresh cycle (every 5 minutes), queues whose `createdAt + queueExpiry` has passed are automatically deactivated.

```
Timeline:
├── Queue created (createdAt)
│
├── messageExpiry ──► Individual messages expire in Aerospike
│
├── queueExpiry ────► Queue deactivated on next refresh cycle
│
├── queueExpiry × 2 ► Queue metadata removed from Aerospike
```

!!! info "Why queueExpiry × 2?"
    The queue entity is persisted with TTL = `queueExpiry × 2` to guarantee that `(queueExpiry + messageExpiry)` time elapses before the metadata is removed. Since `queueExpiry >= messageExpiry`, `queueExpiry × 2 >= queueExpiry + messageExpiry` always holds. This prevents a new queue with the same name from conflicting with lingering messages from the old queue.

---

## 10. Complete Production Example

A realistic setup with multiple queues, different configurations, and proper error handling.

```java
public class ECommerceApplication extends Application<ECommerceConfig> {

    private final MyIgnisMQBundle ignisMQBundle = new MyIgnisMQBundle();

    @Override
    public void initialize(Bootstrap<ECommerceConfig> bootstrap) {
        bootstrap.addBundle(ignisMQBundle);
    }

    @Override
    public void run(ECommerceConfig config, Environment environment) throws Exception {
        IgnisMQManager manager = ignisMQBundle.getIgnisMQManager();

        // ──────────────────────────────────────────────
        // Register ALL handlers upfront
        // ──────────────────────────────────────────────
        manager.initialiseMessageHandlers(Map.of(
                "order-handler",
                    Map.entry(OrderEvent.class, new OrderEventHandler(orderService)),
                "payment-handler",
                    Map.entry(PaymentEvent.class, new PaymentEventHandler(paymentGateway)),
                "notification-handler",
                    Map.entry(NotificationEvent.class, new NotificationHandler(emailClient, smsClient)),
                "analytics-handler",
                    Map.entry(AnalyticsEvent.class, new AnalyticsHandler(analyticsDb))
        ));

        // ──────────────────────────────────────────────
        // Queue 1: Order Events — standard concurrency, shoveling enabled
        // ──────────────────────────────────────────────
        manager.createQueue(CreateQueueRequest.builder()
                .name("order-events")
                .shards(64)
                .concurrency(16)
                .messageHandlerType("order-handler")
                .messageExpiry(new TimeToLive(TimeUnit.DAY, 3))
                .queueExpiry(new TimeToLive(TimeUnit.DAY, 14))
                .shovelConfig(ShovelConfig.builder()
                        .concurrency(4)
                        .timeIntervalInSecs(300)  // Retry failed orders every 5 minutes
                        .build())
                .sweepDurationInMins(30)
                .build());

        // ──────────────────────────────────────────────
        // Queue 2: Payment Events — critical, aggressive shoveling
        // ──────────────────────────────────────────────
        manager.createQueue(CreateQueueRequest.builder()
                .name("payment-events")
                .shards(32)
                .concurrency(8)
                .messageHandlerType("payment-handler")
                .messageExpiry(new TimeToLive(TimeUnit.DAY, 7))
                .queueExpiry(new TimeToLive(TimeUnit.DAY, 30))
                .shovelConfig(ShovelConfig.builder()
                        .concurrency(8)
                        .timeIntervalInSecs(60)   // Retry failed payments every minute
                        .build())
                .sweepDurationInMins(15)
                .build());

        // ──────────────────────────────────────────────
        // Queue 3: Notifications — lower priority, no shoveling
        // ──────────────────────────────────────────────
        manager.createQueue(CreateQueueRequest.builder()
                .name("notification-events")
                .shards(16)
                .concurrency(4)
                .messageHandlerType("notification-handler")
                .messageExpiry(new TimeToLive(TimeUnit.HOUR, 12))
                .queueExpiry(new TimeToLive(TimeUnit.DAY, 2))
                .build());

        // ──────────────────────────────────────────────
        // Queue 4: Analytics — high throughput with batching
        // ──────────────────────────────────────────────
        manager.createQueue(CreateQueueRequest.builder()
                .name("analytics-events")
                .shards(128)
                .concurrency(8)
                .messageHandlerType("analytics-handler")
                .messageExpiry(new TimeToLive(TimeUnit.DAY, 1))
                .queueExpiry(new TimeToLive(TimeUnit.DAY, 3))
                .batchingConfig(BatchingConfig.builder()
                        .maxBatchSize(500)
                        .maxWaitTimeInSecs(10)
                        .build())
                .build());

        // ──────────────────────────────────────────────
        // Publish from your business logic
        // ──────────────────────────────────────────────
        environment.jersey().register(new OrderResource(manager));
    }
}
```

### Resource Publishing Messages

```java
@Path("/orders")
public class OrderResource {

    private final IgnisMQManager manager;

    public OrderResource(IgnisMQManager manager) {
        this.manager = manager;
    }

    @POST
    public Response createOrder(CreateOrderRequest request) {
        Order order = orderService.create(request);

        // Publish to order queue
        try {
            manager.getQueue("order-events").publish(OrderEvent.builder()
                    .orderId(order.getId())
                    .amount(order.getTotal())
                    .status("CREATED")
                    .build());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize order event", e);
            // Order is already created — log the error, don't fail the API
        }

        // Publish to analytics queue
        try {
            manager.getQueue("analytics-events").publish(AnalyticsEvent.builder()
                    .eventType("ORDER_CREATED")
                    .entityId(order.getId())
                    .timestamp(System.currentTimeMillis())
                    .build());
        } catch (JsonProcessingException e) {
            log.warn("Failed to publish analytics event", e);
        }

        return Response.status(201).entity(order).build();
    }
}
```

### Production Handler with Error Handling

```java
public class PaymentEventHandler implements MessageHandler<PaymentEvent> {

    private final PaymentGateway gateway;
    private static final Logger log = LoggerFactory.getLogger(PaymentEventHandler.class);

    public PaymentEventHandler(PaymentGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public boolean handle(PaymentEvent event) throws Exception {
        try {
            PaymentResult result = gateway.process(event);
            if (result.isSuccess()) {
                log.info("Payment {} processed successfully", event.getPaymentId());
                return true;
            } else {
                log.warn("Payment {} declined: {}", event.getPaymentId(), result.getReason());
                return true; // Declined is a final state — don't retry
            }
        } catch (GatewayTimeoutException e) {
            log.warn("Gateway timeout for payment {}, will retry", event.getPaymentId());
            return false; // Sideline for retry via shoveling
        } catch (InvalidPaymentException e) {
            log.error("Invalid payment data for {}: {}", event.getPaymentId(), e.getMessage());
            throw e; // Not in ignorable set → will be sidelined
        }
    }

    @Override
    public boolean handle(List<PaymentEvent> messages) throws Exception {
        boolean allOk = true;
        for (PaymentEvent msg : messages) {
            allOk &= handle(msg);
        }
        return allOk;
    }

    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        return Set.of(
                DuplicatePaymentException.class  // Already processed — skip silently
        );
    }
}
```

### Runtime Operations

```java
// Monitor queue health
manager.getAllQueues().forEach((name, queue) -> {
    QueueMetaData meta = queue.getMetaData();
    log.info("Queue={} published={} consumed={} sidelined={} pending={}",
            name, meta.getPublished(), meta.getConsumed(),
            meta.getSidelined(), queue.getUnconsumedCount());
});

// Scale up during peak hours
manager.increaseConsumers("order-events", 8);    // 16 → 24 consumers
manager.increaseConsumers("payment-events", 4);  // 8  → 12 consumers

// Scale down during off-peak
manager.decreaseConsumers("order-events", 8);    // 24 → 16 consumers
manager.decreaseConsumers("payment-events", 4);  // 12 → 8  consumers

// Emergency: manually trigger shovel for a queue
manager.getQueue("payment-events").shovel(8);

// Emergency: manually trigger sweep
manager.sweepQueue("payment-events");

// Decommission a queue (IRREVERSIBLE)
manager.deactivateQueue("legacy-events");
```
