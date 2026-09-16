# Getting Started

This guide walks you through setting up IgnisMQ and running your first queue in under 5 minutes.

## Prerequisites

| Requirement | Version | Purpose |
|-------------|---------|---------|
| **Java** | 17+ | Sealed classes, pattern matching used throughout |
| **Apache Maven** | 3.8+ | Build tool |
| **Aerospike** | 5.x / 6.x | Queue storage backend |
| **ZooKeeper** | 3.4+ | Leader election and cluster coordination |
| **Docker** | Any recent | Running integration tests via Testcontainers |

## Installation

### Maven

Add the dependency for the module you need:

=== "Core Library Only"

    Use this if you have your own application framework or want to manage the lifecycle yourself.

    ```xml
    <dependency>
        <groupId>com.phonepe</groupId>
        <artifactId>ignismq-core</artifactId>
        <version>${ignismq.version}</version>
    </dependency>
    ```

=== "Dropwizard Bundle"

    Use this if you're building a Dropwizard application. This includes `ignismq-core` transitively.

    ```xml
    <dependency>
        <groupId>com.phonepe</groupId>
        <artifactId>ignismq-dw-bundle</artifactId>
        <version>${ignismq.version}</version>
    </dependency>
    ```

!!! tip "Find the latest version"
    Check [Maven Central](https://central.sonatype.com/search?q=g%3Acom.phonepe+a%3Aignismq*) for the latest release version.

## Build from Source

```bash
git clone https://github.com/PhonePe/ignisMQ.git
cd ignisMQ
mvn clean install
```

To run the full test suite (requires Docker):

```bash
mvn clean test
```

!!! note "Docker required for tests"
    Integration tests use [Testcontainers](https://www.testcontainers.org/) to spin up an Aerospike instance (`aerospike/aerospike-server:6.2.0.7`). Make sure Docker is running before executing tests.

## Your First Queue in 5 Minutes

This example shows the minimal code to create a queue, publish a message, and consume it.

### Step 1: Define Your Message Type

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
```

### Step 2: Implement a Message Handler

The `MessageHandler` interface is how you process messages. Return `true` to acknowledge, `false` to sideline for retry.

```java
public class OrderEventHandler implements MessageHandler<OrderEvent> {

    @Override
    public boolean handle(OrderEvent message) throws Exception {
        System.out.println("Processing order: " + message.getOrderId()
                + " amount: " + message.getAmount());

        // Your business logic here
        // Return true  → message consumed successfully
        // Return false → message sidelined for later retry
        return true;
    }

    @Override
    public boolean handle(List<OrderEvent> messages) throws Exception {
        // This is the overload ignisMQ calls, in every mode. A non-batching
        // queue passes a one-element list.
        for (OrderEvent msg : messages) {
            handle(msg);
        }
        return true;
    }

    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        // These exceptions will NOT cause sidelining
        return Set.of();
    }
}
```

### Step 3: Create the Manager and Queue

=== "Standalone"

    ```java
    // Configure Aerospike storage
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

    // Create the manager
    IgnisMQManager manager = new IgnisMQManager(
        "my-service",               // clientId — identifies this application
        storage,                     // Aerospike storage
        new ObjectMapper(),          // Jackson mapper for serialization
        new SimpleMeterRegistry(),   // any Micrometer MeterRegistry
        curatorFramework,            // ZooKeeper curator client
        "datacenter-1",             // farmId — identifies this deployment
        IgnisMQSettings.defaults()   // worker cap and the metrics switch
    );

    // Register message handlers (MUST be called before createQueue)
    manager.initialiseMessageHandlers(Map.of(
        "order-handler", Map.entry(OrderEvent.class, new OrderEventHandler())
    ));

    // Create the queue
    manager.createQueue(CreateQueueRequest.builder()
        .name("order-events")
        .shards(32)                      // Number of Magazine shards
        .concurrency(4)                  // Consumer tasks on the shared worker pool
        .messageHandlerType("order-handler")  // Must match a registered handler
        .messageExpiry(new TimeToLive(TimeUnit.DAY, 2))
        .queueExpiry(new TimeToLive(TimeUnit.DAY, 7))
        .build());
    ```

=== "Dropwizard Bundle"

    ```java
    public class MyApplication extends Application<MyConfig> {

        private final MyIgnisMQBundle ignisMQBundle = new MyIgnisMQBundle();

        @Override
        public void initialize(Bootstrap<MyConfig> bootstrap) {
            bootstrap.addBundle(ignisMQBundle);
        }

        @Override
        public void run(MyConfig config, Environment env) {
            IgnisMQManager manager = ignisMQBundle.getIgnisMQManager();

            manager.initialiseMessageHandlers(Map.of(
                "order-handler",
                Map.entry(OrderEvent.class, new OrderEventHandler())
            ));

            manager.createQueue(CreateQueueRequest.builder()
                .name("order-events")
                .concurrency(4)
                .messageHandlerType("order-handler")
                .build());
        }
    }
    ```

### Step 4: Publish Messages

```java
IQueue<OrderEvent> queue = manager.getQueue("order-events");

// Publish a single message
queue.publish(OrderEvent.builder()
    .orderId("ORD-001")
    .amount(149.99)
    .status("CREATED")
    .build());

// Publish in a loop
for (int i = 0; i < 1000; i++) {
    queue.publish(OrderEvent.builder()
        .orderId("ORD-" + i)
        .amount(Math.random() * 500)
        .status("CREATED")
        .build());
}
```

That's it! Messages are consumed automatically. There is no "consume" method to call — consumers are
scheduled when the queue is created.

!!! info "Consumer startup delay"
    Consumers start **1 second** after the queue is created (`INITIAL_DELAY_IN_MS`) and then poll on a
    1 second fixed delay. Earlier versions waited 2 minutes; that cold start was removed along with
    the per-consumer timers.

## What Happens Behind the Scenes

After you call `createQueue()`:

1. **Two Magazine instances** are created — a main queue and a sideline queue (`{name}_SIDELINE`) —
   sharing one storage, and therefore one set of Magazine's caches
2. **`concurrency` consumer tasks** are scheduled on the manager's shared worker pool, each polling on
   a 1 second fixed delay. They are tasks, not threads: `concurrency` is a target share of
   `workerThreads`, not a private thread per consumer
3. **Queue metadata** is persisted to Aerospike with TTL = `queueExpiry * 2`

Two more things run in a working process, and neither is triggered by `createQueue`:

- A **background watcher** refreshes queue state from the database every 5 minutes, on the separate
  control pool so that user code cannot starve it. It is scheduled by the manager's **constructor**
- The **TaskInitializer** starts leader election and schedules the sweeper, once per process, when
  something calls **`start()`** — in a Dropwizard app the bundle does this for you

## Next Steps

| Topic | Description |
|-------|-------------|
| [Core Concepts](core-concepts.md) | Understand sidelining, shoveling, sweeping, and leader election |
| [Usage Guide](usage.md) | Advanced examples — batching, shoveling, runtime scaling |
| [Configuration](api/configuration.md) | All config options with defaults and constraints |
| [Metrics](api/metrics.md) | What every meter means, and what it does not cover |
| [Monitoring runbook](operations/monitoring.md) | What to alert on, and what each alert means |
