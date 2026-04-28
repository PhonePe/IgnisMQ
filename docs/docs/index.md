# IgnisMQ

<div style="text-align: center; margin: 2em 0;">
<p style="font-size: 1.2em; color: #666;">
A distributed, persistent, high-throughput message queue for Java
</p>
</div>

---

IgnisMQ is a Java library that provides reliable asynchronous message processing built on top of [Magazine](https://github.com/PhonePe/Magazine) and Aerospike, with first-class Dropwizard integration for microservice environments.

## Key Features

<div class="grid cards" markdown>

-   :material-lightning-bolt:{ .lg .middle } **High Throughput**

    ---

    Backed by Aerospike — handles millions of messages per second with sub-millisecond latency. Sharded queues distribute load across the cluster.

-   :material-shield-check:{ .lg .middle } **Reliable Delivery**

    ---

    Failed messages are automatically **sidelined** and can be **shoveled** back for reprocessing. Stuck messages are **swept** and recovered.

-   :material-account-group:{ .lg .middle } **Distributed Coordination**

    ---

    ZooKeeper-based **leader election** ensures exactly one sweeper runs across the cluster. Load balancing distributes partitions across nodes.

-   :material-tune:{ .lg .middle } **Configurable Everything**

    ---

    Concurrency, TTLs, batch sizes, shovel intervals, sweep durations — every aspect of queue behavior is tunable at runtime.

</div>

## How It Works

```mermaid
sequenceDiagram
    participant App as Your Application
    participant MQ as IgnisMQManager
    participant Q as MagazineQueue
    participant M as Magazine (Aerospike)
    participant S as Sideline Magazine
    
    App->>MQ: publish("order-queue", order)
    MQ->>Q: publish(order)
    Q->>M: magazine.load(json)
    
    Note over Q: Consumer timer fires every 1s
    
    Q->>M: magazine.fire()
    M-->>Q: MagazineData
    Q->>App: handler.handle(order)
    
    alt Handler returns false
        Q->>S: sidelineMagazine.load(json)
        Note over S: Message saved for retry
    end
    
    Q->>M: magazine.delete(data)
    
    Note over S: Shovel timer fires periodically
    S-->>M: Move messages back to main queue
```

## Quick Start

=== "Dropwizard Bundle"

    ```java
    // 1. Extend the bundle
    public class MyIgnisMQBundle extends IgnisMQBundle<MyConfig> {
        @Override
        protected BaseStorage getStorage(MyConfig config) {
            return new AerospikeStorage(config.getAerospikeConfig(), "my-ns");
        }
        @Override
        protected String getClientId(MyConfig c) { return c.getClientId(); }
        @Override
        protected String getFarmId(MyConfig c) { return c.getFarmId(); }
        @Override
        protected CuratorFramework getCuratorFramework() { return curator; }
    }

    // 2. Register in your Application
    bootstrap.addBundle(ignisMQBundle);

    // 3. Initialize handlers and create a queue
    manager.initialiseMessageHandlers(Map.of(
        "order-handler", Map.entry(Order.class, new OrderHandler())
    ));

    manager.createQueue(CreateQueueRequest.builder()
        .name("order-queue")
        .concurrency(8)
        .messageHandlerType("order-handler")
        .build());

    // 4. Publish messages
    manager.getQueue("order-queue").publish(new Order("ORD-123", 99.99));
    ```

=== "Standalone"

    ```java
    // 1. Create storage and manager
    BaseStorage storage = new AerospikeStorage(aerospikeConfig, "my-ns");
    IgnisMQManager manager = new IgnisMQManager(
        "my-client", storage, new ObjectMapper(),
        new MetricRegistry(), curatorFramework, "farm-1"
    );

    // 2. Register handlers
    manager.initialiseMessageHandlers(Map.of(
        "order-handler", Map.entry(Order.class, new OrderHandler())
    ));

    // 3. Create a queue and publish
    manager.createQueue(CreateQueueRequest.builder()
        .name("order-queue")
        .concurrency(4)
        .messageHandlerType("order-handler")
        .build());

    manager.getQueue("order-queue").publish(new Order("ORD-123", 99.99));
    ```

## Message Lifecycle at a Glance

```mermaid
stateDiagram-v2
    [*] --> Published: publish()
    Published --> Fired: consumer fires message
    Fired --> Consumed: handler returns true
    Fired --> Sidelined: handler returns false / exception
    Consumed --> [*]: message deleted
    Sidelined --> Shoveled: shovel task runs
    Shoveled --> Published: message re-queued
    Fired --> Swept: message stuck (no ack)
    Swept --> Sidelined: sweeper recovers
```

## Architecture Overview

```mermaid
flowchart TD
    subgraph Application
        App["Your App"] --> Bundle["IgnisMQBundle"]
        Bundle --> Manager["IgnisMQManager"]
    end

    subgraph Queue Management
        Manager --> Q1["MagazineQueue #1"]
        Manager --> Q2["MagazineQueue #2"]
        Manager --> QN["MagazineQueue #N"]
    end

    subgraph Per-Queue Components
        Q1 --> C["Consumer Timers<br/>(1s interval)"]
        Q1 --> SH["Shovel Timers<br/>(configurable interval)"]
    end

    subgraph Cluster Coordination
        Manager --> TI["TaskInitializer"]
        TI --> LE["LeaderElector"]
        LE --> ZK["ZooKeeper"]
        TI --> SW["Sweeper<br/>(leader-only)"]
    end

    subgraph Storage
        C --> Mag["Magazine<br/>(Main)"]
        C --> Side["Magazine<br/>(Sideline)"]
        SH --> Mag
        SH --> Side
        SW --> AQS["AerospikeQueueService"]
        Mag --> AS["Aerospike"]
        Side --> AS
        AQS --> AS
    end

    Manager -.->|refreshQueues<br/>every 5 min| AS
```

## What to Read Next

| Page | Description |
|------|-------------|
| [Getting Started](getting-started.md) | Prerequisites, installation, and your first queue in 5 minutes |
| [Core Concepts](core-concepts.md) | Message lifecycle, sidelining, shoveling, sweeping, leader election |
| [Usage Guide](usage.md) | Complete examples — Dropwizard, standalone, batching, shoveling |
| [Architecture](architecture.md) | Component deep-dive, threading model, data flow |
| [API Reference](api/api-reference.md) | Every class, method, and field documented |
| [Configuration](api/configuration.md) | All configuration options with defaults and constraints |
| [Error Codes](api/error-codes.md) | Complete error catalog with causes and solutions |
| [Aerospike Backend](backends/aerospike.md) | Data model, indexing, sweep internals |
| [References](references.md) | Academic and industry work that inspired IgnisMQ's design |
