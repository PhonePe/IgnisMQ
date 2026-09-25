# IgnisMQ

[![Build](https://github.com/PhonePe/ignisMQ/actions/workflows/maven.yml/badge.svg)](https://github.com/PhonePe/ignisMQ/actions/workflows/maven.yml)
[![SonarCloud](https://github.com/PhonePe/ignisMQ/actions/workflows/sonarcloud-checks.yml/badge.svg)](https://github.com/PhonePe/ignisMQ/actions/workflows/sonarcloud-checks.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=coverage)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=bugs)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_ignisMQ&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=PhonePe_ignisMQ)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/com.phonepe/ignisMQ)](https://central.sonatype.com/artifact/com.phonepe/ignisMQ)

A distributed, persistent message queue built on top of [Magazine](https://github.com/PhonePe/Magazine) with Aerospike as the storage backend. IgnisMQ provides at-least-once message delivery with a sideline queue for failures, automatic shoveling, and ZooKeeper-based coordination so exactly one instance sweeps.

## Features

- **Persistent queues** backed by Aerospike via Magazine
- **Single-sweeper coordination** via ZooKeeper-based leader election and partition assignment
- **Sideline queue** for messages a handler could not process, re-driven by a configurable shovel
- **Shoveling** — automatic transfer of messages between queues
- **Sweeping** — periodic cleanup and reprocessing of stale messages
- **Micrometer instrumentation** in a framework-agnostic core
- **Dropwizard integration** via `ignismq-dw-bundle`, which bridges those meters into the
  application's `MetricRegistry`

## Getting Started

### Maven

```xml
<dependency>
    <groupId>com.phonepe</groupId>
    <artifactId>ignismq-core</artifactId>
    <version>${ignisMQ.version}</version>
</dependency>

<!-- For Dropwizard applications -->
<dependency>
    <groupId>com.phonepe</groupId>
    <artifactId>ignismq-dw-bundle</artifactId>
    <version>${ignisMQ.version}</version>
</dependency>
```

> **Note:** Find the latest version on [Maven Central](https://search.maven.org/artifact/com.phonepe/ignisMQ).

### Prerequisites

- Java 17+
- Apache Maven 3.8+
- Aerospike server
- ZooKeeper ensemble (for leader election)

### Quick Start

#### 1. Implement a handler

```java
public class OrderEventHandler implements MessageHandler<OrderEvent> {

    @Override
    public boolean handle(final OrderEvent message) {
        // true  -> acknowledged and deleted
        // false -> moved to the sideline queue
        return process(message);
    }

    @Override
    public boolean handle(final List<OrderEvent> messages) {
        messages.forEach(this::handle);
        return true;
    }

    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        // Thrown by the handler, these delete the message instead of sidelining it.
        return Set.of();
    }
}
```

#### 2. Build the manager

```java
final AerospikeStorage storage = new AerospikeStorage(aerospikeConfiguration, "my-namespace");

final IgnisMQManager manager = new IgnisMQManager(
        "my-service",               // clientId: this application
        storage,
        new ObjectMapper(),
        new SimpleMeterRegistry(),  // any Micrometer MeterRegistry
        curatorFramework,           // ZooKeeper client, for leader election
        "datacenter-1",             // farmId: this deployment
        IgnisMQSettings.defaults());
```

#### 3. Register handlers, then create the queue

Handlers must be registered first: `createQueue` resolves `messageHandlerType` against this map and
fails if it is missing.

```java
manager.initialiseMessageHandlers(Map.of(
        "order-handler", Map.entry(OrderEvent.class, new OrderEventHandler())));

manager.createQueue(CreateQueueRequest.builder()
        .name("order-events")
        .concurrency(4)                       // consumers for this queue
        .messageHandlerType("order-handler")  // must match the key above
        .build());
```

#### 4. Publish

```java
final IQueue<OrderEvent> queue = manager.getQueue("order-events");
queue.publish(new OrderEvent("ORD-001", 149.99, "CREATED"));
```

There is no consume call. Consumers are scheduled when the queue is created and poll on a fixed
delay; a handler returning `false`, throwing, or exceeding its timeout sends the message to the
sideline queue - except for exceptions listed in `getIgnorableExceptions()`, which delete the
message instead.

Shut down with `manager.getTaskInitializer().stop()` followed by `manager.stop()`. The first releases
leader election and the sweeper; the second stops the schedulers and the storage client. `stop()` alone
leaves the leader elector's thread and its ZooKeeper selector running.

### Dropwizard Bundle

The bundle builds the manager, bridges ignisMQ's and Magazine's Micrometer meters into Dropwizard's
`MetricRegistry`, registers a cached `ignis.queue.stats` gauge, and ties leader election and shutdown
to the application lifecycle. You supply four things:

```java
public class MyApplication extends Application<MyConfiguration> {

    private final IgnisMQBundle<MyConfiguration> ignisMQBundle = new IgnisMQBundle<>() {

        @Override
        protected IgnisMQContext context(final MyConfiguration config) {
            return IgnisMQContext.builder()
                    .clientId(config.getClientId())
                    .farmId(config.getFarmId())
                    .storage(new AerospikeStorage(config.getAerospike(), config.getNamespace()))
                    .curatorFramework(curatorFramework)
                    // Optional. Defaults to IgnisMQSettings.defaults().
                    .settings(IgnisMQSettings.builder().workerThreads(128).build())
                    .build();
        }
    };

    @Override
    public void initialize(final Bootstrap<MyConfiguration> bootstrap) {
        bootstrap.addBundle(ignisMQBundle);
    }

    @Override
    public void run(final MyConfiguration configuration, final Environment environment) {
        ignisMQBundle.getIgnisMQManager().initialiseMessageHandlers(Map.of(
                "order-handler", Map.entry(OrderEvent.class, new OrderEventHandler())));
    }
}
```

## Modules

| Module | Description |
|--------|-------------|
| `ignismq-core` | Core queue implementation, storage, consumers, shoveling, sweeping |
| `ignismq-dw-bundle` | Dropwizard bundle for easy integration |

## Documentation

Full documentation is available at [https://phonepe.github.io/ignisMQ/](https://phonepe.github.io/ignisMQ/).

Worth reading before you adopt it: the [Roadmap](docs/docs/roadmap.md) states plainly what IgnisMQ
does **not** do — no fan-out to independent consumers, no retry with backoff, no delayed delivery,
at-least-once rather than exactly-once — and why.

## Building

```bash
mvn clean install
```

> **Note:** Tests require Docker (Testcontainers spins up an Aerospike instance).

## References

IgnisMQ's core design — building a durable, distributed message queue on top of a scalable key-value store — was inspired by the following academic work:

- **HBaseMQ: A Durable Distributed Message Queue Service Based on HBase**
  Chen Zhang and Xue Liu, McGill University
  *IEEE INFOCOM 2013*

  The paper introduces the concept of layering a persistent message queue over a distributed key-value store (HBase), covering sharding for parallelism, consumer coordination, and message lifecycle management. IgnisMQ applies the same foundational ideas using Aerospike as the storage layer.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.
