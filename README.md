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

A distributed, persistent message queue built on top of [Magazine](https://github.com/PhonePe/Magazine) with Aerospike as the storage backend. IgnisMQ provides reliable message delivery with consumer groups, leader election, dead-letter queues, and automatic shoveling.

## Features

- **Persistent queues** backed by Aerospike via Magazine
- **Consumer groups** with ZooKeeper-based leader election
- **Dead-letter queue (DLQ)** support with configurable retry policies
- **Shoveling** — automatic transfer of messages between queues
- **Sweeping** — periodic cleanup and reprocessing of stale messages
- **Dropwizard integration** via `ignismq-dw-bundle`
- **Function metrics** with AspectJ-based instrumentation

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

### Prerequisites

- Java 17+
- Apache Maven 3.8+
- Aerospike server
- ZooKeeper ensemble (for leader election)

### Quick Start

```java
// Create queue configuration
IgnisMQConfig config = IgnisMQConfig.builder()
        .queueConfigs(List.of(
            QueueConfig.builder()
                .queueName("my-queue")
                .namespace("test")
                .build()
        ))
        .build();

// Initialize and start the queue manager
IgnisMQManager manager = new IgnisMQManager(config, aerospikeClient, curatorFramework, metricRegistry);
manager.start();

// Enqueue a message
manager.enqueue("my-queue", message);

// Register a consumer
manager.registerConsumer("my-queue", message -> {
    // Process message
    return true;
});
```

### Dropwizard Bundle

```java
public class MyApplication extends Application<MyConfiguration> {
    private final IgnisMQBundle<MyConfiguration> ignisMQBundle = new IgnisMQBundle<>() {
        @Override
        protected IgnisMQConfig getIgnisMQConfig(MyConfiguration config) {
            return config.getIgnisMQConfig();
        }
    };

    @Override
    public void initialize(Bootstrap<MyConfiguration> bootstrap) {
        bootstrap.addBundle(ignisMQBundle);
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
