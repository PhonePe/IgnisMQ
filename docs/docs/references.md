# References

IgnisMQ's core design was influenced by prior academic and industry work on building durable, distributed message queues over scalable key-value stores. This page documents those references.

---

## HBaseMQ: A Durable Distributed Message Queue Service Based on HBase

**Authors:** Chen Zhang and Xue Liu (McGill University)
**Venue:** IEEE INFOCOM 2013
**Reference type:** Academic inspiration

### Summary

HBaseMQ describes a durable, distributed message queue built on top of HBase — a wide-column, distributed key-value store. The paper addresses the fundamental challenge of providing reliable message delivery guarantees without introducing a dedicated messaging broker, instead leveraging the persistence and scalability properties of the underlying store.

Key contributions of the paper:

- **Persistent message storage in a distributed KV store** — messages are stored as rows in HBase, giving durability and replication for free from the storage layer.
- **Sharding for parallelism** — queues are partitioned across HBase regions, allowing parallel producers and consumers to scale horizontally.
- **Consumer coordination** — the paper covers how multiple consumers can safely consume from a shared queue without double-processing messages.
- **Message lifecycle management** — describes the states a message moves through (enqueued → in-flight → acknowledged / dead-lettered) and how the system recovers from failures at each stage.

### Relation to IgnisMQ

IgnisMQ applies the same foundational ideas using **Aerospike** as the storage layer instead of HBase:

| HBaseMQ concept | IgnisMQ equivalent |
|---|---|
| HBase as durable KV store | Aerospike via Magazine |
| Queue sharding across HBase regions | Sharded Magazine queues |
| Consumer coordination | ZooKeeper-based leader election + load-balanced partition assignment |
| Message lifecycle (enqueued → in-flight → ack / DLQ) | Published → Fired → Consumed / Sidelined → Shoveled |
| Failure recovery | Sweeper (detects stuck messages) + Shovel (re-queues sidelined messages) |

The core insight — that a high-throughput, persistent message queue can be built directly on a scalable KV store without a dedicated broker — is the intellectual foundation of IgnisMQ.
