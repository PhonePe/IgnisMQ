# Architecture

This page provides a deep dive into the internal architecture of IgnisMQ — its components, threading model, data flows, type hierarchy, and operational mechanics.

---

## 1. System Overview

IgnisMQ is structured in distinct layers, each with a clear responsibility.

```mermaid
graph TB
    subgraph Application Layer
        A[IgnisMQBundle<br/><i>Dropwizard Bundle</i>]
        B[IgnisMQManager<br/><i>Queue Registry</i>]
    end

    subgraph Queue Layer
        C1[MagazineQueue #1]
        C2[MagazineQueue #2]
        CN[MagazineQueue #N]
        subgraph "Each MagazineQueue"
            MQ_MAIN[Main Magazine]
            MQ_SIDE[Sideline Magazine]
        end
    end

    subgraph Consumer Layer
        D1[MagazineConsumerTask Timers]
        D2[ShovelTask Timers]
    end

    subgraph Coordination Layer
        E1[TaskInitializer]
        E2[LeaderElector]
        E3[Sweeper]
        E4[ZooKeeper / Curator]
    end

    subgraph Storage Layer
        F1[AerospikeQueueService]
        F2[AerospikeStoreClient]
        F3[(Aerospike)]
    end

    A -->|creates| B
    B -->|holds| C1
    B -->|holds| C2
    B -->|holds| CN
    C1 --> D1
    C1 --> D2
    E1 -->|creates| E2
    E1 -->|creates| E3
    E2 -->|LeaderSelector| E4
    E3 -->|activate/deactivate| D1
    C1 --> F1
    F1 --> F2
    F2 --> F3
```

!!! info "Layered Design"
    Each layer only communicates with its immediate neighbours. The application layer never touches storage directly — it always goes through the queue abstraction.

---

## 2. Component Relationships

```mermaid
classDiagram
    class IgnisMQBundle {
        -IgnisMQManager manager
        +run(config, environment)
    }

    class IgnisMQManager {
        -ConcurrentHashMap~String, IQueue~ queues
        +registerQueue(config, handler) IQueue
        +getQueue(name) IQueue
    }

    class IQueue~M~ {
        <<sealed interface>>
        +publish(M message)
        +publishWithDelay(M message, long delay)
    }

    class MagazineQueue~M~ {
        -Magazine mainMagazine
        -Magazine sidelineMagazine
        -List~Timer~ consumerTimers
        -List~Timer~ shovelTimers
        -ObjectMapper objectMapper
        +publish(M message)
        +publishWithDelay(M message, long delay)
    }

    class Magazine {
        -QueueService queueService
        -String setName
        +load(Record record)
        +fire() Record
        +delete(String id)
    }

    class MagazineConsumerTask {
        -Magazine magazine
        -Magazine sidelineMagazine
        -QueueHandler handler
        +run()
    }

    class ShovelTask {
        -Magazine sidelineMagazine
        -Magazine mainMagazine
        +run()
    }

    class TaskInitializer {
        -LeaderElector leaderElector
        -Sweeper sweeper
        +initialize()
    }

    class LeaderElector {
        -LeaderSelector leaderSelector
        -CuratorFramework curator
        +takeLeadership()
        +updateState()
    }

    class Sweeper {
        <<implements LoadBalancer>>
        -ExecutorService executorService
        +activate()
        +deactivate()
        +run()
    }

    class QueueStatGauge {
        <<Supplier~List~>>
        +get() List
    }

    class QueueService {
        <<sealed interface>>
        +load(Record record)
        +fire(String set) Record
        +delete(String set, String id)
        +scan(String set, Filter filter) List
    }

    class AerospikeQueueService {
        -AerospikeStoreClient client
    }

    class AerospikeStoreClient {
        -IAerospikeClient aerospikeClient
    }

    IgnisMQBundle --> IgnisMQManager : creates
    IgnisMQManager o-- IQueue : ConcurrentHashMap
    IQueue <|.. MagazineQueue : permits
    MagazineQueue *-- Magazine : main
    MagazineQueue *-- Magazine : sideline
    MagazineQueue *-- MagazineConsumerTask : consumerTimers
    MagazineQueue *-- ShovelTask : shovelTimers
    MagazineConsumerTask --> Magazine : reads from main
    MagazineConsumerTask --> Magazine : writes to sideline
    ShovelTask --> Magazine : reads from sideline
    ShovelTask --> Magazine : writes to main
    TaskInitializer --> LeaderElector : creates
    TaskInitializer --> Sweeper : creates
    LeaderElector ..> Sweeper : activates/deactivates
    Magazine --> QueueService
    QueueService <|.. AerospikeQueueService : permits
    AerospikeQueueService --> AerospikeStoreClient
    IgnisMQManager --> QueueStatGauge : reads
    IgnisMQBundle --> QueueStatGauge : exposes as CachedGauge
```

!!! note "ConcurrentHashMap"
    `IgnisMQManager` uses a `ConcurrentHashMap<String, IQueue>` to hold all registered queues. This allows thread-safe registration and lookup at runtime.

---

## 3. Threading Model

IgnisMQ relies heavily on `java.util.Timer` threads and a shared `ForkJoinPool` for background work.

### Timer Schedule Summary

| Component | Count | Initial Delay | Period | Notes |
|---|---|---|---|---|
| **Consumer Timer** | 1 per concurrency slot | 2 min | 1 sec | `Timer.schedule(task, 120_000, 1_000)` |
| **Shovel Timer** | 1 per shovel concurrency | 2 min | `timeIntervalInSecs × 1000` | Configurable interval |
| **Watcher Timer** | 1 global | 1 min | 5 min | `Timer.schedule(refreshQueues, 60_000, 300_000)` |
| **Sweeper Timer** | 1 global | 10 min | 15 min | `Timer.schedule(sweeper, 600_000, 900_000)` |
| **Leader Election** | 1 global | — | 30 sec poll | `updateState()` polling loop |
| **Sweeper Executor** | ForkJoinPool | — | — | `parallelism = 64`, used for parallel queue sweeps |

```mermaid
gantt
    title IgnisMQ Timer Startup Timeline
    dateFormat ss
    axisFormat %S s

    section Consumers
    Initial delay (2 min)           :done, c_delay, 00, 120s
    Consumer fires every 1s         :active, c_run, after c_delay, 30s

    section Shovels
    Initial delay (2 min)           :done, s_delay, 00, 120s
    Shovel fires every N sec        :active, s_run, after s_delay, 30s

    section Watcher
    Initial delay (1 min)           :done, w_delay, 00, 60s
    Watcher fires every 5 min       :active, w_run, after w_delay, 30s

    section Sweeper
    Initial delay (10 min)          :done, sw_delay, 00, 600s
    Sweeper fires every 15 min      :active, sw_run, after sw_delay, 30s

    section Leader Election
    Curator LeaderSelector           :active, le, 00, 30s
    updateState poll (30s loop)      :active, le2, after le, 30s
```

!!! warning "Thread Counts"
    For a queue with `concurrency = 8` and `shovelConcurrency = 2`, IgnisMQ spawns **10 Timer threads** for that single queue alone. Plan your concurrency settings carefully.

---

## 4. Data Flow

### 4.1 Publish Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant IQ as IQueue.publish()
    participant OM as ObjectMapper
    participant Mag as Main Magazine
    participant QS as AerospikeQueueService
    participant AS as Aerospike

    App->>IQ: publish(message)
    IQ->>OM: writeValueAsString(message)
    OM-->>IQ: JSON string
    IQ->>Mag: load(record)
    Mag->>QS: load(record)
    QS->>AS: put(key, bins)
    AS-->>QS: ok
    QS-->>Mag: ok
    Mag-->>IQ: ok
    IQ-->>App: void
```

### 4.2 Consume Flow

```mermaid
sequenceDiagram
    participant T as Timer
    participant CT as MagazineConsumerTask
    participant M as Main Magazine
    participant QS as QueueService
    participant AS as Aerospike
    participant H as QueueHandler

    T->>CT: run()
    CT->>M: fire()
    M->>QS: fire(setName)
    QS->>AS: query(set, filter)
    AS-->>QS: Record
    QS-->>M: Record
    M-->>CT: Record (with fireTS added)
    CT->>CT: addFireTimestamp(record)
    CT->>CT: deserialize(JSON → M)
    CT->>H: handle(message)

    alt Success
        H-->>CT: ok
        CT->>M: delete(id)
        M->>QS: delete(set, id)
        QS->>AS: delete(key)
    else Failure
        H-->>CT: exception
        CT->>CT: sidelineMagazine.load(record)
        CT->>M: delete(id)
    end
```

### 4.3 Shovel Flow

```mermaid
sequenceDiagram
    participant T as Timer
    participant ST as ShovelTask
    participant SL as Sideline Magazine
    participant M as Main Magazine
    participant AS as Aerospike

    T->>ST: run()
    ST->>SL: fire()
    SL->>AS: query(sidelineSet)
    AS-->>SL: Record
    SL-->>ST: Record
    ST->>M: load(record)
    M->>AS: put(mainSet, record)
    AS-->>M: ok
    M-->>ST: ok
    ST->>SL: delete(id)
    SL->>AS: delete(sidelineSet, id)
```

### 4.4 Sweep Flow

```mermaid
sequenceDiagram
    participant Timer
    participant SW as Sweeper
    participant FJP as ForkJoinPool (p=64)
    participant QS as QueueService
    participant AS as Aerospike
    participant SL as Sideline Magazine

    Timer->>SW: run()
    SW->>QS: getQueues(active=true)
    QS-->>SW: List<QueueConfig>

    loop For each queue + shard (parallel via FJP)
        SW->>FJP: submit(sweepTask)
        FJP->>QS: scan(set, fireTS < threshold)
        QS->>AS: scan with filter
        AS-->>QS: stale Records
        loop For each stale record
            QS->>SL: load(record) [reload to sideline]
            QS->>AS: delete(original record)
        end
    end
```

!!! tip "Sweep Threshold"
    The sweeper scans for records whose `fireTS` is older than a configurable threshold. These are messages that were dequeued (fire timestamp set) but never successfully processed or deleted — indicating a crashed consumer.

---

## 5. Sealed Type Hierarchy

IgnisMQ uses Java 17 sealed types to enforce a closed set of implementations.

```mermaid
classDiagram
    class IQueue~M~ {
        <<sealed interface>>
        +publish(M message)
        +publishWithDelay(M message, long delay)
    }
    class MagazineQueue~M~ {
        <<final>>
    }
    IQueue <|.. MagazineQueue : permits

    class QueueService {
        <<sealed interface>>
        +load(Record)
        +fire(String set) Record
        +delete(String set, String id)
    }
    class AerospikeQueueService {
        <<final>>
    }
    QueueService <|.. AerospikeQueueService : permits

    class BaseStorage {
        <<abstract sealed>>
        +accept(StorageVisitor~T~ visitor) T
    }
    class AerospikeStorage {
        <<final>>
        -String namespace
        -IAerospikeClient client
    }
    BaseStorage <|-- AerospikeStorage : permits

    class StorageClient~T~ {
        <<interface>>
        +get(Key key) T
        +put(Key key, Bin... bins)
        +delete(Key key)
    }
    class AerospikeStoreClient {
        <<final>>
    }
    StorageClient <|.. AerospikeStoreClient : implements
```

!!! info "Why Sealed Types?"
    Sealed types provide exhaustiveness guarantees at compile time. When you pattern-match on `IQueue`, the compiler knows the only possible implementation is `MagazineQueue`. This makes the codebase safer to extend — adding a new storage backend requires updating all `permits` clauses and their consumers.

---

## 6. Visitor Pattern

IgnisMQ uses the visitor pattern to decouple storage construction from storage type specifics.

```mermaid
classDiagram
    class BaseStorage {
        <<abstract sealed>>
        +accept(StorageVisitor~T~ visitor) T
    }

    class AerospikeStorage {
        -String namespace
        -IAerospikeClient client
        +accept(StorageVisitor~T~ visitor) T
    }

    class StorageVisitor~T~ {
        <<interface>>
        +visit(AerospikeStorage storage) T
    }

    class MagazineStorageVisitor {
        <<inner class of MagazineQueue>>
        +visit(AerospikeStorage storage) Magazine
    }

    class QueueServiceVisitor {
        <<used by IgnisMQManager>>
        +visit(AerospikeStorage storage) QueueService
    }

    class StoreClientVisitor {
        <<used by IgnisMQManager>>
        +visit(AerospikeStorage storage) StorageClient
    }

    BaseStorage <|-- AerospikeStorage : permits
    StorageVisitor <|.. MagazineStorageVisitor
    StorageVisitor <|.. QueueServiceVisitor
    StorageVisitor <|.. StoreClientVisitor
    BaseStorage --> StorageVisitor : accept()
    AerospikeStorage ..|> BaseStorage
```

**How it works:**

1. `IgnisMQManager` receives a `BaseStorage` instance (which is actually `AerospikeStorage`).
2. To build a `QueueService`, it calls `storage.accept(new QueueServiceVisitor())`.
3. `AerospikeStorage.accept()` dispatches to `visitor.visit(this)`.
4. The visitor has access to the concrete `AerospikeStorage` fields and constructs `AerospikeQueueService`.

The same pattern is used inside `MagazineQueue` — `MagazineStorageVisitor` builds the `Magazine` instances from the concrete storage.

!!! note "Extensibility"
    To add a new storage backend (e.g., Redis), you would: (1) create `RedisStorage extends BaseStorage`, (2) add `visit(RedisStorage)` to `StorageVisitor`, (3) implement the visitor methods. The sealed hierarchy ensures you cannot forget any call site.

---

## 7. Metrics

Core records metrics through Micrometer. `ignismq-dw-bundle` bridges them into the application's
Dropwizard `MetricRegistry` and supplies Dropwizard-specific gauges and lifecycle adapters.

### Registered Metrics

| Metric Name Pattern | Type | Description |
|---|---|---|
| `commands.{queueName}_publish.all` | Timer | Latency and throughput of publish operations |
| `commands.{queueName}_consume.all` | Timer | Latency and throughput of consume operations |
| `magazine.*` | Counters and timers | Magazine's own instrumentation, tagged by magazine and outcome |
| `ignis.queue.stats` | CachedGauge (3 min TTL, bundle only) | Reports `published`, `consumed`, `unconsumed`, `sidelined`, `shovelled` counts |

Dropwizard has no notion of tags, so the bundle's bridge flattens each Micrometer tag into the
metric name. `magazine.fire.outcomes` tagged `magazine=orders, outcome=delivered` is published as
`magazine.fire.outcomes.magazine.orders.outcome.delivered`. Untagged meters, including the
`commands.*` timers above, keep their names unchanged.

### QueueStatGauge

```mermaid
flowchart LR
    A[Metrics Reporter] -->|getValue| B[QueueStatGauge]
    B -->|cached?| C{Cache fresh?<br/>TTL = 3 min}
    C -->|Yes| D[Return cached Map]
    C -->|No| E[loadValue]
    E --> F[Query Aerospike<br/>for queue stats]
    F --> G[Return Map with<br/>published, consumed,<br/>unconsumed, sidelined,<br/>shovelled]
    G --> D
```

### Method-level metrics

Earlier versions wove `@MonitoredFunction` timers into most public methods with AspectJ. That was
removed along with the Dropwizard coupling: it pulled AspectJ and a build-time weaving step into
`ignismq-core`, and the resulting timers measured method entry and exit rather than anything
operationally meaningful. Deliberate instrumentation is being added back in its place; until then
the `commands.*` timers and Magazine's `magazine.*` meters are the supported surface.

---

## 8. ZooKeeper Node Structure

IgnisMQ uses ZooKeeper (via Apache Curator) for leader election and work distribution among instances.

```
/{clientId}-ignis-workers/
  └── {clientId}/
      ├── loadbalancer-leader          ← LeaderSelector (persistent node)
      │                                  Only one instance holds leadership.
      │                                  Leader runs the Sweeper.
      │
      ├── loadbalancer/
      │   └── {balancerId}             ← Ephemeral sequential nodes.
      │                                  Each running IgnisMQ instance
      │                                  registers itself here.
      │
      └── readers/
          └── {partition}              ← Communicator nodes.
                                         Each partition stores the
                                         assigned balancerId, mapping
                                         partitions to instances.
```

```mermaid
graph TD
    ROOT["/{clientId}-ignis-workers"] --> CLIENT["/{clientId}"]
    CLIENT --> LEADER["loadbalancer-leader<br/><i>LeaderSelector</i>"]
    CLIENT --> LB["loadbalancer/"]
    CLIENT --> READERS["readers/"]

    LB --> LB1["{balancerId-001}<br/><i>ephemeral</i>"]
    LB --> LB2["{balancerId-002}<br/><i>ephemeral</i>"]
    LB --> LBN["{balancerId-N}<br/><i>ephemeral</i>"]

    READERS --> R1["{partition-0}<br/>assigned: balancerId-001"]
    READERS --> R2["{partition-1}<br/>assigned: balancerId-002"]
    READERS --> RN["{partition-N}<br/>assigned: balancerId-001"]

    style LEADER fill:#f9a825,stroke:#f57f17
    style LB1 fill:#81d4fa,stroke:#0277bd
    style LB2 fill:#81d4fa,stroke:#0277bd
    style LBN fill:#81d4fa,stroke:#0277bd
```

**Leader election flow:**

1. Each instance creates an ephemeral node under `loadbalancer/`.
2. Curator's `LeaderSelector` competes for `loadbalancer-leader`.
3. The winning instance calls `takeLeadership()`, which activates the `Sweeper`.
4. The leader polls `updateState()` every **30 seconds** to reassign partitions across live balancer nodes.
5. When the leader dies, its ephemeral node disappears and Curator triggers a new election.

!!! warning "ZooKeeper Dependency"
    ZooKeeper is required for multi-instance deployments. In single-instance mode, the instance always becomes the leader. If ZK is unavailable, sweeping and partition rebalancing will stall — but publish and consume continue to work.
