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
        D1[MagazineConsumerTask x concurrency]
        D2[ShovelTask x shovel concurrency]
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
    E2 -->|activates the assigned instance| E3
    B --> F1
    E3 --> F1
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
        <<abstract>>
        -IgnisMQManager ignisMQManager
        +run(config, environment)
        #getStorage(config) BaseStorage
        #getClientId(config) String
        #getFarmId(config) String
        #getCuratorFramework() CuratorFramework
        #getSettings(config) IgnisMQSettings
    }

    class IgnisMQManager {
        -Map~String, IQueue~ ignisMQMap
        +createQueue(CreateQueueRequest)
        +getQueue(name) IQueue
    }

    class IQueue~M~ {
        <<sealed interface>>
        +publish(M message) boolean
        +getUnconsumedCount() long
        +getMetaData() QueueMetaData
        +shovel(int concurrency)
    }

    class MagazineQueue~M~ {
        -Magazine~String~ magazine
        -Magazine~String~ sidelineMagazine
        -MessageHandler~M~ messageHandler
        -List~ScheduledFuture~ consumers
        -List~ScheduledFuture~ sidelineConsumers
        -HandlerExecutor handlerExecutor
        -ObjectMapper mapper
        +publish(M message) boolean
        +createConsumers(int count)
        +stopConsumers(int count)
    }

    class Magazine~T~ {
        -BaseMagazineStorage~T~ baseMagazineStorage
        -String magazineIdentifier
        +load(T data) boolean
        +fire() MagazineData~T~
        +delete(MagazineData~T~ magazineData)
    }

    class MagazineConsumerTask~M~ {
        -Magazine~String~ magazine
        -Magazine~String~ sidelineMagazine
        -MessageHandler~M~ messageHandler
        -HandlerExecutor handlerExecutor
        +run()
    }

    class ShovelTask {
        -Magazine~String~ magazine
        -Magazine~String~ sidelineMagazine
        +run()
    }

    class TaskInitializer {
        -LeaderElector leaderElector
        -ScheduledFuture sweeperTask
        +start()
        +stop()
    }

    class LeaderElector {
        -LeaderSelector leaderSelector
        -CuratorFramework curatorFramework
        +takeLeadership()
        -updateState(boolean force)
    }

    class Sweeper {
        <<implements LoadBalancer>>
        -AtomicBoolean active
        -QueueSweeper queueSweeper
        +activate()
        +deactivate()
        +run()
    }

    class QueueStatGuage {
        <<Supplier~List~>>
        +get() List
    }

    class QueueService {
        <<sealed interface>>
        +exists(String name) boolean
        +get(String name) Optional~QueueEntity~
        +store(String name, QueueEntity entity, int ttl)
        +getQueues(boolean active) Map~String, QueueEntity~
        +updateSweepProgress(...)
    }

    class AerospikeQueueService {
        -IAerospikeClient client
        -String namespace
        -String setName
    }

    class AerospikeStoreClient {
        -IAerospikeClient client
        -AerospikeConfiguration config
    }

    IgnisMQBundle --> IgnisMQManager : creates
    IgnisMQManager o-- IQueue : ConcurrentHashMap
    IQueue <|.. MagazineQueue : permits
    MagazineQueue *-- Magazine : main
    MagazineQueue *-- Magazine : sideline
    MagazineQueue *-- MagazineConsumerTask : consumers
    MagazineQueue *-- ShovelTask : sidelineConsumers
    MagazineConsumerTask --> Magazine : reads from main
    MagazineConsumerTask --> Magazine : writes to sideline
    ShovelTask --> Magazine : reads from sideline
    ShovelTask --> Magazine : writes to main
    TaskInitializer --> LeaderElector : creates
    TaskInitializer --> Sweeper : creates
    LeaderElector ..> Sweeper : activates/deactivates
    QueueService <|.. AerospikeQueueService : permits
    AerospikeQueueService --> AerospikeStoreClient
    IgnisMQManager --> QueueStatGuage : reads
    IgnisMQBundle --> QueueStatGuage : exposes as CachedGauge
```

!!! note "ConcurrentHashMap"
    `IgnisMQManager` holds its queues in `ignisMQMap`, a `Map<String, IQueue<?>>` backed by a `ConcurrentHashMap`. This allows thread-safe registration and lookup at runtime.

---

## 3. Threading Model

!!! warning "Rewritten in 2.0"
    IgnisMQ used to run one `java.util.Timer` thread per consumer, per shovel, plus one each for the
    watcher and sweeper. A queue with 8 consumers and 2 shovels cost 10 threads on scheduling alone.
    Everything is now a task on one of three shared pools.

### The three pools

| Pool | Size | Runs | Isolation rationale |
|---|---|---|---|
| **Control** | Fixed at 2 | Queue watcher, sweeper | The control plane. The watcher is how a queue created elsewhere becomes consumable here; the sweeper is the only thing that rescues messages from a dead consumer. Neither may be starved by the data plane it supervises |
| **Worker** | Grows to `workerThreads` (256) | Consumers, shovels | Shovels belong here, not with the watcher: they are per-queue, configurable and do storage I/O — the same class of work as a consumer. A third pool would add threads and a knob without buying isolation that matters |
| **Handler** | Up to `workerThreads` | User `MessageHandler` code only | The only way to bound how long a worker waits for arbitrary user code is to not run it on that thread |

Sharing one pool coupled control with data: enough slow consumers and the watcher silently stops
refreshing queues while the sweeper stops recovering orphans.

### Schedule summary

| Component | Scheduled on | Initial delay | Period |
|---|---|---|---|
| **Consumer task** | Worker | 1 s | 1 s fixed delay |
| **Shovel task** | Worker | 1 s | `timeIntervalInSecs`, default 600 s |
| **Queue watcher** | Control | 1 min | 5 min |
| **Sweeper** | Control | 10 min | 15 min |
| **Leader election** | Its own Curator threads | — | 30 s state poll |
| **Sweep fan-out** | Fixed pool, `PARALLEL_FACTOR = 64` | — | Per pass, across queues |

Fixed **delay**, not fixed rate: the gap is measured after a task finishes, so a slow consumer never
has runs queued up behind it.

### What bounds a thread

The split bounds the blast radius of a badly behaved handler; it does not make the worker pool immune
to one. Three mechanisms do that:

| Mechanism | Value | Closes |
|---|---|---|
| **Consumer run budget** | 30 s | A backlogged queue holding a worker thread forever. The consumer yields and resumes next run; a batch in hand is always finished first, so the budget never costs a delivery |
| **Handler timeout** | `handlerTimeoutInMins`, clamped to half of `sweepDuration` | A hung handler holding a worker, and the duplicate-processing window where the sweeper re-homes a record the handler is still working on |
| **Saturation refusal** | 1 s grace, then refuse | Running a batch inline with no bound when the handler pool is exhausted. The batch is sidelined under `reason=saturated` instead |

!!! note "`concurrency` is a target, not a thread count"
    For a queue with `concurrency = 8` and `shovelConcurrency = 2`, IgnisMQ schedules **10 tasks**
    that share the worker pool — not 10 threads. Size `workerThreads` as concurrently active queues
    times their concurrency.

---

## 4. Data Flow

### 4.1 Publish Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant IQ as IQueue.publish()
    participant OM as ObjectMapper
    participant Mag as Main Magazine
    participant MS as BaseMagazineStorage
    participant AS as Aerospike

    App->>IQ: publish(message)
    IQ->>OM: writeValueAsString(message)
    OM-->>IQ: JSON string
    IQ->>Mag: load(json)
    Mag->>MS: write slot, advance pointer
    MS->>AS: put(key, bins)
    AS-->>MS: ok
    MS-->>Mag: ok
    Mag-->>IQ: true
    IQ-->>App: boolean
```

### 4.2 Consume Flow

```mermaid
sequenceDiagram
    participant T as Worker pool
    participant CT as MagazineConsumerTask
    participant M as Main Magazine
    participant MS as BaseMagazineStorage
    participant AS as Aerospike
    participant H as MessageHandler

    T->>CT: run()
    CT->>M: fire()
    M->>MS: claim next pointer, read slot
    MS->>AS: operate(meta) + get(data)
    AS-->>MS: Record
    MS-->>M: Record
    M-->>CT: MagazineData
    CT->>CT: deserialize(JSON → M)
    CT->>H: handle(List) on the handler pool, with a timeout

    alt Handler returns true
        H-->>CT: true
        CT->>M: delete(record)
        M->>MS: delete(slot)
        MS->>AS: delete(key)
    else Returns false, throws, times out, or the pool is saturated
        H-->>CT: failure
        CT->>CT: sidelineMagazine.load(payload)
        alt Sideline accepted it
            CT->>M: delete(record)
        else Sideline refused it
            CT->>CT: leave the record for the sweeper
        end
    end
```

### 4.3 Shovel Flow

```mermaid
sequenceDiagram
    participant T as Worker pool
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
    participant Sched as Control pool
    participant SW as Sweeper
    participant FJP as Sweep pool (fixed, 64)
    participant QS as QueueService
    participant M as Main Magazine
    participant SL as Sideline Magazine

    Sched->>SW: run()
    SW->>QS: getQueues(active=true)
    QS-->>SW: Map<String, QueueEntity>

    loop For each queue (parallel, capped at 64)
        SW->>FJP: submit(sweepTask)
        FJP->>M: firePointerBefore(now - sweepDuration)
        M-->>FJP: per-shard watermark, or absent
        loop Until each shard reaches its watermark
            FJP->>M: peek(pointers, batched across shards)
            M-->>FJP: records still present = abandoned
            FJP->>SL: load(payload)
            FJP->>M: delete(record) only if the load succeeded
        end
        FJP->>QS: updateSweepProgress(pointers, swept)
    end
```

!!! tip "How the sweeper knows a message is old enough"
    It does not read a timestamp from the message — there is no longer one to read. Magazine reports
    where each shard's fire pointer stood at a past moment, so everything at or below that pointer was
    claimed at least `sweepDuration` ago. See
    [delivery-time watermarks](backends/aerospike.md#delivery-time-watermarks).

    If the retained checkpoint history does not reach back far enough, the sweeper **refuses the pass
    and logs an error** rather than guessing. Alert on `ignismq.sweep{outcome=failure}`.

---

## 5. Sealed Type Hierarchy

IgnisMQ uses Java 17 sealed types to enforce a closed set of implementations.

```mermaid
classDiagram
    class IQueue~M~ {
        <<sealed interface>>
        +publish(M message) boolean
        +getUnconsumedCount() long
        +getMetaData() QueueMetaData
        +shovel(int concurrency)
    }
    class MagazineQueue~M~ {
        <<final>>
    }
    IQueue <|.. MagazineQueue : permits

    class QueueService {
        <<sealed interface>>
        +exists(String name) boolean
        +get(String name) Optional~QueueEntity~
        +store(String name, QueueEntity entity, int ttl)
        +updateSweepProgress(...)
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
        -AerospikeConfiguration configuration
        -String namespace
    }
    BaseStorage <|-- AerospikeStorage : permits

    class StorageClient~T~ {
        <<interface>>
        +getClient() T
        +stop()
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
        -AerospikeConfiguration configuration
        -String namespace
        +accept(StorageVisitor~T~ visitor) T
    }

    class StorageVisitor~T~ {
        <<interface>>
        +visit(AerospikeStorage storage) T
    }

    class MagazineStorageVisitor {
        <<final, top-level>>
        +visit(AerospikeStorage storage) BaseMagazineStorage~String~
    }

    class QueueServiceVisitor {
        <<anonymous, inside IgnisMQManager>>
        +visit(AerospikeStorage storage) QueueService
    }

    class StoreClientVisitor {
        <<anonymous, inside IgnisMQManager>>
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
2. To build a `QueueService`, it calls `storage.accept(...)` with an anonymous `StorageVisitor`.
3. `AerospikeStorage.accept()` dispatches to `visitor.visit(this)`.
4. The visitor has access to the concrete `AerospikeStorage` fields and constructs `AerospikeQueueService`.

!!! note "Two of the three visitors have no name"
    `QueueServiceVisitor` and `StoreClientVisitor` above are labels for readability, not types — in
    source both are anonymous `new StorageVisitor<>() { ... }` expressions inside
    `IgnisMQManager.buildQueueCommands` and `buildStorageClient`. Only `MagazineStorageVisitor` is a
    real named type, and it is **top-level rather than nested in `MagazineQueue`**: the sweeper needs
    the same storage recipe, and having a queue type own it made the sweep package depend back on the
    queue package. That is a genuine cycle, and the architecture rules reject it.

The `Magazine` instances themselves are built through `MagazineStorageVisitor`, which returns a
`BaseMagazineStorage<String>` — the storage a `Magazine` is constructed on, not a `Magazine`.

!!! note "Extensibility"
    To add a new storage backend (e.g., Redis), you would: (1) create `RedisStorage extends BaseStorage`, (2) add `visit(RedisStorage)` to `StorageVisitor`, (3) implement the visitor methods. The sealed hierarchy ensures you cannot forget any call site.

---

## 7. Metrics

Core records metrics through Micrometer. `ignismq-dw-bundle` bridges them into the application's
Dropwizard `MetricRegistry` and supplies Dropwizard-specific gauges and lifecycle adapters.
`ignismq-core` has no Dropwizard dependency at any scope, and an enforcer rule fails the build if one
appears.

### Registered metrics

Full definitions, tags and semantics: **[Metrics](api/metrics.md)**. In outline:

| Meter family | Covers |
|---|---|
| `ignismq.publish`, `ignismq.consume`, `ignismq.messages`, `ignismq.poll`, `ignismq.sideline` | The message path |
| `ignismq.handler.*` | Duration, batch size, timeouts, saturation |
| `ignismq.queue.*` | Depth and lifetime counters, as gauges from one cached snapshot |
| `ignismq.sweep*`, `ignismq.shovel*`, `ignismq.queue.refresh`, `ignismq.queue.create` | Control plane |
| `ignismq.pool.*` | Control and worker pool saturation |
| `magazine.*` | Magazine's own instrumentation, tagged by magazine and outcome |
| `ignis.queue.stats` | Bundle only: a Dropwizard `CachedGauge`, 3 minute TTL |

Every dimension is a tag rather than part of the name, so a queue is a tag value and not a new meter.

Dropwizard has no notion of tags, so the bundle's bridge flattens each Micrometer tag into the metric
name. `magazine.fire.outcomes` tagged `magazine=orders, outcome=delivered` is published as
`magazine.fire.outcomes.magazine.orders.outcome.delivered`.

### The queue-stat gauges

```mermaid
flowchart LR
    A[Metrics scrape] --> B[Queue depth gauges]
    B --> C{Snapshot fresh?}
    C -->|Yes| D[Serve from the cached snapshot]
    C -->|No| E[One metadata read per queue]
    E --> F[published, consumed, unconsumed,<br/>sidelined, shovelled]
    F --> D
```

Depth is the one value that costs a storage read, so all five gauges for a queue share a single
refresh rather than each triggering their own. Without that, a scrape would cost one metadata read
per queue **per meter**.

### What happened to `commands.*`

Earlier versions wove `@MonitoredFunction` timers into most public methods with AspectJ. That was
removed along with the Dropwizard coupling: it pulled AspectJ and a build-time weaving step into
`ignismq-core`, and the resulting timers measured method entry and exit rather than anything
operationally meaningful.

The two that were worth keeping — publish and consume latency — are now `ignismq.publish` and
`ignismq.consume`, with the queue as a tag. **The rest of the `commands.*` namespace is gone**, and
nothing is aliased: see the [upgrade notes](upgrading.md).

---

## 8. ZooKeeper Node Structure

IgnisMQ uses ZooKeeper (via Apache Curator) for leader election and work distribution among instances.

```
/{clientId}-ignis-workers/
  └── {clientId}/
      ├── loadbalancer-leader          ← LeaderSelector (persistent node)
      │                                  Only one instance holds leadership.
      │                                  The leader assigns the sweep
      │                                  partition; the assigned instance
      │                                  runs the Sweeper, leader or not.
      │
      ├── loadbalancer/
      │   └── {balancerId}             ← Ephemeral nodes, named by the
      │                                  instance's balancerId UUID.
      │                                  Not sequential: the UUID is
      │                                  already the identity.
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

    READERS --> R1["1<br/>assigned: balancerId-002"]

    style LEADER fill:#f9a825,stroke:#f57f17
    style LB1 fill:#81d4fa,stroke:#0277bd
    style LB2 fill:#81d4fa,stroke:#0277bd
    style LBN fill:#81d4fa,stroke:#0277bd
```

**Leader election flow:**

1. Each instance creates an ephemeral node under `loadbalancer/`.
2. Curator's `LeaderSelector` competes for `loadbalancer-leader`.
3. The winning instance calls `takeLeadership()`, which forces an immediate reassignment pass. Leadership is what makes an instance the *assigner*; it is not what activates a sweeper.
4. The leader reassigns partitions across live balancer nodes by round-robin, writing each assignment into that partition's node under `readers/`. It rewrites them only when membership changes, or on the forced pass taken at step 3.
5. Every instance **polls** its assignment every 30 seconds - these nodes are read, not watched - and the instance whose `balancerId` matches activates its `Sweeper`. That may or may not be the leader, and activation can lag a reassignment by up to 30 seconds.
6. When the leader dies, its ephemeral node disappears and Curator triggers a new election.

!!! warning "ZooKeeper Dependency"
    ZooKeeper is required for multi-instance deployments. In single-instance mode, the instance always becomes the leader. If ZK is unavailable, sweeping and partition rebalancing will stall — but publish and consume continue to work.
