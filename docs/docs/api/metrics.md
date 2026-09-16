# Metrics

Every meter IgnisMQ publishes, what it measures, and — just as important — what it does not.

!!! warning "Changed in 2.0"
    The old `commands.{queueName}_publish.all` / `commands.{queueName}_consume.all` timers are gone,
    along with the rest of the `commands.*` namespace. See the [upgrade notes](../upgrading.md).

## Naming

```
ignismq.<area>.<measurement>
```

Every dimension is a **tag**, never part of the name, so a queue is a tag value rather than a new
meter. Tag values come from closed sets, so cardinality is `queue count x a constant`.

| Tag | Values |
|---|---|
| `queue` | Queue name |
| `outcome` | `success`, `failure`, `acked`, `sidelined`, `ignored`, `timeout`, `saturated`, `retried`, `fatal` — depends on the meter |
| `reason` | `rejected`, `exception`, `timeout`, `saturated`, `sideline_refused` |
| `result` | `message`, `empty` |
| `magazine` | `main`, `sideline` |
| `pool` | `control`, `worker` |
| `mode` | `pooled`, `refused` |

IgnisMQ also passes its `MeterRegistry` to Magazine, so `magazine.*` meters appear alongside these.

## The message path

| Meter | Type | Tags | Measures |
|---|---|---|---|
| `ignismq.publish` | Timer | `queue`, `outcome` | One `publish()` call: serialisation plus the storage write |
| `ignismq.consume` | Timer | `queue` | **Processing one batch after it has been claimed.** See the scope note below |
| `ignismq.messages` | Counter | `queue`, `outcome` | Terminal disposition per message: `acked`, `sidelined` or `ignored` |
| `ignismq.poll` | Counter | `queue`, `result` | Whether a poll returned work. A high `empty` rate means consumers are over-provisioned for the traffic |
| `ignismq.sideline` | Counter | `queue`, `reason` | Why a message was sidelined, including `sideline_refused` |
| `ignismq.consumer.budget.exhausted` | Counter | `queue` | A consumer handed its thread back with work still waiting |

!!! important "What `ignismq.consume` does not include"
    It starts **after** `fire()` has returned a batch, so it excludes the poll that claimed the
    messages. It covers deserialisation, the handler call, and the resulting deletes or sideline
    transfers.

    This matters when you compare it against `ignismq.handler.duration`: the difference between the
    two is deserialisation plus delete/sideline I/O, **not** the time spent fetching. To see fetch
    cost, use Magazine's own meters.

## The handler

| Meter | Type | Tags | Measures |
|---|---|---|---|
| `ignismq.handler.duration` | Timer | `queue`, `outcome` | The handler call alone. `outcome` is `success`, `failure`, `timeout` or `saturated` |
| `ignismq.handler.batch.size` | Summary | `queue` | Messages handed over per call. **Published only by queues that batch** — its absence is the honest signal that a queue does not, since a constant 1 would be indistinguishable from a batching consumer that never fills |
| `ignismq.handler.executions` | Counter | `mode` | `pooled` ran; `refused` never ran because no handler thread was free |
| `ignismq.handler.timeouts` | Counter | `queue` | Handler calls that exceeded `handlerTimeoutInMins` |
| `ignismq.handler.threads.active` | Gauge | — | Handler threads currently in use |

## Queue state

Gauges, refreshed together from one cached snapshot so that a dashboard scrape does not turn into a
metadata read per queue per meter.

| Meter | Measures |
|---|---|
| `ignismq.queue.depth` | Backlog: published minus consumed |
| `ignismq.queue.published` | Lifetime messages published |
| `ignismq.queue.consumed` | Lifetime messages consumed |
| `ignismq.queue.sidelined` | Messages currently in the sideline queue |
| `ignismq.queue.shovelled` | Messages moved back out of the sideline |
| `ignismq.queue.consumers` | Consumers this process is running for the queue |

!!! note "Deactivated queues stop reporting"
    Their gauges are removed rather than frozen at a last value, so a deactivated queue cannot hold
    an alert open forever.

## Control plane

| Meter | Type | Tags | Measures |
|---|---|---|---|
| `ignismq.queue.create` | Timer | `outcome` | Queue creation |
| `ignismq.queue.refresh` | Timer | `outcome` | One watcher pass reconciling queues against the database |
| `ignismq.sweep` | Timer | `queue`, `magazine`, `outcome` | One sweep pass over one magazine. **Failures are timed too** — refusing immediately and dying half way through a scan are different faults with the same outcome tag |
| `ignismq.sweep.rehomed` | Counter | `queue`, `magazine` | Abandoned messages moved to the sideline |
| `ignismq.shovel` | Timer | `queue`, `outcome` | One shovel pass |
| `ignismq.shovel.moved` | Counter | `queue` | Messages the shovel re-homed. Counts the re-append-to-sideline fallback too, so it is "moved somewhere safe", not strictly "moved to main" |

## Pools

Tagged `pool=control` or `pool=worker`. The handler pool reports through the `ignismq.handler.*`
meters above instead.

| Meter | Type | Measures |
|---|---|---|
| `ignismq.pool.threads` | Gauge | Current pool size |
| `ignismq.pool.threads.max` | Gauge | Ceiling this pool may grow to |
| `ignismq.pool.tasks.active` | Gauge | Tasks currently executing |
| `ignismq.pool.tasks.due` | Gauge | **Overdue** tasks, not queued ones. Every idle periodic task sits in the queue awaiting its next due time, so a raw queue size cannot tell idle from saturated |
| `ignismq.pool.task.wait` | Timer | Time a task spent waiting for a thread after becoming due |
| `ignismq.pool.task.failures` | Counter | Tasks that threw. Additionally tagged `outcome` |

`ignismq.pool.task.failures` is the one pool meter with a second tag, and the two values mean very
different things:

| `outcome` | Meaning |
|---|---|
| `retried` | The task threw an `Exception`. It is still scheduled and will run again |
| `fatal` | The task threw an `Error`. **Its schedule is gone until the process restarts** — rescheduling through an `OutOfMemoryError` would loop on a condition that cannot clear, so it is deliberately not retried |

A single `outcome=fatal` is therefore a restart-level event, not a rate to watch.

## Turning metrics off

```java
IgnisMQSettings.builder().metricsEnabled(false).build();
```

This silences every `ignismq.*` meter **and Magazine's `magazine.*` meters**, because a caller cannot
reasonably be asked to configure a transitive dependency they never chose. Every call site keeps
working — recordings go to a sink — so disabling instrumentation never becomes a second code path.

## Dropwizard

`ignismq-dw-bundle` bridges all of the above into the application's `MetricRegistry`, and adds one
Dropwizard-native gauge:

| Metric | Notes |
|---|---|
| `ignis.queue.stats` | Cached for 3 minutes, because each load issues metadata reads per queue |
