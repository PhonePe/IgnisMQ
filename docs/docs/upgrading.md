# Upgrading to 2.0

2.0 is a breaking release. Nothing here is accidental, but several changes are silent — the code
still compiles, or the configuration still loads, and the behaviour is different. Those are marked
**silent** and are the ones worth reading carefully.

## At a glance

| Area | Change |
|---|---|
| Metrics | The whole `commands.*` namespace is gone, replaced by tagged `ignismq.*` meters |
| Metrics API | Core takes a Micrometer `MeterRegistry`, not a Dropwizard `MetricRegistry` |
| Lifecycle | `IgnisMQManager.start()` is deleted; `Managed` is no longer implemented by core types |
| Threading | Consumers are pooled tasks, not one `Timer` thread each. **`concurrency` changes meaning** |
| Handlers | Handler execution is now bounded by a timeout, and refused under saturation |
| Storage | The per-message `fireTS` bin and `addFireTimestamp()` are gone |
| Correctness | A message is no longer deleted when the transfer to the sideline failed |
| Configuration | New `IgnisMQSettings`; new `handlerTimeoutInMins` on a queue |

---

## Constructors and lifecycle

`IgnisMQManager` now takes an `IgnisMQSettings` as its final argument. Pass `null` for defaults.

```java
// 1.x
new IgnisMQManager(clientId, storage, mapper, metricRegistry, curator, farmId);

// 2.0
new IgnisMQManager(clientId, storage, mapper, meterRegistry, curator, farmId,
        IgnisMQSettings.defaults());
```

**`manager.start()` is deleted.** The constructor always built the storage client, so the call was
already doing nothing. Remove it.

**`Managed` is gone from `IgnisMQManager`, `TaskInitializer` and `AerospikeStoreClient.`** Core no
longer depends on Dropwizard at any scope, and an enforcer rule now fails the build if that
regresses. If you were registering these with Dropwizard's lifecycle directly, use
`ignismq-dw-bundle`, which does it for you.

**`stop()` now releases the scheduler pools**, and leaves a `StorageClient` you supplied open — the
manager only closes what it built.

## Metrics

Core accepts a Micrometer `MeterRegistry`. The Dropwizard bridge lives in `ignismq-dw-bundle`, so a
Dropwizard application passes `environment.metrics()` to the bundle and gets the same metrics
bridged back.

| 1.x | 2.0 |
|---|---|
| `commands.{queue}_publish.all` | `ignismq.publish` with `queue` and `outcome` tags |
| `commands.{queue}_consume.all` | `ignismq.consume` with a `queue` tag |
| The rest of `commands.*` | Deleted. They were method-level woven timers, replaced by [deliberate instrumentation](api/metrics.md) |

**Dashboards and alerts must be rewritten.** The names are not aliased, deliberately: leaving the old
namespace half-populated is worse than removing it.

`metricsEnabled(false)` silences `ignismq.*` **and Magazine's `magazine.*` meters**. In an earlier
draft it left Magazine's alone; that was reversed, because a caller cannot be asked to configure a
transitive dependency they never chose.

## Threading — the silent one

!!! danger "`concurrency` is now a target, not a guarantee. **Silent.**"
    Under 1.x, `concurrency: 4` meant four `java.util.Timer` threads dedicated to that queue. It now
    means four scheduled tasks competing for a shared, capped worker pool. Nothing warns you.

    **Size `workerThreads` as concurrently-active-queues x their concurrency.** The default is 256. A
    deployment with many busy queues that previously ran on thread-per-consumer may need it raised;
    one with many idle queues will use far fewer threads than before.

The 2-minute cold start before the first consume is also gone — consumers now start after 1 second.

## Handler execution — the other silent one

!!! danger "Handlers are now bounded. **Silent.**"
    A handler that exceeds `handlerTimeoutInMins` (default 10) has its worker thread taken back and
    **its message sidelined**. The handler may still be running: cancellation only interrupts.

    In 1.x a hung handler held its thread forever, and after `sweepDuration` the sweeper would
    sideline and delete the record *while the handler was still working on it*. The timeout is
    clamped to at most half of `sweepDuration` precisely to close that window.

    If you have handlers that legitimately run for a long time, set `handlerTimeoutInMins` before
    upgrading, and check it against `sweepDurationInMins`.

**Saturation refuses rather than running inline.** If no handler thread is free within one second,
the batch is sidelined under `reason=saturated` and never runs. This sheds work visibly instead of
silently losing the bound. Alert on `ignismq.handler.executions{mode=refused}`.

## Storage format

**The `fireTS` bin is gone, and so is `addFireTimestamp()`.** The sweeper now works from shard-level
[delivery-time watermarks](backends/aerospike.md#delivery-time-watermarks) instead of a per-message
timestamp, which removes one Aerospike write per delivery and one bin per record.

Existing records keep their stale `fireTS` bin; nothing reads it. It disappears as records expire.

**New bin: `handlerTimeout`.** Queues created before 2.0 do not have it, and fall back to the
**default** — not to zero. This matters more than it looks: zero would be clamped to a 1 ms floor and
every batch on every pre-existing queue would time out instantly.

## Correctness changes you are unlikely to object to

- **A message is no longer deleted when the sideline transfer failed.** In 1.x the source record was
  deleted regardless, so a failing sideline destroyed the only copy. It is now left for the sweeper.
  The same fix applies to the shovel and the sweeper.
- **The sweeper no longer sweeps unfired records.** It previously read a fixed 1,000-key window
  without stopping at the fire pointer and treated a missing timestamp as `0`.
- **Batching no longer waits when a full batch is available.** A `<=` that should have been `<` meant
  a complete batch slept for the full linger period before being handed over.
- **Validation moved from Dropwizard's `@ValidationMethod` to `@AssertTrue`.** Behaviour is the same,
  but the **violation property path changes**, which is visible to Jersey clients parsing validation
  errors.

## Recommended order

1. Raise `handlerTimeoutInMins` for any queue with slow handlers, and check it against
   `sweepDurationInMins`.
2. Set `workerThreads` from your real concurrency, rather than accepting the default blindly.
3. Rewrite dashboards and alerts against [the new meters](api/metrics.md) and the
   [runbook](operations/monitoring.md).
4. Deploy, and watch `ignismq.handler.timeouts`, `ignismq.handler.executions{mode=refused}` and
   `ignismq.sweep{outcome=failure}` first. Those three catch every silent change above.
