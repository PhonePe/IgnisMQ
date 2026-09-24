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
| Bundle | `IgnisMQBundle`'s four abstract methods and one overridable collapse into one `context(T config)` |
| Handlers | **A queue without batching now reaches `handle(M)`.** It previously reached `handle(List<M>)` with a one-element list |
| Limits | The consumer and shovel caps are now reachable: 100, not 99 |
| Correctness | **A message that fails to deserialise is no longer sidelined twice**, and no longer deleted when its sideline write was refused |
| Handlers | **A `String` or primitive queue now receives its payload as published**, instead of JSON-encoded |
| Metrics | `ignismq.sideline` gains `reason=unreadable`, previously counted as `reason=exception` |
| Bundle | `IgnisMQException` now maps to a **4xx** where the caller is at fault, instead of a blanket `500` |
| Bundle | A console is mounted at `/ignismq/v1` and `/ignisConsole`. **New HTTP endpoints appear in your application** |

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

## String payloads arrive as published — the third silent one

!!! danger "A `String` or primitive queue now receives what was published. **Silent.**"
    POJO queues are unaffected, and always were correct.

`publish` has always JSON-encoded every payload, but the consumer decoded it again for every type
*except* `String` and the primitives — so the encoding was applied and never undone:

| Published | Delivered in 1.x | Delivered in 2.0 |
|---|---|---|
| `hello world` | `"hello world"` | `hello world` |
| `{"a":1}` | `"{\"a\":1}"` | `{"a":1}` |

Not just the surrounding quotes: full escaping was applied, so quotes, newlines and tabs came back
with backslashes the publisher never wrote.

**The wire format has not changed** — the fix is on the read side only. 1.x encodes identically, so
decoding on read leaves stored records untouched and retroactively fixes messages already in flight
or sidelined. Nothing needs draining, and a rollback is equally safe.

**What to do:** if a `String` handler compensated by stripping quotes or re-parsing, remove that or
it will now corrupt the message. Handlers that simply used the value need no change.

A literal `int.class` queue was separately broken — it threw `ClassCastException` on every message,
while boxed `Integer` always worked — and now behaves as `Integer.class` already did.

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
3. Check whether any queue's message type is `String` or a primitive, and if so whether its handler
   compensates for the old encoding. Nothing needs draining — only the handler may need editing.
4. Rewrite dashboards and alerts against [the new meters](api/metrics.md) and the
   [runbook](operations/monitoring.md).
5. Deploy, and watch `ignismq.handler.timeouts`, `ignismq.handler.executions{mode=refused}` and
   `ignismq.sweep{outcome=failure}` first. Those three catch every silent change above.


---

## The bundle takes a context, not five methods


`IgnisMQBundle` had four abstract methods plus an overridable `getSettings`. It now has one:

```java
// 1.x
protected BaseStorage getStorage(T config) { ... }
protected String getClientId(T config) { ... }
protected String getFarmId(T config) { ... }
protected CuratorFramework getCuratorFramework() { ... }
protected IgnisMQSettings getSettings(T config) { ... }   // optional

// 2.0
@Override
protected IgnisMQContext context(T config) {
    return IgnisMQContext.builder()
            .clientId(config.getClientId())
            .farmId(config.getFarmId())
            .storage(new AerospikeStorage(config.getAerospike(), namespace))
            .curatorFramework(curatorFramework)
            .build();
}
```

**This is a compile error, not a silent change**, which is the good case: your subclass will not
build until it is converted.

**What you lose, stated plainly:** the compiler no longer enforces that all four required values are
supplied. They are `@NonNull` on the builder instead, so a missing one throws when the bundle runs -
at application startup, on the first boot, rather than at compile time.

**Why it was worth it:** adding a fifth input previously meant adding an abstract method, which
breaks every subclass in every downstream service. It is now a new field on a builder, which breaks
nobody.

---

## A non-batching queue now calls `handle(M)`

`MessageHandler` has always declared both `handle(M)` and `handle(List<M>)`. Only the `List` overload
was ever called - including for queues with no `batchingConfig`, which received a one-element list.

**This is silent, and it is the one change on this page most likely to alter behaviour without a
compiler error.** If your handler put its real logic in `handle(List<M>)` and left `handle(M)`
throwing, returning `false`, or empty, a non-batching queue will now take that path.

```java
// Safe in both versions: one implementation, one delegation.
@Override
public boolean handle(OrderEvent message) {
    return process(message);
}

@Override
public boolean handle(List<OrderEvent> messages) {
    return messages.stream().allMatch(this::handle);
}
```

Queues **with** a `batchingConfig` are unaffected: they still receive `handle(List<M>)`.

---

## The consumer and shovel caps are reachable

`MAX_CONSUMERS_ALLOWED` is 100, and the guard rejected a request that *reached* it, so 99 was the
real maximum. `CreateQueueRequest.concurrency` is annotated `@Max(100)`, so `concurrency: 100`
passed validation and then failed at queue creation.

The guard now rejects only requests that exceed the cap. A queue may have 100 consumers and 100
shovels. Nothing that worked before stops working.

---

## A message that fails to deserialise is dealt with once

**Silent, and it fixes two defects on the same path.**

A payload that cannot be read is sidelined and deleted while the batch is being unpacked, before the
handler sees it. The rest of the batch then went to the handler — and the batch's own outcome was
afterwards applied to **every** record in it, including the one already dealt with. So:

- a rejected batch sidelined the poison message a **second** time, leaving two copies of it in the
  sideline for one delivery;
- an accepted batch **deleted** a poison message whose sideline write had been refused — and that
  record was the only copy of the payload left anywhere. That is the data-loss shape 2.0's
  transfer-then-delete rule exists to prevent, on the one path that bypassed it.

The batch outcome now applies only to the records that reached the handler. If nothing reached it —
a single-message queue whose one message was unreadable — the handler is not called at all, where it
previously received an empty list.

Nothing to do. If you counted on an empty `handle(List<M>)` call as a signal, it no longer arrives.

---

## Unreadable payloads have their own sideline reason

**Silent, and it will move numbers on an existing dashboard.**

`ignismq.sideline` gains a `reason` value: `unreadable`, for a payload that could not be turned into
the consumer's message type. Previously these were counted as `reason=exception`, identical to a
handler that threw.

The two mean different things and are fixed differently — `unreadable` is publisher and consumer
disagreeing about the format, usually a half-finished deploy, and the handler never ran at all. They
were not separable before.

**What to do:** any alert or dashboard filtering on `ignismq.sideline{reason="exception"}` will see
its rate drop, and the difference now appears under `unreadable`. If you alert on the total, use
`sum without (reason)` and nothing changes. Only the tag value is new; no meter was renamed and no
behaviour changed — the same messages are sidelined, under the same transfer-then-delete rule.

A handler that declares the deserialisation failure in `getIgnorableExceptions()` still has its
message dropped without any sideline at all, exactly as before.

---

## `IgnisMQException` no longer always means 500

**Silent, application-wide, and it will move your 5xx rate.**

The bundle registers an `ExceptionMapper` for `IgnisMQException`. Previously any one of them escaping
a Jersey resource produced a `500`; now the status follows the error code, and six of the nine codes
are the caller's fault. Asking to scale past the consumer cap, for instance, answered `500` and now
answers `409`.

The full table is in [Error codes](api/error-codes.md#over-http). The two that matter for alerting:
`AEROSPIKE_ERROR` becomes **`503`** rather than `500`, and `INTERNAL_ERROR` stays `500`.

**What to do:**

- **Alerts that count `500`s** will see the rate fall, and part of it reappear as `503`. If you alert
  on `5xx` as a class, nothing changes. If you alert on `500` exactly, widen it.
- **Clients that treat 4xx as permanent and 5xx as retryable now behave correctly** for these cases,
  which is the point — but a client that blindly retried everything will now stop retrying requests
  that cannot succeed. That is the intended change, and it is worth knowing before it happens.
- **A 5xx body produced by this mapper no longer carries the exception's message.** It is a fixed string, and the
  real message is logged. Anything scraping response bodies for detail should read the logs instead.

**This is registered even when the console is disabled**, because the exception is the library's
rather than the console's. To opt out entirely, do not use `IgnisMQBundle`'s registration - the mapper
is a plain `@Provider` you can choose not to register if you build your own wiring.

---

## The bundle mounts a console

**New endpoints appear in your application**, which is worth knowing before a deploy rather than
after:

- `GET /ignismq/v1/...` — read-only views of queues, per-shard depth and this instance
- `POST /ignismq/v1/...` — shovel, sweep and consumer scaling, requiring the `ignismq_operate` role;
  deactivation requires a **separate** `ignismq_deactivate` role plus a `confirm` parameter
- `/ignisConsole` — a dashboard page

The bundle also registers Jersey's `RolesAllowedDynamicFeature`, which was probably already
registered if you use `@RolesAllowed` elsewhere; registering it twice is harmless.

**The actions are closed unless your application grants the role.** The bundle defines no
authentication, so with no `SecurityContext` every `POST` returns `403` while the reads stay open.
If that is not the posture you want, turn the console off:

```java
.console(ConsoleConfiguration.builder().enabled(false).build())
```

`dashboardEnabled(false)` keeps the JSON endpoints without serving the page. Full detail in the
[console guide](operations/console.md).
