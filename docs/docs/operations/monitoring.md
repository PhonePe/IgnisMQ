# Monitoring Runbook

What to alert on, what each alert means, and what to do about it. Meter definitions are in
[Metrics](../api/metrics.md).

IgnisMQ publishes into whatever `MeterRegistry` you hand it, and passes the same registry to
Magazine, so `ignismq.*` and `magazine.*` arrive together. Several of the most valuable signals are
Magazine's.

---

## Alert on these

### 1. Sweep failures — `ignismq.sweep{outcome=failure} > 0`

**Severity: high.** The sweeper is the only thing that rescues messages claimed by a consumer that
died. While it is failing, those messages stay invisible until the queue's TTL deletes them.

The most likely cause is deliberate and loud: the retained checkpoint history no longer reaches back
to `now - sweepDuration`, so Magazine raises `INVALID_REQUEST` and the sweeper refuses the pass
rather than guessing which messages are abandoned. Increasing `sweepDurationInMins` without capacity
for the matching history, or a long process outage, both produce it.

**Check:** the error log names the magazine. Confirm `sweepDurationInMins` against Magazine's fire
history configuration.

!!! danger "Silence is not success"
    A sweeper that refuses every pass and a healthy queue with nothing to sweep look identical on a
    depth graph. This alert is the only thing that distinguishes them — do not drop it because it
    "never fires".

### 2. Fire exhaustion — `magazine.fire.outcomes{outcome=exhausted} > 0`

**Severity: high.** Consumers tried to claim work and gave up without getting any. Usually means
contention: too many consumers against too few shards.

**Check:** consumer count against shard count. Either reduce `concurrency` or create the queue with
more shards — shard count is fixed once persisted.

### 3. Sustained hole skips — `magazine.fire.hole.skips` climbing steadily

**Severity: medium.** A claim succeeded but the data record was not there. A few are normal — a hole
appears whenever a message expires by TTL before delivery. A sustained rate means messages are
expiring before consumers reach them, which is silent data loss from the caller's point of view.

**Check:** `messageExpiry` against actual queue latency, and `ignismq.queue.depth` for a backlog that
outlives the TTL.

### 4. Claims drained — `magazine.fire.claims{outcome=drained}` high relative to `won`

**Severity: medium.** Consumers are competing for the same shards and losing. The same diagnosis as
exhaustion, one step earlier.

### 5. Handler saturation — `ignismq.handler.executions{mode=refused} > 0`

**Severity: high.** No handler thread became available within the grace period, so the batch was
**refused and sidelined** under `reason=saturated`. It was never run. Because handler demand cannot
exceed worker demand, this normally means handler threads have leaked — a handler ignoring
interruption after a timeout will do it.

**Check:** `ignismq.handler.threads.active` against `ignismq.pool.threads.max`, and
`ignismq.handler.timeouts` for the leak's source. Fix the handler; raising `workerThreads` only
delays it.

### 6. Handler timeouts — `ignismq.handler.timeouts` non-zero

**Severity: medium.** A handler exceeded its budget. The worker thread was freed but **the handler may
still be running**, and the message has been sidelined — so the work may happen twice.

**Check:** `ignismq.handler.duration{outcome=success}` p99 against `handlerTimeoutInMins`. Either the
handler is too slow or the budget is too tight.

### 7. Sideline growth — `ignismq.queue.sidelined` rising without `ignismq.queue.shovelled` following

**Severity: medium.** Messages are failing and nothing is bringing them back. Either no shovel is
configured, or the shovel is failing too.

**Check:** `ignismq.sideline` by `reason` for *why* — that tag distinguishes a rejecting handler
(`rejected`), a throwing one (`exception`), timeouts, saturation, and a sideline that would not
accept the message at all (`sideline_refused`).

### 8. Sideline refused — `ignismq.sideline{reason=sideline_refused} > 0`

**Severity: high.** The sideline magazine would not take a message, so the record was deliberately
left in the main magazine for the sweeper. Nothing is lost, but the queue is degraded and delivery is
delayed by up to `sweepDuration`.

### 9. Storage call amplification — `magazine.aerospike.calls` per delivery, trending up

**Severity: low, but it is the efficiency signal.** Divide `magazine.aerospike.calls` by
`ignismq.messages{outcome=acked}` and watch the *trend* rather than an absolute figure. The 5.04
ratio quoted in the Magazine 2 comparison predates this release and included the per-message fire
timestamp write, which no longer exists, so it is an upper bound rather than a target. A ratio that
climbs with consumer count is the thing worth alerting on.

Watch `operation=refresh_active_shards` specifically: a high rate there means active-shard discovery
is running hotter than the traffic justifies.

### 10. Control-pool starvation — `ignismq.pool.tasks.due{pool=control} > 0` sustained

**Severity: high.** The control pool is fixed at two threads and runs the queue watcher and the
sweeper. If tasks are overdue there, queue changes are not being picked up and sweeps are not
running. This pool is deliberately isolated from user code, so a sustained value points at a stuck
sweep rather than at slow handlers.

### 11. Worker saturation — `ignismq.pool.threads{pool=worker}` at `ignismq.pool.threads.max`

**Severity: medium**, and the healthy interpretation first: the pool is *meant* to grow to its
ceiling. It matters when paired with a rising `ignismq.pool.task.wait` or
`ignismq.consumer.budget.exhausted`, which together mean consumers are queueing for threads.

**Check:** `workerThreads` should be sized as *concurrently active queues x their concurrency*, not
total configured consumers.

---

## Useful, but not alerts

| Signal | Reading |
|---|---|
| `ignismq.poll{result=empty}` high | Consumers are over-provisioned for the traffic. Costs polls, not correctness |
| `ignismq.consumer.budget.exhausted` | A consumer hit its 30 s budget with work left. Normal under load; sustained means the queue is under-consumed |
| `ignismq.handler.batch.size` well under `maxBatchSize` | Batches are flushing on the linger timer, not on size. Lower `maxBatchSize` or accept the latency |
| `ignismq.consume` ≫ `ignismq.handler.duration` | The gap is deserialisation plus delete and sideline I/O — not fetch time, which is outside the timer |
| `ignismq.queue.consumers` | Per process. Summing it across instances is how you see total consumers for a queue |

---

## A note on what these numbers describe

Everything under `ignismq.*` except the queue-state gauges is **per process**. Throughput, latency
and pool saturation describe the instance that published them. The queue-state gauges
(`ignismq.queue.*`) come from storage and are the same everywhere.

Aggregate across instances in your metrics backend, not in the application — IgnisMQ has no registry
of running processes and inventing one would mean building a control plane to duplicate what the
backend already does.
