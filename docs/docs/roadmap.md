# Roadmap

What IgnisMQ does not do yet, why, and what it would take. Nothing here is committed work — this page
exists so that you can tell quickly whether IgnisMQ is a fit, and so that a feature request starts
from facts rather than from guesswork.

If you need one of these, say so. Several are cheap and are unbuilt only because nobody has asked.

---

## Already supported, sometimes mistaken for missing

### Publish now, consume later

A queue created with `concurrency: 0` accepts publishes and runs no consumers. Call
`increaseConsumers(name, n)` to start consumption, and back to `0` to pause it. The count is
persisted, so it applies across the deployment rather than to one process.

This covers publish-only queues, deliberately paused queues, and manually triggered draining. See
[Publishing now, consuming later](usage.md#publishing-now-consuming-later).

!!! warning "Pausing does not suspend expiry"
    Messages still expire `messageExpiry` after they were published, consumed or not. Pausing for
    longer than that loses them.

### Scaling consumers at runtime

`increaseConsumers` / `decreaseConsumers`, or the console, up to 100 per queue. No restart, and the
change propagates.

### Competing consumers

Many consumers on one queue, each message delivered to exactly one of them. This is the default and
what `concurrency` means.

---

## Not supported, and the honest reasons

### Fan-out to independent consumers (Kafka-style consumer groups)

**Wanted:** one publisher, several independent readers, each receiving every message.

**Not available today, and the reason is a deliberate design choice rather than a hard limit.** A
queue keeps a **single fire pointer** per shard, and claiming a message is a destructive
compare-and-set. That is what competing consumers need, and it is exactly what makes them scale:
two readers of the same queue share the work rather than each getting a copy.

Fan-out is a different shape, and it is reachable. Several fire pointers can be maintained over the
same data — one per consumer group — so each group advances independently over every message. The
piece that has to change alongside it is the **explicit delete**: today a message is deleted once
its single reader is done, which is correct for one pointer and wrong for several. With independent
groups, a message can only be retired once every group has passed it, so retirement becomes a
function of the slowest pointer and TTL rather than of any one consumer.

Nobody has asked for it, so it is not built. It is not off the table either — it is a real amount of
work across both IgnisMQ and its storage engine, and it can be brought into scope if there is a use
case for it.

Duplicating each message into N queues at publish time works today. It costs N times the storage and
fixes the subscriber set at publish time, so a reader added later sees no history.

### Retry with backoff, and a maximum delivery count

A handler returning `false` sidelines the message on the **first** failure. There is no attempt
count, no delay, and no automatic redelivery — a sidelined message comes back only when a shovel runs.

This is the largest functional gap, and it is blocked on a message envelope (below).

### Scheduled or delayed delivery

Messages become visible when published. There is no "deliver this in thirty minutes".

### Triggered start of consumption

Consumption starts when you call `increaseConsumers` or press the button in the console. Starting it
on a schedule, on a queue-depth threshold, or on an external event is not built in — you would drive
it from your own scheduler or alerting.

Of these, a **depth threshold** is the only one worth building into IgnisMQ, because it is the only
one that needs data the library alone has. Time-based scheduling is something your deployment already
has.

### Per-message inspection

You cannot browse the sideline, search for a message, or ask how old the oldest message is. A
sidelined message records no reason, no attempt count and no enqueue time, so a browser could only
show payloads.

### Ordering guarantees

None. Shards are chosen at random on publish, and consumers race. Per-key ordering would need
key-based shard routing, which is cheap to add but does not exist.

### Exactly-once delivery

IgnisMQ is **at-least-once** and the sweeper's design depends on it. Use idempotent handlers.

### Priority

A sharded pointer ring delivers in pointer order by construction. Priority would mean either several
rings per queue or a scan — and the first is just "use two queues".

---

## The change that unblocks most of this

Today a message **is** its payload. There is nowhere to record how many times it has been attempted,
when it was enqueued, why it failed, what trace it belongs to, or what type it is.

That single absence is what blocks retry, sideline diagnostics, delayed delivery, per-message TTL,
trace propagation, message-age metrics and every per-message console view. A message envelope is a
**wire-format change**, so it is done at a major version or it is done with a migration story.

---

## Configuration IgnisMQ currently decides for you

These are Magazine settings that IgnisMQ fixes. Each is a reasonable default; none is currently
yours to change. Listed because a limit you cannot see is worse than one you can.

| Setting | Fixed at | What it affects |
|---|---|---|
| Deduplication | Off | Publish-side suppression of identical payloads |
| Active-shard refresh | 5 seconds | Steady-state read load, and wake-up latency on an idle queue |
| Active-shard cache size | 1024 entries | How many queues one process can hold before it re-reads shard information. See below |
| Fire-history recording | Always on | A small write cost on every queue, including ones that never sweep |
| Fire-history depth | 32 checkpoints | How far back the sweeper can safely reach |
| Metadata TTL | 2 x queue expiry | How long a queue's bookkeeping outlives its messages |
| Shard count after creation | Immutable | A changed `shards` value on an existing queue is ignored |

---

## A limit worth knowing about

Active-shard information is cached per magazine, capped at 1024 entries, and IgnisMQ uses two
magazines per queue. A single process holding more than roughly **500 queues** will start evicting
and re-reading that information. Nothing breaks; the read rate rises. No deployment has reached this,
and it is stated here because it is not obvious from anything else.

The cap is currently a **fixed constant rather than a setting** — unlike the refresh interval
listed above it, which is already a parameter. Making it configurable is a small change, and an
obvious one to take if any deployment gets close to the limit.
