# Benchmark

Two separate measurements, taken against the shipping 2.0 code on one laptop.

1. **The dependency change** — what moving from Magazine 1 to Magazine 2, together with the removal
   of the per-message fire-timestamp write, does to throughput and to Aerospike round trips.
2. **The consume path** — where time actually goes inside ignisMQ's own consumer, and which of four
   suspected hot spots are real.

Everything below is a median or a mean over **five forked JVMs** at **each** of 1, 4, 8, 16, 32 and
50 consumers. Raw per-fork numbers are included for every table, because the fork spread is the
honest measure of how much of a difference to believe.

!!! warning "This is one laptop, one Aerospike container, and a synthetic drain"
    These numbers describe **scaling shape and relative cost**. They are not a production latency
    estimate and not a capacity plan. A single-node container on a laptop shares CPU with the
    client JVM, so absolute throughput is bounded by the machine, not by ignisMQ or Aerospike.

---

## Environment

| | |
|---|---|
| Host | Apple M1 Pro, 10 cores, 16 GiB, arm64 |
| OS | macOS 26.6.2 (build 25G83) |
| Java | OpenJDK 17.0.14 (Homebrew) |
| Worker JVM | `-Xms512m -Xmx512m`, one fresh JVM per variant per consumer count per fork |
| Aerospike | `aerospike/aerospike-server:6.2.0.7`, one local container, `MEM_GB=1 STORAGE_GB=1`, namespace `ignis_benchmark` |
| Docker server | 26.1.3, via Rancher Desktop |
| ignisMQ | `2.0.0-SNAPSHOT` |
| Magazine 2 | `com.phonepe:magazine-core:2.0.0-SNAPSHOT`, commit `6a05f2c` |
| Magazine 1 | `com.phonepe:magazine:v1.0.0-4` |
| Messages per run | 10,000 |
| Shards | 32 |
| Payload | 256 bytes |
| Forks | 5 |

The whole sweep ran with the machine held awake. This matters: an earlier attempt produced two runs
that looked exactly like stranded messages, and were in fact a sleeping laptop — the wall-clock gaps
between consecutive runs were 53 minutes, 2 h 42 and 2 h 19. In the sweep published here all 35 runs
(30 drain plus 5 delete-cost) started between 25 and 73 seconds after the previous one, and every
one handled 10,000 of 10,000 messages — the 30 drain runs with `sidelined=0`, `unconsumed=0` and
`saturationRefusals=0`.

---

## Methodology

### The harness

A local, unshipped harness forks a fresh JVM per measurement and parses a single result line from
its stdout. Two suites run on top of it:

- The **cross-version suite** runs one worker JAR per Magazine version against the same container,
  and reports **means across forks**.
- The **internal suite** runs ignisMQ's own classes — the real `MagazineQueue` and
  `MagazineConsumerTask` — and reports **medians, with the min-max fork spread in brackets**.

The two suites use different aggregations and, more importantly, **different throughput
definitions**. Do not compare a msg/s figure from one table against a msg/s figure from the other.

| | Cross-version suite | Internal `drain` |
|---|---|---|
| Aggregation | Mean | Median, with min-max |
| Throughput window | From the start latch releasing all consumer threads, to the last delivery | From the **first** handler delivery to the **last** |
| Formula | `delivered / elapsed` | `(delivered - 1) / (last - first)` |
| Includes ramp-up | Yes | No |

The internal `drain` window deliberately starts at the first delivery rather than at
`createConsumers`, because the consumer scheduler has a fixed one-second initial delay
(`INITIAL_DELAY_IN_MS = 1000`). Charging a 10,000-message run for that second would understate
throughput by an amount that varies with how fast the run is.

Every run is prefilled before measurement begins, and the client-operation counter is reset after
the prefill, so publishing never appears in the results. Variant order is reversed on even-numbered
forks to cancel ordering bias, and all variants share a single Aerospike container for the whole
sweep.

### Counting Aerospike operations

`attempts` is a count of **client-side calls**, taken by a dynamic proxy wrapped around the
Aerospike `IAerospikeClient` interface. It intercepts the operation methods — `get`, `put`,
`operate`, `delete`, `exists`, `query`, `scanAll`, `touch`, `truncate` and friends — and counts one
per invocation.

Two consequences worth stating plainly:

- **It is not a server round-trip count.** Retries internal to the Aerospike driver are invisible to
  it, because they happen below the interface it wraps.
- **A batch read counts as one attempt** regardless of how many keys it carries. Where that matters,
  a separate `batchKeys` counter records the key count, and both are reported.

### The fire-timestamp asymmetry

This is the part of the comparison most easily misread, so it is stated up front rather than
buried.

ignisMQ 1.x wrote a per-message `fireTS` bin to Aerospike on every delivery. Version 2.0 removed
that write entirely, replacing it with a delivery-time watermark. In the comparison below:

- the **Magazine 1 worker writes `fireTS`**, because that is what 1.x did;
- the **Magazine 2 worker does not**, because the run passes `--fireTimestamp=false`.

Both workers contain the identical write; it is a run-time flag, not a source difference. That flag
is set deliberately. **The asymmetry is the measurement, not a defect in it** — the point is to
compare the old deployed shape against the new deployed shape, and the removed write is part of what
changed.

It does mean the headline throughput delta cannot be attributed to Magazine alone. The attempt
counts let it be decomposed exactly, and it is decomposed [below](#what-the-attempt-saving-is-made-of).

One further difference is not isolated either: each Magazine version pulls its own transitive
Aerospike driver — 6.1.7 for v1, 6.3.1 for v2 — and Magazine 2's Micrometer instrumentation stays
switched on. Both are part of the dependency bump as an operator would actually take it.

---

## Part 1 — Magazine 1 to Magazine 2

Workload: prefill 10,000 messages, then drain them through `fire` → *(fire timestamp, v1 only)* →
handler → `delete`, with the stated number of concurrent consumer threads.

| Consumers | Variant | msg/s | vs v1 | attempts/msg | vs v1 | p99 ms |
|---:|---|---:|---:|---:|---:|---:|
| 1 | `magazine-v1` | 180.39 | — | 6.047 | — | 21.874 |
| 1 | `magazine-v2` | **297.66** | **+65.0%** | **4.004** | **−33.8%** | **6.187** |
| 4 | `magazine-v1` | 495.25 | — | 6.174 | — | 35.236 |
| 4 | `magazine-v2` | **836.20** | **+68.8%** | **4.004** | **−35.1%** | **7.148** |
| 8 | `magazine-v1` | 811.65 | — | 6.255 | — | 36.579 |
| 8 | `magazine-v2` | **1417.06** | **+74.6%** | **4.005** | **−36.0%** | **8.481** |
| 16 | `magazine-v1` | 1271.97 | — | 6.417 | — | 54.392 |
| 16 | `magazine-v2` | **2225.46** | **+75.0%** | **4.005** | **−37.6%** | **11.628** |
| 32 | `magazine-v1` | 1994.05 | — | 7.103 | — | 58.884 |
| 32 | `magazine-v2` | **3176.32** | **+59.3%** | **4.006** | **−43.6%** | **20.564** |
| 50 | `magazine-v1` | 2359.62 | — | 6.455 | — | 59.938 |
| 50 | `magazine-v2` | **3753.83** | **+59.1%** | **4.009** | **−37.9%** | **21.928** |

Means across 5 forks. All 60 runs delivered 10,000 unique messages with zero duplicates and zero
errors.

### The shape of the result

Throughput improves by **59% to 75% at every consumer count**, and p99 service time falls by roughly
a factor of three.

The more durable result is the attempt count. **Magazine 2 is flat at 4.00 attempts per message**
across a fifty-fold change in consumer count, varying only from 4.004 to 4.009. Magazine 1 rises
from 6.05 to a peak of 7.10 at 32 consumers. That rise is the contention signature: Magazine 1's read-then-increment fire path
lets competing consumers act on a stale pointer read and then retry a missing payload, so adding
consumers adds wasted reads. Magazine 2's guarded increment claims only a pointer that is actually
available, so a consumer either wins the claim or does not, and never pays for a speculative read.

The flatness is what makes the change worth taking. A throughput number is a property of this
laptop; an attempt count that does not move with consumer count is a property of the algorithm.

### What the attempt saving is made of

The per-message attempt saving is **not** all Magazine. Breaking it down by client method — each
figure is a mean per delivered message, over 5 forks:

| Consumers | Total saving | of which: `fireTS` write removed | of which: Magazine `get` reduction |
|---:|---:|---:|---:|
| 1 | 2.043 | 1.000 | 1.046 |
| 4 | 2.170 | 1.000 | 1.174 |
| 8 | 2.251 | 1.000 | 1.255 |
| 16 | 2.412 | 1.000 | 1.417 |
| 32 | 3.096 | 1.000 | 2.102 |
| 50 | 2.446 | 1.000 | 1.454 |

The fire-timestamp column is exactly 1.000 everywhere, which is the arithmetic check that the flag
did what it claims: Magazine 1 records 10,000 `put` calls per 10,000-message run, Magazine 2 records
zero. That component is **constant** — it is one write per message, and removing it removes one
attempt per message no matter how many consumers there are.

Everything that **grows** with consumer count is Magazine's. The `delete` count is exactly 1.000 per
message in both versions, and `operate` is 2.00 in both to within 0.01. The entire difference is in
`get`: Magazine 1 rises from 2.05 per message at one consumer to a peak of 3.10 at 32, while
Magazine 2 holds at 1.00 throughout.

So: of the ~2.0 attempts saved at one consumer, half is the fire-timestamp removal and half is
Magazine. Of the additional attempts saved as consumers scale, **all** of it is Magazine.

??? note "Raw forks — Magazine comparison (60 runs)"

    | Consumers | Variant | Fork | msg/s | attempts/msg | p50 ms | p95 ms | p99 ms |
    |---:|---|---:|---:|---:|---:|---:|---:|
    | 1 | `magazine-v1` | 1 | 175.92 | 6.048 | 4.965 | 7.115 | 24.794 |
    | 1 | `magazine-v1` | 2 | 188.67 | 6.039 | 4.720 | 6.199 | 18.845 |
    | 1 | `magazine-v1` | 3 | 179.68 | 6.053 | 4.839 | 6.507 | 19.384 |
    | 1 | `magazine-v1` | 4 | 179.82 | 6.051 | 4.854 | 6.840 | 18.922 |
    | 1 | `magazine-v1` | 5 | 177.88 | 6.044 | 4.855 | 6.967 | 27.427 |
    | 1 | `magazine-v2` | 1 | 297.44 | 4.004 | 3.251 | 4.386 | 5.469 |
    | 1 | `magazine-v2` | 2 | 296.33 | 4.004 | 3.284 | 4.407 | 5.373 |
    | 1 | `magazine-v2` | 3 | 305.12 | 4.004 | 3.208 | 4.283 | 5.262 |
    | 1 | `magazine-v2` | 4 | 306.77 | 4.004 | 3.162 | 4.169 | 5.460 |
    | 1 | `magazine-v2` | 5 | 282.65 | 4.004 | 3.249 | 5.124 | 9.370 |
    | 4 | `magazine-v1` | 1 | 491.14 | 6.176 | 7.232 | 9.447 | 35.283 |
    | 4 | `magazine-v1` | 2 | 492.39 | 6.199 | 7.021 | 9.254 | 33.860 |
    | 4 | `magazine-v1` | 3 | 487.18 | 6.195 | 7.026 | 10.657 | 33.233 |
    | 4 | `magazine-v1` | 4 | 508.59 | 6.090 | 6.827 | 8.706 | 31.701 |
    | 4 | `magazine-v1` | 5 | 496.94 | 6.210 | 6.798 | 9.476 | 42.101 |
    | 4 | `magazine-v2` | 1 | 796.66 | 4.004 | 4.903 | 6.549 | 7.887 |
    | 4 | `magazine-v2` | 2 | 802.63 | 4.004 | 4.908 | 6.330 | 7.347 |
    | 4 | `magazine-v2` | 3 | 845.17 | 4.004 | 4.668 | 5.989 | 6.844 |
    | 4 | `magazine-v2` | 4 | 847.72 | 4.004 | 4.651 | 5.940 | 6.895 |
    | 4 | `magazine-v2` | 5 | 888.82 | 4.004 | 4.426 | 5.736 | 6.767 |
    | 8 | `magazine-v1` | 1 | 801.12 | 6.238 | 8.639 | 12.285 | 35.167 |
    | 8 | `magazine-v1` | 2 | 799.53 | 6.252 | 8.522 | 11.389 | 34.686 |
    | 8 | `magazine-v1` | 3 | 842.94 | 6.281 | 8.083 | 10.226 | 23.822 |
    | 8 | `magazine-v1` | 4 | 767.49 | 6.245 | 8.458 | 16.747 | 55.393 |
    | 8 | `magazine-v1` | 5 | 847.18 | 6.260 | 8.298 | 11.156 | 33.830 |
    | 8 | `magazine-v2` | 1 | 1366.00 | 4.005 | 5.755 | 7.397 | 8.759 |
    | 8 | `magazine-v2` | 2 | 1328.12 | 4.005 | 5.882 | 7.844 | 9.767 |
    | 8 | `magazine-v2` | 3 | 1431.98 | 4.004 | 5.472 | 7.165 | 8.374 |
    | 8 | `magazine-v2` | 4 | 1479.25 | 4.004 | 5.326 | 6.777 | 7.606 |
    | 8 | `magazine-v2` | 5 | 1479.95 | 4.004 | 5.314 | 6.906 | 7.900 |
    | 16 | `magazine-v1` | 1 | 1232.80 | 6.409 | 10.557 | 14.356 | 47.649 |
    | 16 | `magazine-v1` | 2 | 1230.95 | 6.364 | 10.694 | 21.207 | 57.790 |
    | 16 | `magazine-v1` | 3 | 1260.02 | 6.455 | 9.976 | 19.645 | 67.123 |
    | 16 | `magazine-v1` | 4 | 1336.98 | 6.437 | 10.153 | 18.181 | 49.532 |
    | 16 | `magazine-v1` | 5 | 1299.08 | 6.422 | 10.378 | 19.950 | 49.866 |
    | 16 | `magazine-v2` | 1 | 2077.97 | 4.005 | 7.454 | 10.469 | 13.009 |
    | 16 | `magazine-v2` | 2 | 2164.89 | 4.005 | 7.189 | 9.957 | 12.843 |
    | 16 | `magazine-v2` | 3 | 2312.61 | 4.005 | 6.823 | 8.890 | 9.987 |
    | 16 | `magazine-v2` | 4 | 2161.82 | 4.005 | 7.174 | 10.062 | 12.775 |
    | 16 | `magazine-v2` | 5 | 2410.01 | 4.005 | 6.537 | 8.473 | 9.526 |
    | 32 | `magazine-v1` | 1 | 1989.83 | 7.325 | 14.023 | 23.443 | 52.551 |
    | 32 | `magazine-v1` | 2 | 1990.48 | 7.389 | 13.567 | 23.119 | 61.984 |
    | 32 | `magazine-v1` | 3 | 1973.35 | 7.275 | 14.830 | 24.697 | 45.886 |
    | 32 | `magazine-v1` | 4 | 1988.64 | 7.317 | 14.316 | 22.885 | 72.215 |
    | 32 | `magazine-v1` | 5 | 2027.93 | 6.208 | 13.268 | 23.544 | 61.785 |
    | 32 | `magazine-v2` | 1 | 2625.83 | 4.006 | 11.336 | 19.616 | 32.454 |
    | 32 | `magazine-v2` | 2 | 3345.00 | 4.007 | 9.186 | 13.827 | 19.861 |
    | 32 | `magazine-v2` | 3 | 3581.86 | 4.005 | 8.550 | 12.764 | 17.436 |
    | 32 | `magazine-v2` | 4 | 3121.52 | 4.006 | 10.003 | 14.215 | 18.776 |
    | 32 | `magazine-v2` | 5 | 3207.39 | 4.006 | 9.937 | 12.632 | 14.293 |
    | 50 | `magazine-v1` | 1 | 2289.94 | 6.395 | 20.277 | 27.805 | 59.912 |
    | 50 | `magazine-v1` | 2 | 2392.64 | 6.492 | 18.393 | 29.971 | 58.993 |
    | 50 | `magazine-v1` | 3 | 2490.72 | 6.526 | 18.559 | 29.396 | 61.761 |
    | 50 | `magazine-v1` | 4 | 2297.91 | 6.424 | 20.014 | 26.264 | 57.035 |
    | 50 | `magazine-v1` | 5 | 2326.89 | 6.439 | 19.223 | 32.766 | 61.991 |
    | 50 | `magazine-v2` | 1 | 3641.50 | 4.009 | 13.736 | 19.028 | 24.581 |
    | 50 | `magazine-v2` | 2 | 3810.94 | 4.008 | 13.031 | 17.618 | 22.708 |
    | 50 | `magazine-v2` | 3 | 3625.77 | 4.009 | 13.725 | 18.578 | 22.957 |
    | 50 | `magazine-v2` | 4 | 4000.14 | 4.009 | 12.376 | 16.539 | 18.801 |
    | 50 | `magazine-v2` | 5 | 3690.81 | 4.008 | 13.537 | 17.508 | 20.592 |

    Two dispersion notes. At 32 consumers, Magazine 1 fork 5 reports 6.208 attempts/msg against
    7.28–7.39 for forks 1–4; the published mean of 7.103 is pulled down by that one fork, and the
    32-consumer row is the least stable in the table. At 32 consumers Magazine 2 fork 1 reports
    2625.83 msg/s against 3121–3582 for the rest, which is why the +59.3% at that row is the
    weakest improvement figure and should not be read as a real dip between 16 and 32.

---

## Part 2 — inside the ignisMQ consume path

This half drives the **real shipping classes** — `MagazineQueue` and `MagazineConsumerTask` with
real metrics, a real handler executor and a real scheduler — rather than a reimplementation.

### Drain throughput

Prefill 10,000 messages, start *n* consumers, drain to empty. Medians over 5 forks, min-max in
brackets.

| Consumers | msg/s | `ignismq.consume` mean µs | handler mean µs | attempts/delivery | empty polls |
|---:|---:|---:|---:|---:|---:|
| 1 | 292.8 [242.8–308.4] | 847.5 [799.0–1035.6] | 30.6 [25.7–41.6] | 4.007 | 0 |
| 4 | 820.3 [687.2–893.3] | 1244.7 [1142.5–1485.5] | 34.5 [25.9–45.5] | 4.007 | 3 [2–3] |
| 8 | 1351.3 [1132.8–1517.3] | 1506.3 [1336.4–1798.2] | 36.2 [29.9–57.3] | 4.008 | 7 [5–7] |
| 16 | 2112.0 [1637.9–2328.0] | 1910.1 [1735.8–2468.2] | 59.6 [44.9–97.8] | 4.009 | 13 [9–16] |
| 32 | 3243.0 [2483.9–3509.2] | 2515.0 [2282.4–3292.1] | 96.8 [66.5–183.1] | 4.011 | 31 [27–32] |
| 50 | 3447.1 [3062.4–4123.1] | 3658.0 [3025.8–4231.8] | 138.5 [129.8–285.5] | 4.013 | 50 [47–50] |

All 30 runs delivered 10,000 unique messages, with no sidelining, nothing unconsumed and no
saturation refusals.

Throughput scales close to linearly to 32 consumers and then flattens — 32 → 50 buys about 6%, on a
10-core machine where the Aerospike container competes with the client JVM for those cores. That
knee is the laptop, not a property of the queue, and the flat attempt count says as much: 4.007 at
one consumer and 4.013 at fifty, so the extra consumers are not generating extra work, they are
simply waiting on a saturated host.

`ignismq.consume` covers decode, the handler call, the per-message delete and any sidelining. The
handler timer covers the handler call and its thread-pool handoff only. The gap between them — about
817 µs at one consumer, about 3,520 µs at fifty — is where the consume path's real cost lives, and
almost all of it is Aerospike I/O.

Note that the empty-poll count tracks roughly one per consumer, less one: 0 at a single consumer,
then 3, 7, 13, 31 and 50. That is the end of the drain, where every consumer that is not the one
taking the last message wakes once, finds nothing and records a single empty poll. It is not a
per-poll overhead, and it does not grow with the length of the run.

??? note "Raw forks — drain (30 runs)"

    | Consumers | Fork | msg/s | consume µs | handler µs | attempts/delivery | delivered | unique | empty polls |
    |---:|---:|---:|---:|---:|---:|---:|---:|---:|
    | 1 | 1 | 297.9 | 829.0 | 28.820 | 4.0073 | 10000 | 10000 | 0 |
    | 1 | 2 | 308.4 | 799.0 | 25.685 | 4.0105 | 10000 | 10000 | 0 |
    | 1 | 3 | 292.8 | 847.5 | 30.646 | 4.0073 | 10000 | 10000 | 0 |
    | 1 | 4 | 242.8 | 1035.6 | 41.607 | 4.0073 | 10000 | 10000 | 0 |
    | 1 | 5 | 282.9 | 877.6 | 32.070 | 4.0105 | 10000 | 10000 | 0 |
    | 4 | 1 | 869.5 | 1177.7 | 29.773 | 4.0073 | 10000 | 10000 | 3 |
    | 4 | 2 | 820.3 | 1244.7 | 34.544 | 4.0073 | 10000 | 10000 | 2 |
    | 4 | 3 | 771.9 | 1320.7 | 37.281 | 4.0072 | 10000 | 10000 | 3 |
    | 4 | 4 | 687.2 | 1485.5 | 45.493 | 4.0078 | 10000 | 10000 | 3 |
    | 4 | 5 | 893.3 | 1142.5 | 25.908 | 4.0073 | 10000 | 10000 | 2 |
    | 8 | 1 | 1393.1 | 1451.7 | 35.950 | 4.0077 | 10000 | 10000 | 7 |
    | 8 | 2 | 1351.3 | 1506.3 | 36.220 | 4.0075 | 10000 | 10000 | 7 |
    | 8 | 3 | 1243.9 | 1627.5 | 44.921 | 4.0081 | 10000 | 10000 | 6 |
    | 8 | 4 | 1132.8 | 1798.2 | 57.278 | 4.0079 | 10000 | 10000 | 5 |
    | 8 | 5 | 1517.3 | 1336.4 | 29.924 | 4.0075 | 10000 | 10000 | 7 |
    | 16 | 1 | 2135.4 | 1902.7 | 59.601 | 4.0097 | 10000 | 10000 | 16 |
    | 16 | 2 | 2112.0 | 1910.1 | 53.469 | 4.0088 | 10000 | 10000 | 13 |
    | 16 | 3 | 1806.6 | 2225.9 | 78.910 | 4.0087 | 10000 | 10000 | 14 |
    | 16 | 4 | 1637.9 | 2468.2 | 97.847 | 4.0087 | 10000 | 10000 | 9 |
    | 16 | 5 | 2328.0 | 1735.8 | 44.895 | 4.0082 | 10000 | 10000 | 13 |
    | 32 | 1 | 3243.0 | 2515.0 | 96.831 | 4.0098 | 10000 | 10000 | 27 |
    | 32 | 2 | 3258.5 | 2486.6 | 81.680 | 4.0108 | 10000 | 10000 | 31 |
    | 32 | 3 | 2518.0 | 3284.0 | 183.1 | 4.0117 | 10000 | 10000 | 32 |
    | 32 | 4 | 2483.9 | 3292.1 | 172.6 | 4.0106 | 10000 | 10000 | 32 |
    | 32 | 5 | 3509.2 | 2282.4 | 66.513 | 4.0109 | 10000 | 10000 | 31 |
    | 50 | 1 | 3702.4 | 3404.4 | 138.5 | 4.0108 | 10000 | 10000 | 50 |
    | 50 | 2 | 3447.1 | 3658.0 | 129.8 | 4.0133 | 10000 | 10000 | 50 |
    | 50 | 3 | 3062.4 | 4231.8 | 271.3 | 4.0125 | 10000 | 10000 | 50 |
    | 50 | 4 | 3090.6 | 4191.9 | 285.5 | 4.0124 | 10000 | 10000 | 47 |
    | 50 | 5 | 4123.1 | 3025.8 | 130.0 | 4.0131 | 10000 | 10000 | 50 |

### The per-message delete costs a quarter of the path

A separate scenario drives a raw `Magazine` single-threaded, timing `fire()` and `delete()` around
each other with nothing else in between.

| | Median | Min–max across forks |
|---|---:|---|
| `fire` per message | 2379.4 µs | 2097.4 – 3158.4 |
| `delete` per message | 782.6 µs | 694.6 – 1024.2 |
| **delete as a share of `fire` + `delete`** | **24.75%** | **24.49 – 24.88** |
| attempts per message | 4.004 | 4.0036 – 4.0039 |

The share is remarkably stable — under half a percentage point of spread across five forks, even
though the absolute microsecond figures move by 50%. That is the expected signature of a ratio
between two costs that scale together with host load.

!!! note "What this 24.75% is a share *of*"
    It is the share of the `fire` + `delete` pair **only**. It is not a share of the full consume
    path, which also contains deserialisation, the handler call and the scheduling overhead. Read it
    as: *of the Aerospike work the consume path does per message, a quarter is the delete.*

This is cross-checked against the drain scenario, which reaches the same per-message attempt count
by a completely different route: drain reports 4.007–4.013 attempts per delivery through
`MagazineQueue` and `MagazineConsumerTask`, while this scenario reports 4.004 driving `Magazine`
directly. Two independent instruments agreeing to within 0.01 attempts is the evidence that neither
is miscounting.

**This is the one finding with a live follow-up.** A quarter of the storage cost of consuming a
message is spent retiring it, one round trip at a time. A batched delete is the obvious lever, and
it is a request to make of Magazine rather than a change ignisMQ can make alone.

??? note "Raw forks — delete cost (5 runs)"

    | Fork | fire µs/msg | delete µs/msg | delete share % | attempts/msg | handled |
    |---:|---:|---:|---:|---:|---:|
    | 1 | 2370.2 | 779.8 | 24.756 | 4.0036 | 10000 |
    | 2 | 2379.4 | 782.6 | 24.749 | 4.0037 | 10000 |
    | 3 | 3158.4 | 1024.2 | 24.486 | 4.0039 | 10000 |
    | 4 | 2973.4 | 964.7 | 24.496 | 4.0038 | 10000 |
    | 5 | 2097.4 | 694.6 | 24.878 | 4.0036 | 10000 |

    Single-threaded by construction, so this scenario runs once per fork and its consumer-count
    label is not meaningful.

### Three suspected hot spots that are not hot spots

Three structural costs were suspected of mattering enough to justify redesign. All three were
measured. None of them do.

#### The handler thread-pool handoff

Every delivery hands the handler call to a bounded executor and waits on the future, rather than
calling the handler on the polling thread. The cost of that handoff, measured against calling the
identical no-op handler inline:

| Consumers | Handoff ns/call | Inline ns/call |
|---:|---:|---:|
| 1 | 7023.7 [6296.9–7941.4] | 11.9 |
| 4 | 7294.6 [6971.3–9251.4] | 12.4 |
| 8 | 7672.6 [7146.2–9081.3] | 13.5 |
| 16 | 7603.0 [7161.0–9567.8] | 12.6 |
| 32 | 7440.7 [7003.2–10396.1] | 12.1 |
| 50 | 7569.4 [6735.8–9112.3] | 12.2 |

The handoff costs **7.0–7.7 µs and is flat** from 1 to 50 consumers — it does not degrade under
concurrency, which was the actual worry. An inline call is 12 ns, so the handoff is roughly 600×
the cost of not doing it.

That ratio sounds alarming until it is put beside the message path. At 50 consumers a message spends
about 3,658 µs in `ignismq.consume`. A 7.5 µs handoff is **about 0.2% of it**. The bound that the
executor buys — a hard cap on concurrent handler execution, and the ability to refuse rather than
melt — costs almost nothing. It stays.

#### The overdue-task gauge

`ignismq.pool.tasks.due` streams the scheduler's delay queue on every scrape, so it is O(scheduled
tasks). Confirmed:

| Scheduled tasks | ns per scrape | ns per task per scrape |
|---:|---:|---:|
| 1 | 702.3 [671.7–741.9] | 702.3 |
| 4 | 859.6 [842.1–908.3] | 214.9 |
| 8 | 1071.9 [739.6–1116.7] | 134.0 |
| 16 | 1475.2 [1268.2–1786.9] | 92.2 |
| 32 | 1665.7 [1111.7–1814.1] | 52.1 |
| 50 | 2125.8 [1794.7–2817.2] | 42.5 |

O(n) is real — a least-squares fit over all 30 runs gives about 30 ns per additional task, on a base
of roughly 700 ns for the first. But the endpoint
is 2.1 µs at fifty consumers, on a gauge scraped once per Prometheus interval. Making it O(1) would
save two microseconds per scrape. Immaterial.

#### The idle poll floor

Each consumer wakes on a fixed 1-second period (`DELAY_PERIOD_IN_MS = 1000`) whether or not there is
work. On a fully drained queue, over a 15-second window:

| Consumers | Empty polls | Empty polls per consumer per second | Client attempts | Batch keys |
|---:|---:|---:|---:|---:|
| 1 | 14 | 0.933 | 2 | 64 |
| 4 | 56 | 0.933 | 2 | 64 |
| 8 | 112 | 0.933 | 2 | 64 |
| 16 | 224 | 0.933 | 2 | 64 |
| 32 | 448 | 0.933 | 2 | 64 |
| 50 | 700 | 0.933 | 2 | 64 |

The 0.933 figure is the 1-second floor, confirmed exactly: 14 polls in a 15-second window.

**The important column is not the poll count — it is the client attempts.** Aerospike traffic on an
idle queue holds at a median of 2 attempts covering 64 batch keys per 15 seconds, whether there is
one consumer or fifty. Fifty idle consumers generate the same storage load as one.

!!! warning "This corrects a premise the project held for some time"
    An idle-backoff feature was scoped on the belief that idle consumers cost a constant CPU **and
    Aerospike** load, and that backing off would reduce both. The Aerospike half is false, and
    Magazine is the reason. `ActiveShardSelector` caches shard state with `refreshAfterWrite`, and
    `suppress` prunes a shard the caller has just observed to be drained, so once every shard is
    pruned a drained magazine answers `NOTHING_TO_FIRE` from cache with no round trip at all. The
    only idle traffic is the periodic active-shard refresh, which fans out across all 32 shards
    **per magazine, not per consumer**.

    The fork data shows this directly: three of the thirty runs recorded 34 attempts rather than 2,
    and 34 − 2 = 32, one read per shard. Those three runs are the ones where a refresh happened to
    land inside the measurement window, and they occur at 4 and 16 consumers — not preferentially
    at high consumer counts.

    So an idle backoff would save timer wake-ups only, and would pay for them in wake-up latency.
    The lever that would actually reduce idle storage traffic is the active-shard refresh interval
    (`ACTIVE_SHARD_REFRESH_SECONDS`, currently 5), not the poll period.

??? note "Raw forks — handoff, gauge and idle poll (90 runs)"

    **Handler handoff**

    | Consumers | Fork | pooled ns/call | inline ns/call | handoff ns/call |
    |---:|---:|---:|---:|---:|
    | 1 | 1 | 6626.3 | 12.435 | 6613.9 |
    | 1 | 2 | 7035.4 | 11.626 | 7023.7 |
    | 1 | 3 | 6308.8 | 11.891 | 6296.9 |
    | 1 | 4 | 7953.8 | 12.385 | 7941.4 |
    | 1 | 5 | 7349.3 | 8.602 | 7340.7 |
    | 4 | 1 | 6984.9 | 13.591 | 6971.3 |
    | 4 | 2 | 7447.0 | 9.129 | 7437.9 |
    | 4 | 3 | 7098.6 | 12.432 | 7086.2 |
    | 4 | 4 | 7306.8 | 12.133 | 7294.6 |
    | 4 | 5 | 9264.0 | 12.596 | 9251.4 |
    | 8 | 1 | 7486.8 | 13.894 | 7472.9 |
    | 8 | 2 | 7686.1 | 13.459 | 7672.6 |
    | 8 | 3 | 7158.5 | 12.358 | 7146.2 |
    | 8 | 4 | 8934.1 | 12.992 | 8921.1 |
    | 8 | 5 | 9096.0 | 14.686 | 9081.3 |
    | 16 | 1 | 7173.4 | 12.328 | 7161.0 |
    | 16 | 2 | 7615.8 | 12.800 | 7603.0 |
    | 16 | 3 | 7601.4 | 11.926 | 7589.4 |
    | 16 | 4 | 9580.7 | 12.848 | 9567.8 |
    | 16 | 5 | 8502.2 | 12.580 | 8489.6 |
    | 32 | 1 | 7045.3 | 11.560 | 7033.7 |
    | 32 | 2 | 7831.5 | 12.052 | 7819.4 |
    | 32 | 3 | 7015.6 | 12.397 | 7003.2 |
    | 32 | 4 | 10408.0 | 11.814 | 10396.1 |
    | 32 | 5 | 7454.4 | 13.625 | 7440.7 |
    | 50 | 1 | 9125.1 | 12.858 | 9112.3 |
    | 50 | 2 | 7581.8 | 12.412 | 7569.4 |
    | 50 | 3 | 6863.5 | 12.130 | 6851.4 |
    | 50 | 4 | 8073.4 | 11.989 | 8061.4 |
    | 50 | 5 | 6748.1 | 12.235 | 6735.8 |

    **Scheduler gauge**

    | Tasks | Fork | ns/scrape | ns/task/scrape |
    |---:|---:|---:|---:|
    | 1 | 1 | 709.4 | 709.4 |
    | 1 | 2 | 671.7 | 671.7 |
    | 1 | 3 | 679.6 | 679.6 |
    | 1 | 4 | 741.9 | 741.9 |
    | 1 | 5 | 702.3 | 702.3 |
    | 4 | 1 | 898.5 | 224.6 |
    | 4 | 2 | 854.3 | 213.6 |
    | 4 | 3 | 908.3 | 227.1 |
    | 4 | 4 | 859.6 | 214.9 |
    | 4 | 5 | 842.1 | 210.5 |
    | 8 | 1 | 1116.7 | 139.6 |
    | 8 | 2 | 1071.9 | 134.0 |
    | 8 | 3 | 1019.4 | 127.4 |
    | 8 | 4 | 739.6 | 92.451 |
    | 8 | 5 | 1111.9 | 139.0 |
    | 16 | 1 | 1695.9 | 106.0 |
    | 16 | 2 | 1452.1 | 90.754 |
    | 16 | 3 | 1268.2 | 79.265 |
    | 16 | 4 | 1786.9 | 111.7 |
    | 16 | 5 | 1475.2 | 92.199 |
    | 32 | 1 | 1665.7 | 52.052 |
    | 32 | 2 | 1111.7 | 34.742 |
    | 32 | 3 | 1438.2 | 44.945 |
    | 32 | 4 | 1814.1 | 56.690 |
    | 32 | 5 | 1775.6 | 55.486 |
    | 50 | 1 | 2125.8 | 42.516 |
    | 50 | 2 | 2593.7 | 51.874 |
    | 50 | 3 | 2817.2 | 56.344 |
    | 50 | 4 | 1794.7 | 35.894 |
    | 50 | 5 | 2124.1 | 42.482 |

    **Idle poll** — 15-second window on a drained queue

    | Consumers | Fork | empty polls | per consumer per second | client attempts | batch keys |
    |---:|---:|---:|---:|---:|---:|
    | 1 | 1 | 15 | 1.000 | 2 | 64 |
    | 1 | 2 | 14 | 0.933 | 2 | 64 |
    | 1 | 3 | 14 | 0.933 | 2 | 64 |
    | 1 | 4 | 14 | 0.933 | 2 | 64 |
    | 1 | 5 | 14 | 0.933 | 2 | 64 |
    | 4 | 1 | 56 | 0.933 | 34 | 64 |
    | 4 | 2 | 56 | 0.933 | 2 | 64 |
    | 4 | 3 | 56 | 0.933 | 2 | 64 |
    | 4 | 4 | 56 | 0.933 | 2 | 64 |
    | 4 | 5 | 56 | 0.933 | 34 | 64 |
    | 8 | 1 | 112 | 0.933 | 2 | 64 |
    | 8 | 2 | 112 | 0.933 | 2 | 64 |
    | 8 | 3 | 112 | 0.933 | 2 | 64 |
    | 8 | 4 | 112 | 0.933 | 2 | 64 |
    | 8 | 5 | 112 | 0.933 | 2 | 64 |
    | 16 | 1 | 224 | 0.933 | 2 | 64 |
    | 16 | 2 | 224 | 0.933 | 34 | 64 |
    | 16 | 3 | 224 | 0.933 | 2 | 64 |
    | 16 | 4 | 224 | 0.933 | 2 | 64 |
    | 16 | 5 | 224 | 0.933 | 2 | 64 |
    | 32 | 1 | 448 | 0.933 | 2 | 64 |
    | 32 | 2 | 448 | 0.933 | 2 | 64 |
    | 32 | 3 | 448 | 0.933 | 2 | 64 |
    | 32 | 4 | 448 | 0.933 | 2 | 64 |
    | 32 | 5 | 448 | 0.933 | 2 | 64 |
    | 50 | 1 | 700 | 0.933 | 2 | 64 |
    | 50 | 2 | 700 | 0.933 | 2 | 64 |
    | 50 | 3 | 700 | 0.933 | 2 | 64 |
    | 50 | 4 | 700 | 0.933 | 2 | 64 |
    | 50 | 5 | 700 | 0.933 | 2 | 64 |

### The cost of the metrics themselves

ignisMQ 2.0 instruments the consume path with Micrometer timers and counters. To price them, the
same drain scenario was run with instrumentation on and off — the same worker JAR, the same
container, differing only by a flag.

| Consumers | metrics on (msg/s) | metrics off (msg/s) | Difference |
|---:|---:|---:|---:|
| 1 | 287.2 | 285.3 | **+0.7%** |
| 16 | 2033.2 | 2044.3 | **−0.5%** |
| 50 | 3698.7 | 3692.7 | **+0.2%** |

Medians over 5 forks. A positive figure means the **instrumented** build measured faster.

Instrumentation on measured *faster* than instrumentation off at two of the three consumer counts,
which is the clearest possible statement that the effect is below the noise floor. At 50 consumers
the fork-to-fork spread across both variants runs from 3,112 to 4,211 msg/s — a range of about 30%
of the median, against a measured difference of 0.2%.

The honest conclusion is not "metrics are cheap", it is **"the cost of metrics is not measurable by
this experiment"**. There is no instrumentation overhead here worth removing.

??? note "Raw forks — metrics on vs off (30 runs)"

    | Consumers | Variant | Fork | msg/s |
    |---:|---|---:|---:|
    | 1 | `metrics-on` | 1 | 287.2 |
    | 1 | `metrics-on` | 2 | 262.8 |
    | 1 | `metrics-on` | 3 | 294.8 |
    | 1 | `metrics-on` | 4 | 282.1 |
    | 1 | `metrics-on` | 5 | 298.7 |
    | 1 | `metrics-off` | 1 | 272.7 |
    | 1 | `metrics-off` | 2 | 285.3 |
    | 1 | `metrics-off` | 3 | 287.4 |
    | 1 | `metrics-off` | 4 | 294.8 |
    | 1 | `metrics-off` | 5 | 278.3 |
    | 16 | `metrics-on` | 1 | 2109.0 |
    | 16 | `metrics-on` | 2 | 1998.4 |
    | 16 | `metrics-on` | 3 | 2035.4 |
    | 16 | `metrics-on` | 4 | 2033.2 |
    | 16 | `metrics-on` | 5 | 2018.4 |
    | 16 | `metrics-off` | 1 | 1968.8 |
    | 16 | `metrics-off` | 2 | 2011.9 |
    | 16 | `metrics-off` | 3 | 2076.7 |
    | 16 | `metrics-off` | 4 | 2073.3 |
    | 16 | `metrics-off` | 5 | 2044.3 |
    | 50 | `metrics-on` | 1 | 3391.7 |
    | 50 | `metrics-on` | 2 | 4210.9 |
    | 50 | `metrics-on` | 3 | 3850.3 |
    | 50 | `metrics-on` | 4 | 3390.7 |
    | 50 | `metrics-on` | 5 | 3698.7 |
    | 50 | `metrics-off` | 1 | 3338.1 |
    | 50 | `metrics-off` | 2 | 3930.9 |
    | 50 | `metrics-off` | 3 | 3692.7 |
    | 50 | `metrics-off` | 4 | 3112.6 |
    | 50 | `metrics-off` | 5 | 4131.4 |

    With instrumentation off the Micrometer-derived columns are necessarily zero, so only
    throughput is comparable between the two variants.

---

## Summary

| Question | Answer |
|---|---|
| Is the Magazine 2 bump worth taking? | Yes. +59% to +75% throughput at every consumer count, and attempts per message flat at 4.00 where Magazine 1 rises from 6.0 to a peak of 7.1 |
| Does the handler handoff need removing? | No. 7.0–7.7 µs, flat to 50 consumers, about 0.2% of the message path |
| Does the overdue-task gauge need to be O(1)? | No. 2.1 µs at 50 tasks, scraped once per interval |
| Do idle consumers need a backoff? | Not for storage cost — that is already flat in consumer count. Only timer wake-ups would be saved |
| Is the per-message delete worth batching? | **Yes — the one live item.** 24.75% of the per-message Aerospike work |
| Is the instrumentation worth removing? | Not measurable. Well inside fork-to-fork noise |

---

## Limitations

- **One machine, one container, one namespace.** No network between client and server, no
  replication, no multi-node coordination. Real deployments have all three.
- **Absolute throughput is host-bound.** The client JVM and the Aerospike server share ten cores, so
  the flattening beyond 32 consumers is the laptop saturating. Treat the shape as meaningful and the
  ceiling as an artefact.
- **`attempts` counts client calls, not server round trips.** Driver-internal retries are invisible,
  and a batch read counts as one attempt however many keys it carries.
- **The two suites are not mutually comparable.** They use different throughput windows and
  different aggregations, as set out in the methodology.
- **The Magazine comparison bundles three changes**: the Magazine version, the removal of the
  fire-timestamp write, and each version's transitive Aerospike driver (6.1.7 against 6.3.1). The
  attempt decomposition separates the first two; the driver is not isolated.
- **Each version creates a fresh queue.** Magazine 2 therefore uses its unified metadata layout;
  consuming a pre-existing Magazine 1 legacy-layout queue is a different exercise.
- **Five forks bound dispersion, they do not establish significance.** Where the fork spread is wide
  — the 32-consumer rows especially — the min-max brackets are the number to read, not the median.
