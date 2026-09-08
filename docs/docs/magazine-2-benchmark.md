# Magazine 2.0 Comparison

Preliminary fresh-queue comparison of the Magazine dependency bump only:

- Before: `com.phonepe:magazine:v1.0.0-4`
- After: `com.phonepe:magazine-core:2.0.0-SNAPSHOT`, Magazine commit `f3873f4a4dbb`
- Workload: prefill, then `fire -> fire timestamp -> successful handler -> delete`
- Messages: 1,000 per fork
- Shards: 32
- Payload: 256 bytes
- Forks: 2, with variant order reversed in the second fork
- Aerospike: `aerospike/aerospike-server:6.2.0.7`, one local container
- Host: Apple M1 Pro, 16 GiB, arm64
- Java: OpenJDK 17.0.14
- Docker server: 26.1.3

## Comparison

| Consumers | Variant | msg/s | Throughput vs v1 | attempts/msg | Attempts vs v1 | p99 service ms | Complete forks |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | `magazine-v1` | 76.68 | baseline | 6.251 | baseline | 96.100 | 2/2 |
| 1 | `magazine-v2` | 85.04 | +10.9% | 5.033 | -19.5% | 47.426 | 2/2 |
| 8 | `magazine-v1` | 303.89 | baseline | 7.423 | baseline | 141.625 | 2/2 |
| 8 | `magazine-v2` | 562.36 | +85.1% | 5.041 | -32.1% | 31.616 | 2/2 |
| 16 | `magazine-v1` | 459.88 | baseline | 9.392 | baseline | 138.609 | 2/2 |
| 16 | `magazine-v2` | 729.60 | +58.6% | 5.048 | -46.3% | 53.926 | 2/2 |

All 12 runs delivered 1,000 unique messages with zero duplicates and zero errors.

Magazine 2's native metrics measured core fire operations at approximately 3.03-3.05 calls per
delivery. The end-to-end client-attempt ratio is approximately 5.04 because ignis adds one fire
timestamp write and Magazine deletes the payload after successful handling. Small excesses are
empty-shard probes at the end of a drain.

Magazine 1's attempts per delivery rise with consumer count because its read-then-increment fire
path lets competing consumers act on stale pointer reads and retry missing payloads. Magazine 2
remains flat because each guarded increment claims only an available pointer.

## Raw Forks

| Consumers | Variant | msg/s | attempts/msg | p99 service ms |
|---:|---|---:|---:|---:|
| 1 | `magazine-v1` | 69.00 | 6.221 | 98.046 |
| 1 | `magazine-v2` | 84.63 | 5.032 | 51.668 |
| 1 | `magazine-v2` | 85.45 | 5.033 | 43.184 |
| 1 | `magazine-v1` | 84.36 | 6.281 | 94.153 |
| 8 | `magazine-v1` | 301.82 | 7.493 | 146.254 |
| 8 | `magazine-v2` | 598.19 | 5.039 | 26.332 |
| 8 | `magazine-v2` | 526.53 | 5.044 | 36.901 |
| 8 | `magazine-v1` | 305.96 | 7.353 | 136.996 |
| 16 | `magazine-v1` | 483.21 | 9.472 | 116.253 |
| 16 | `magazine-v2` | 764.91 | 5.042 | 39.640 |
| 16 | `magazine-v2` | 694.30 | 5.054 | 68.212 |
| 16 | `magazine-v1` | 436.56 | 9.312 | 160.965 |

## Caveats

This is one machine, one container, two forks and a synthetic prefilled drain. It demonstrates the
scaling shape and client-operation amplification, not production latency or a statistically robust
throughput estimate.

Each version creates a fresh queue. Magazine 2 therefore uses unified metadata; consuming an
existing Magazine 1 legacy-layout queue requires a separate compatibility scenario.

The dependency bump includes each Magazine version's transitive Aerospike driver and Magazine 2's
default Micrometer instrumentation. Client attempts are proxy-observed API calls, not server-side
round trips, and do not expose retries internal to the Aerospike driver.
