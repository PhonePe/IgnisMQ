# Console

A read-only console, plus a small set of actions behind a role. It ships with `ignismq-dw-bundle`
and is mounted automatically.

## The shape of it, and why

**ignisMQ has no broker.** RabbitMQ's management UI and Kafka's consoles talk to a server that knows
the whole cluster. ignisMQ is a library: the only thing that knows the whole system is Aerospike, and
the only thing that knows a *process* is that process.

So the console is two views over two sources, and it never lets one borrow the other's authority.

| View | Source | True of |
|---|---|---|
| **Cluster** | Aerospike, through the queue service | What queues exist, their stored configuration, and whether they are active. Correct wherever it is read |
| **Instance** | This process's own registry | Consumers, shovels and meters here. Correct for this process and nothing else |

Every response and every panel says which it is.

**A queue is active or it is not — that is the only state the console treats as a category.** Every
instance adopts every active queue on its refresh pass, so "which instances serve this queue" is not
a useful distinction and the console does not present one.

The exception is real but narrow. A queue can be active in storage and **not yet adopted here**:
between its creation on another instance and this instance's next refresh, or permanently if this
instance has no handler registered for its `messageHandlerType`. Depth is read through live
magazines, so that queue reports its configuration and **no depth at all** rather than zeros, which
would read as an idle queue. The console flags it as the anomaly it is, not as a category of queue.

### Aggregating across instances

**It does not.** Instance metrics are per-process and ignisMQ has no register of processes. Rolling
them up is the metrics backend's job — scrape `ignismq.*` with Prometheus and aggregate in Grafana,
which is what those tools are for. See the [monitoring runbook](monitoring.md).

The rejected alternative is worth stating, because it sounds reasonable: instances could register
themselves in Aerospike and publish periodic metric snapshots. That is a control plane, with its own
liveness, staleness and retention semantics — which is to say it is **a broker**, the exact component
ignisMQ's design declines to have. Adding one as a side effect of a monitoring feature would make the
library's central claim false.

## Endpoints

Mounted at `/ignismq/v1`. The dashboard is at `/ignisConsole`.

Both are relative to the application's Jersey root path: if you set `server.rootPath` the endpoints
move under it, while `/ignisConsole` is a servlet and does not. The dashboard derives the API base
from its own URL, so it follows a root path of `/` without configuration and needs the base adjusting
if you set one.

| Method | Path | View | Role |
|---|---|---|---|
| `GET` | `/queues` | Cluster | none |
| `GET` | `/queues/{name}` | Cluster, plus the instance view when this process serves it | none |
| `GET` | `/queues/{name}/shards` | Instance - `409` if this process does not serve the queue | none |
| `GET` | `/instance` | Instance | none |
| `GET` | `/instance/metrics` | Instance | none |
| `GET` | `/whoami` | The caller | none |
| `POST` | `/queues/{name}/shovel` | Instance, or cluster with `intervalSeconds` | `ignismq_operate` |
| `POST` | `/queues/{name}/sweep` | Cluster | `ignismq_operate` |
| `POST` | `/queues/{name}/consumers` | Instance | `ignismq_operate` |
| `POST` | `/queues/{name}/deactivate` | Cluster | `ignismq_deactivate` |

`GET /queues` lists deactivated queues too, marked `"active": false`. A deactivated queue is still a
queue an operator has to be able to look at.

### Reading a queue

```
GET /ignismq/v1/queues/orders
```

```json
{
  "queue": {
    "name": "orders",
    "active": true,
    "heldHere": true,
    "shards": 8,
    "concurrency": 6,
    "createdAt": 1700000000000,
    "messageExpirySeconds": 172800,
    "queueExpirySeconds": 172800,
    "sweepDurationMillis": 1200000,
    "handlerTimeoutMillis": 600000,
    "shovelConcurrency": null,
    "shovelIntervalSeconds": null,
    "maxBatchSize": null,
    "maxWaitTimeSeconds": null,
    "messageHandlerType": "orderHandler"
  },
  "depth": {"published": 120, "consumed": 100, "unconsumed": 20, "sidelined": 5, "shovelled": 2},
  "shards": [
    {"shard": "SHARD_0", "published": 100, "consumed": 90, "pending": 10},
    {"shard": "SHARD_1", "published": 20, "consumed": 10, "pending": 10}
  ],
  "instance": {"name": "orders", "consumers": 4, "shovels": 1,
               "shovelConcurrency": 2, "shovelIntervalSeconds": 600}
}
```

`depth`, `shards` and `instance` are `null` when this process does not serve the queue. A stored
shovel that was never configured is `null` rather than the `-1` that is stored, which would read as a
value.

### Shard balance

```
GET /ignismq/v1/queues/orders/shards
```

**There is no such thing as a hot key here, because there is no key.** `publish` picks a shard with
`ThreadLocalRandom.nextInt(shards)`, and a consumer fires from a randomly chosen *active* shard.
Nothing routes by content. (Ordering-key routing is a candidate feature, not a current one.)

So an even spread is the expected shape, and this view is for the opposite question: **is any shard
sitting far from where random assignment would have put it?** A shard well above the others is one
whose consumers are not keeping up with it — most often because messages on it are being retried,
sidelined slowly, or handled by something that has stalled. The dashboard marks a shard above
`max(3 x even share, even share + 50)`, and states the even share so you can judge it yourself. The
absolute floor stops a nearly empty queue flagging ordinary noise as an outlier.

What it is **not** evidence for is the shard count. Whether eight shards or thirty-two is right is a
question about contention on each shard's pointer counters and about discovery fan-out, and it is
answered by throughput under concurrency, not by this table.

`/shards` returns `409 Conflict` for a queue this instance has not adopted — a different answer from
`404`, which means the queue does not exist.

### The instance tab

`GET /instance` reports the process and the queues it is running. `GET /instance/metrics` reports
**every ignisMQ and Magazine meter this process currently holds**, read straight from the meter
registry so it cannot drift from what a scrape would report.

Magazine's meters are included deliberately: ignisMQ's own meters say nothing about the storage calls
underneath them, and an operator reading one without the other draws the wrong conclusion.

The dashboard groups them rather than listing them flat, because forty rows in name order answer no
question:

- **Process** — pools, schedulers, and anything not tied to one queue
- **One group per queue** — that queue's meters *and* the Magazine meters beneath it, including its
  sideline magazine, which carries its own tag value and is folded in and marked
- Free-text search across meter names and tag values, and a link from any queue on the Queues tab
  straight to its own meters here

Timers are shown as count, mean and max in milliseconds rather than as Micrometer's raw statistics.

**There is no cluster total on that tab and there cannot be one.** It is one process out of however
many are deployed. Scrape `ignismq.*` and `magazine.*` and aggregate in your metrics backend — the
[monitoring runbook](monitoring.md) is the companion to this tab.

### One console, one client

The header states the `clientId` and `farmId` everything on screen is scoped to. It is a label, not a
selector, and that is structural rather than a simplification: a bundle builds **one**
`IgnisMQManager` with one `clientId`, and the Aerospike set it reads is named
`<farmId>_<clientId>_ignis_queues`. A process can only see its own client's queues, so a picker would
have exactly one entry.

If one application really does run several managers, the console would have to be given all of them;
say so and it can be, but nothing in the bundle builds that shape today.

## Actions and the roles

Every mutating endpoint carries a role, and **deactivation carries a different one**.

| Role | Grants |
|---|---|
| `ignismq_operate` | Shovel, scheduled shovel, sweep, consumer scaling |
| `ignismq_deactivate` | Deactivation, and nothing else |

They are separate because deactivation cannot be undone by any API. The grant that lets an on-call
engineer drain a sideline at 3am should not also let them destroy a queue, and `ignismq_operate` is
the grant that gets handed out widely.

Deactivation additionally requires `?confirm=<queue name>`, checked **server-side**. A UI-only
confirmation protects nobody driving this over HTTP. **The bundle defines no
authentication of its own.** It registers Jersey's `RolesAllowedDynamicFeature`, so without a
`SecurityContext` supplied by your application the actions are closed and return `403`. Reads stay
open.

### How the console authenticates

**IgnisMQ ships no authentication and the dashboard has no login.** The role is read from the Jersey
`SecurityContext` that *your* application populates, so what the browser has to send depends entirely
on how your application authenticates:

| Your application uses | What the console does |
|---|---|
| **Session cookies** | Nothing to configure. Requests are sent with `credentials: 'same-origin'`, so the browser attaches them |
| **A bearer token** | The dashboard has no way to know your token, so there is a **Token** button in the header. Paste one and every request carries `Authorization: Bearer <token>` |
| **mTLS or a proxy that injects identity** | Nothing to configure, provided the proxy is in front of the console too |

The token is kept in `sessionStorage`, so it is gone when the tab closes. It is readable by any script
on that origin, so use a **short-lived** token and serve the console over HTTPS. It is never put in a
URL.

A `401` or `403` says which of the two situations you are in: whether credentials were sent at all, or
whether they were sent and simply lack the role.

### Asking what the caller may do

`GET /ignismq/v1/whoami` answers with the two grants, so the dashboard can disable an action rather
than offer it and let it fail:

```json
{
  "authenticated": true,
  "operate": true,
  "deactivate": false
}
```

`authenticated` is reported separately from the grants because the remedies differ: `false` means no
credentials arrived and a token is needed, while `true` with no grants means the credentials are fine
and a role is missing. The dashboard puts exactly that sentence in the disabled button's tooltip.

**It reports capability, never identity.** No principal name, no role list, nothing beyond what the
caller could already discover by attempting one `POST` - which is why it is open like the other reads.

It is a **courtesy, not a control**. Every mutating endpoint still enforces its own role server-side,
and if this call fails the dashboard disables every action rather than assuming permission.

Grant the role from your own authentication. Whatever supplies it, it has to run at
`Priorities.AUTHENTICATION`: `RolesAllowedDynamicFeature` evaluates at `Priorities.AUTHORIZATION`, so
a filter left at the default user priority populates the `SecurityContext` only after the role check
has already failed.

```java
@Priority(Priorities.AUTHENTICATION)
public final class OperatorRoleFilter implements ContainerRequestFilter {

    @Override
    public void filter(final ContainerRequestContext requestContext) {
        final Operator operator = authenticate(requestContext);
        requestContext.setSecurityContext(new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
                return operator::name;
            }

            @Override
            public boolean isUserInRole(final String role) {
                // Both roles are answered here. Checking only OPERATE_ROLE would leave
                // deactivation permanently denied, which is safe but silent.
                if (IgnisMQResource.OPERATE_ROLE.equals(role)) {
                    return operator.mayOperateQueues();
                }
                return IgnisMQResource.DEACTIVATE_ROLE.equals(role) && operator.mayDeactivateQueues();
            }

            @Override
            public boolean isSecure() {
                return true;
            }

            @Override
            public String getAuthenticationScheme() {
                return SecurityContext.BASIC_AUTH;
            }
        });
    }
}
```

### What each action actually changes

This is the part worth reading before pressing a button, because the blast radius is not uniform.

| Action | Scope | Notes |
|---|---|---|
| `shovel` without `intervalSeconds` | **This instance** | One-shot drain of the sideline back into the main magazine. Nothing is persisted, no other instance is affected |
| `shovel` with `intervalSeconds` | **Cluster** | Stores a repeating shovel. Every instance adopts it on its next refresh |
| `sweep` | **Cluster** | Re-homes anything a consumer claimed and never retired. Runs **on the calling thread**, so a large queue holds the request open for as long as the sweep takes |
| `consumers` | **This instance** | `?delta=2` adds two here, `?delta=-3` stops three here. The resulting count is persisted, but the consumers themselves are this process's. `delta=0` is a `400` |
| `deactivate` | **Cluster**, irreversible | Consumers stop on every instance and messages still in the queue are left where they are. There is no supported way to activate a queue again. Separate role, plus `?confirm=<queue name>` |

Creating queues is deliberately absent: a queue is only useful on an instance whose handler map
contains its `messageHandlerType`, and a console has no way to know which instances those are.

## Configuration

The console is configured through `IgnisMQContext`:

```java
@Override
protected IgnisMQContext context(final AppConfig config) {
    return IgnisMQContext.builder()
            .clientId(config.getServiceName())
            .farmId(config.getFarmId())
            .storage(new AerospikeStorage(config.getAerospike(), config.getNamespace()))
            .curatorFramework(curatorFramework)
            .console(ConsoleConfiguration.builder()
                    .enabled(true)
                    .dashboardEnabled(true)
                    .cacheSeconds(5)
                    .build())
            .build();
}
```

| Setting | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Mounts the resource. `false` registers nothing at all |
| `dashboardEnabled` | `true` | Serves the page at `/ignisConsole`. `false` keeps the JSON endpoints |
| `cacheSeconds` | `5` | How long the queue inventory and each queue's depth are held. `0` disables caching |

### Action feedback

Every action shows a toast: one while it is running, replaced by the result. All action buttons are
locked for the duration, not just the one clicked — they act on the same queue, and **a sweep runs
synchronously on the server and can take minutes on a large queue.** Without that, a button that
looks inert invites a second click, and the second click is a second sweep.

**`cacheSeconds` is not a detail.** Rendering the console is a storage read per queue, fanned out
over every shard. An open browser tab polls every thirty seconds; anything scripted polls harder. The
cache is what stops a console being a load source against the cluster serving production traffic,
and it is the same argument behind the three-minute TTL on the `ignis.queue.stats` gauge.

## Seeing it without a cluster

A demo application lives in the bundle's test sources. It boots the console against a stateful
in-memory stand-in for the manager, so there is nothing to install and no Aerospike to start:

```bash
mvn -B -pl ignismq-dw-bundle test-compile
mvn -B -pl ignismq-dw-bundle exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=com.phonepe.ignis.demo.ConsoleDemoApplication -Dexec.args=server
```

Then open <http://localhost:8080/ignisConsole/>.

Its three queues are chosen to show the states that are easiest to get wrong:

| Queue | Shows |
|---|---|
| `order-events` | Active and adopted here: an even shard spread with **one shard drifting behind**, which is the only thing that view can honestly diagnose |
| `payment-retries` | Active but not yet adopted here: configuration, and explicitly no depth |
| `legacy-imports` | Deactivated, and still listed |

The demo grants both roles unconditionally so the actions can be tried, and the
stand-in is stateful — scaling consumers and deactivating a queue really do change what the console
then shows. **A real deployment must not grant the role that way.**

## What is deliberately missing

Anything per-message: message search, sideline inspection with a failure reason, the age of the
oldest message, attempt counts.

Today a message **is** its payload. A sidelined message carries no record of why it was sidelined,
so a "sideline browser" could only show the payload — the least useful column. These views need a
message envelope first; faking them before that means inventing a side-channel that the real
envelope would then have to replace.
