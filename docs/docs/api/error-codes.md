# Error Codes

Complete error code reference for IgnisMQ.

## ErrorCode Enum

`public enum ErrorCode`

| Error Code | Description | Thrown By | Common Cause | Resolution |
|---|---|---|---|---|
| `QUEUE_ALREADY_EXISTS` | Queue name already taken | `createQueue()` | Duplicate name | Use unique name or check `getAllQueues()` |
| `QUEUE_NOT_FOUND` | Queue not in memory | `getQueue()` | Not created/expired/deactivated | Create queue first, check lifecycle |
| `NOT_IMPLEMENTED` | Unsupported storage | Nothing today | Unreachable: `BaseStorage` is sealed to `AerospikeStorage` | — |
| `AEROSPIKE_ERROR` | Aerospike failure | `AerospikeQueueService` | Network/timeout/namespace | Check connectivity, retries |
| `INVALID_REQUEST` | Validation failed | `createQueue()` | `queueExpiry < messageExpiry`, or a TTL over the allowed maximum. **Bean-validation annotations such as `@NotBlank` are not run by `createQueue`** | Fix request |
| `MAX_ALLOWED_CONSUMERS_EXCEEDED` | Consumer or shovel cap | `createConsumers`/`increaseConsumers`/`createShovel` | Total would exceed 100 | Reduce concurrency |
| `INVALID_SHOVEL_TIME_INTERVAL` | Bad interval | `scheduleShoveling()`/`createQueue()` | >86400, or negative. `shovel(int)` passes a fixed `0` and cannot raise it | Use 0..86400 |
| `INVALID_MESSAGE_HANDLER` | Handler not found | `createQueue`/`refreshQueues` | `messageHandlerType` not registered | Register via `initialiseMessageHandlers()` |
| `INTERNAL_ERROR` | Unexpected error | Various | Bug | Check logs |

## IgnisMQException

- Extends `RuntimeException` (unchecked)
- Field: `ErrorCode errorCode`
- Static `propagate()` methods

## Over HTTP

The Dropwizard bundle registers `IgnisMQExceptionMapper`, so an `IgnisMQException` escaping a Jersey
resource becomes the status its cause deserves instead of a blanket `500`. The dividing line is
whether the caller can do anything about it. Retrying a request to scale past the consumer cap will
not help until something else changes, whereas a storage failure may well succeed on the next
attempt.

| Error Code | Status | Why |
|---|---|---|
| `QUEUE_NOT_FOUND` | `404` | The named queue is not there |
| `QUEUE_ALREADY_EXISTS` | `409` | The request conflicts with what exists |
| `MAX_ALLOWED_CONSUMERS_EXCEEDED` | `409` | Well-formed, and refused by the **current state**. Not a `400`: the same request succeeds against a queue with fewer consumers, which is exactly what `409` means |
| `INVALID_REQUEST` | `400` | The request itself is wrong |
| `INVALID_SHOVEL_TIME_INTERVAL` | `400` | As above |
| `INVALID_MESSAGE_HANDLER` | `400` | As above |
| `NOT_IMPLEMENTED` | `501` | Unreachable today |
| `AEROSPIKE_ERROR` | `503` | Not the caller's doing, and retryable — which `503` says and `500` does not |
| `INTERNAL_ERROR` | `500` | Ours |

The body names the code, so a client can branch on it without parsing prose:

```json
{
  "errorCode": "MAX_ALLOWED_CONSUMERS_EXCEEDED",
  "message": "Total consumers would exceed 100"
}
```

!!! note "A 5xx body carries no message"
    For any `5xx` this mapper produces, the `message` is the fixed string `The request could not be completed.` and the real
    one is logged. A storage failure's message is a driver string naming nodes and error numbers,
    which belongs in your logs rather than in a response.

The mapper is registered **whether or not the console is enabled** — the exception is the library's,
not the console's, and an application calling IgnisMQ from its own resources is exactly the one whose
callers see the `500` today.

## Error Handling Example

```java
try {
    manager.createQueue(request);
} catch (IgnisMQException e) {
    switch (e.getErrorCode()) {
        case QUEUE_ALREADY_EXISTS -> log.warn("Queue exists: {}", request.getName());
        case INVALID_REQUEST -> log.error("Bad request: {}", e.getMessage());
        case AEROSPIKE_ERROR -> { /* retry logic */ }
        default -> throw e;
    }
}
```

## Best Practices

!!! tip "Transient errors"
    `AEROSPIKE_ERROR` is often transient — implement retry logic with exponential backoff.

!!! warning "QUEUE_NOT_FOUND after startup"
    `refreshQueues()` hasn't run yet. It runs on a 5-minute interval with a 1-minute initial delay. Wait for the first refresh or call it manually.

!!! important "Handler registration order"
    Always register handlers via `initialiseMessageHandlers()` **before** creating queues. Otherwise `INVALID_MESSAGE_HANDLER` will be thrown.
