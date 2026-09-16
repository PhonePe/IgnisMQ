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
| `MAX_ALLOWED_CONSUMERS_EXCEEDED` | Consumer or shovel cap | `createConsumers`/`increaseConsumers`/`createShovel` | Total would reach 100; 99 is the highest attainable | Reduce concurrency to 99 or below |
| `INVALID_SHOVEL_TIME_INTERVAL` | Bad interval | `shovel()`/`scheduleShoveling()`/`createQueue()` | >86400, or negative | Use 0..86400 |
| `INVALID_MESSAGE_HANDLER` | Handler not found | `createQueue`/`refreshQueues` | `messageHandlerType` not registered | Register via `initialiseMessageHandlers()` |
| `INTERNAL_ERROR` | Unexpected error | Various | Bug | Check logs |

## IgnisMQException

- Extends `RuntimeException` (unchecked)
- Field: `ErrorCode errorCode`
- Static `propagate()` methods

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
