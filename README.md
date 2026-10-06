# Hypercube Amazon SQS Queue

An Amazon SQS Standard queue implementation of the [go-hypercube queue contract](https://pkg.go.dev/github.com/go-hypercube/go-hypercube/queue), built on AWS SDK for Go v2.

It implements:

- `queue.Queue`
- `queue.VisibilityExtender`
- `queue.RedriveProvider`

`queue.StatsProvider` is intentionally not implemented. SQS exposes approximate counts for known physical URLs, but it cannot reliably enumerate logical queue names when custom URL resolvers are used.

## Features

- Existing SQS queues resolved from Hypercube logical queue names
- SQS long polling with a configurable overall empty-pop timeout
- SQS batch operations chunked at 10 entries
- Send batches also packed under SQS's 1 MiB aggregate message-body limit
- Binary-safe JSON envelope for all message fields
- Delayed push and retry, rounded up to whole seconds
- Per-message/default visibility timeouts and visibility extension
- Attempts reconstructed from a persisted base plus SQS `ApproximateReceiveCount`
- Delivery-specific receipt handles with local expiry/current-delivery validation
- Updated retry and dead-letter snapshots published before source deletion
- Explicit per-source DLQs and programmatic manual redrive
- Atomic logical-ID reservation through a pluggable `IDStore`
- Partial batch errors with original input indexes
- No hidden goroutines and no resources owned by the driver

## Installation

```sh
go get github.com/go-hypercube/hypercube-queue-sqs
```

## AWS resource requirements

This driver does **not** create, update, or delete queues. Provision these resources before starting producers or workers:

1. One **Standard** SQS queue for each logical queue name, for example `emails`.
2. One explicit dead-letter queue for each source, named `<source>-dlq` by default, for example `emails-dlq`.
3. Queue-level `DelaySeconds=0` on source queues and explicit DLQs.
4. Queue-level `ReceiveMessageWaitTimeSeconds=0` if the application uses `WithPollingTimeout(0)` and expects a truly immediate empty check.

The AWS SDK omits zero-valued per-request `DelaySeconds` and `WaitTimeSeconds` fields. Consequently, a nonzero queue-level default would override the driver's intended immediate delivery or immediate receive behavior.

The AWS identity needs these SQS actions for every relevant source and explicit DLQ:

- `sqs:GetQueueUrl`
- `sqs:SendMessage`
- `sqs:ReceiveMessage`
- `sqs:DeleteMessage`
- `sqs:ChangeMessageVisibility`

Encrypted queues also require the corresponding KMS permissions.

## Quick start

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/go-hypercube/go-hypercube/queue"
    sqsqueue "github.com/go-hypercube/hypercube-queue-sqs"
)

func main() {
    ctx := context.Background()

    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        log.Fatal(err)
    }

    q := sqsqueue.New(
        sqs.NewFromConfig(cfg),
        sqsqueue.WithPollingTimeout(20*time.Second),
        sqsqueue.WithDefaultVisibilityTimeout(30*time.Second),
    )

    msg := &queue.Message{
        QueueName: "emails",
        Namespace: "app",
        JobName:   "send-welcome-email",
        Payload:   []byte(`{"user_id":42}`),
    }
    if err := q.Push(ctx, msg); err != nil {
        log.Fatal(err)
    }

    messages, err := q.Pop(ctx, "emails", 10)
    switch {
    case errors.Is(err, queue.ErrEmpty):
        return
    case err != nil:
        log.Fatal(err)
    }

    for _, delivery := range messages {
        // Process delivery.Payload. SQS is at-least-once, so handlers must be
        // idempotent.
        if err := q.Ack(ctx, delivery); err != nil {
            log.Printf("ack: %v", err)
        }
    }
}
```

The caller owns the AWS client and configuration. `New` performs no network request.

## Queue URL resolution

By default:

- source `emails` is resolved with `GetQueueUrl(QueueName="emails")`;
- its explicit DLQ is resolved with `GetQueueUrl(QueueName="emails-dlq")`.

Successful resolutions are cached for the life of the `Queue`.

Use custom resolvers for cross-account queues, physical naming conventions, fixed URLs, LocalStack, or other SQS-compatible endpoints:

```go
q := sqsqueue.New(client,
    sqsqueue.WithQueueURLResolver(func(ctx context.Context, name string) (string, error) {
        return sourceURLs[name], nil
    }),
    sqsqueue.WithDeadLetterQueueURLResolver(func(ctx context.Context, name string) (string, error) {
        return deadLetterURLs[name], nil
    }),
)
```

The DLQ resolver receives the original logical source name. When only the naming suffix differs, use:

```go
sqsqueue.WithDeadLetterSuffix("-failed")
```

A source and explicit DLQ must not resolve to the same URL.

## Configuration

| Option | Default | Behavior |
|---|---:|---|
| `WithPollingTimeout` | `20s` | Overall duration an empty `Pop` waits before `queue.ErrEmpty`. SQS requests are split at its 20-second long-poll limit. Non-positive values perform one receive. |
| `WithPollingInterval` | `100ms` | Pause before another receive when SQS returns empty earlier than requested. Non-positive values are ignored. |
| `WithDefaultVisibilityTimeout` | `30s` | Lease used when a message has zero `VisibilityTimeout`. Must be in `(0, 12h]`; invalid configuration is reported before receive. |
| `WithIDStore` | new process-local store | Atomic logical-ID reservation backend. Use a shared/distributed implementation for broader uniqueness. |
| `WithQueueURLResolver` | `GetQueueUrl(name)` | Maps a logical source queue name to a URL. |
| `WithDeadLetterQueueURLResolver` | `GetQueueUrl(name + "-dlq")` | Maps a logical source queue name to its explicit DLQ URL. |
| `WithDeadLetterSuffix` | `-dlq` | Changes default explicit-DLQ naming. |

## Logical message IDs

`Push` reserves every logical `Message.ID` before publishing. If `ID` is empty, the driver assigns a collision-resistant value. A duplicate reservation fails that item with `queue.ErrAlreadyExists`.

The default `MemoryIDStore` enforces uniqueness only in the current process and only among `Queue` values sharing that store. A single `Queue` uses one automatically:

```go
ids := sqsqueue.NewMemoryIDStore()
producer := sqsqueue.New(client, sqsqueue.WithIDStore(ids))
consumer := sqsqueue.New(client, sqsqueue.WithIDStore(ids))
```

For multiple application processes, implement `sqsqueue.IDStore` with a strongly consistent backend such as a DynamoDB conditional item:

```go
type IDStore interface {
    Reserve(ctx context.Context, id string) (token string, reserved bool, err error)
    Release(ctx context.Context, id, token string) error
}
```

Requirements:

- `Reserve` must atomically create an ID only when absent.
- Its isolation scope must cover every logical queue that shares ID uniqueness.
- It must return a nonempty, unguessable ownership token.
- `Release` must compare that token before deletion and must not remove a newer reservation.
- A missing reservation should be treated as an idempotent successful release.

The token is persisted inside the driver-owned SQS envelope, so any consumer can release the reservation after a confirmed `Ack`.

A transport-level send error has an unknown outcome. The driver deliberately retains the reservation in that case. An explicit failed SQS batch entry is known not to have been accepted, so its reservation is released.

SQS retention expiry, external purge, queue deletion, and automatic DLQ expiry happen outside the driver. A distributed ID store therefore needs an operator-defined reconciliation or reservation-retention policy; otherwise IDs for externally removed messages can remain reserved.

## Message lifecycle

```text
Push             -> source pending
Pop              -> source in flight
Ack              -> source removed
Retry            -> updated source copy published -> original source receipt deleted
DeadLetter       -> updated explicit-DLQ copy published -> original source receipt deleted
ExtendVisibility -> source remains in flight with replacement visibility
Redrive          -> reset source copy published -> explicit-DLQ receipt deleted
```

### Push and batches

A variadic push may span logical queues. The driver groups entries by resolved queue URL and then packs calls under both SQS limits:

- at most 10 entries per batch;
- at most 1 MiB of encoded message bodies per batch.

A single encoded body larger than 1 MiB fails with `sqsqueue.ErrMessageTooLarge`. The effective raw payload limit is lower because JSON metadata and base64 encoding consume part of the body. A queue may also be configured with a smaller `MaximumMessageSize`; SQS then returns an item failure.

Positive delays are rounded upward to whole seconds and must not exceed 15 minutes. Non-positive delays are immediate. Caller-provided `Attempt` and `DriverData` are ignored on push.

### Pop, attempts, and visibility

`Pop` returns at most 10 messages because that is SQS's receive limit; returning fewer than the requested maximum is allowed by the portable contract.

Every receive requests `ApproximateReceiveCount`. The returned attempt is:

```text
persisted base attempt + SQS ApproximateReceiveCount
```

A normal first delivery has attempt 1. Retry republishes the completed attempt as the new base, so its next delivery increments again.

SQS applies one visibility timeout to an entire receive request. The driver receives with its configured default, decodes each message, then uses `ChangeMessageVisibilityBatch` for messages with an explicit per-message timeout. A message is never returned when that adjustment is known to have failed; the driver attempts to make the received set visible again first.

Visibility values are rounded upward to whole seconds and must be in `(0, 12h]`. SQS measures its 12-hour ceiling from the original receive, so setting a value very close to 12 hours after receive can still be rejected by the service.

### Retry and explicit dead-lettering

SQS message bodies are immutable. `Retry` and `DeadLetter` therefore never use a native NACK to represent the updated message:

1. Encode the caller's current `Payload`, `Extra`, `Delay`, attempt, routing, and visibility state.
2. Publish that snapshot to the source queue for Retry or the explicit DLQ for DeadLetter.
3. Delete the exact original source receipt only after confirmed publication.

This ordering intentionally favors duplicates over message loss. If publication succeeds but deletion fails, the updated copy and original can both remain. A confirmed publication is staged in the current `Queue` instance, so repeating the same settlement retries deletion without publishing another copy. That staging is process-local; it cannot survive a restart or disambiguate a lost network response.

The driver never chooses an application attempt limit and never automatically dead-letters a job.

### Redrive

`Redrive(ctx, queueName, count)` manually receives messages from the configured explicit DLQ. For each message it:

1. preserves ID, routing fields, payload, metadata, visibility, and ID reservation token;
2. resets attempt and delay to zero;
3. publishes the reset snapshot to the source queue;
4. deletes the DLQ receipt only after publication succeeds.

The returned count includes only messages for which both publication and DLQ deletion completed. On a later error, a source copy and DLQ copy may both remain by design.

Native `StartMessageMoveTask` is not used because it cannot rewrite the body to reset the portable attempt and delay fields.

## Automatic SQS redrive policy

Explicit `Queue.DeadLetter` and an SQS queue `RedrivePolicy` are different mechanisms.

This driver does not configure or disable broker-managed redrive. If a source queue has a native redrive policy, SQS can move the original immutable body without an explicit `DeadLetter` call. That move competes with application retry policy and can happen before the application records its latest failure metadata.

Prefer one of these configurations:

- disable native redrive and use explicit Hypercube dead-lettering; or
- set native `maxReceiveCount` higher than every application retry limit so it acts only as crash-loop protection.

When native redrive is enabled, configure its target consistently with this driver's explicit DLQ resolver.

## Batch errors and cancellation

`Push`, `Ack`, `Retry`, and `DeadLetter` return `*queue.BatchError` for item-level failures. Valid siblings continue when possible.

```go
if err := q.Retry(ctx, deliveries...); err != nil {
    var batch *queue.BatchError
    if errors.As(err, &batch) {
        for _, item := range batch.Failed {
            log.Printf("item %d: %v", item.Index, item.Err)
        }
    }
}
```

Common item errors:

- `queue.ErrInvalidArgument` for nil messages and unsupported duration values;
- `queue.ErrAlreadyExists` for an already-reserved pushed ID;
- `queue.ErrNotFound` for a locally stale/expired delivery or a receipt SQS rejects as stale;
- `sqsqueue.ErrMessageTooLarge` for a body over 1 MiB;
- `sqsqueue.ErrTransitionInProgress` after another publish-before-delete transition was staged;
- `*sqsqueue.BatchEntryError` for an SQS batch item failure.

Empty batches return nil without consulting the context or contacting AWS. Context cancellation is returned directly rather than hidden in a `BatchError`. As with every remote backend, cancellation does not prove an already-submitted AWS request had no effect.

## SQS-specific guarantees and limitations

SQS is an at-least-once immutable broker, so several portable concepts can only be approximated:

- **Receipt freshness across processes:** the driver rejects locally expired, superseded, or settled delivery tokens. It maps SQS `MessageNotInflight` and invalid-receipt responses to `queue.ErrNotFound`. However, AWS documents that deleting with an old receipt handle can return success without deleting the message. Another process's newer receipt cannot be discovered atomically. Strict cross-process current-delivery verification is not available from SQS alone.
- **Attempt counts:** `ApproximateReceiveCount` counts broker receives, not only successful `Pop` returns. Lost responses, malformed envelopes, failed visibility adjustment, or SDK/service retries can make `Attempt` higher than the number of handler invocations.
- **Per-message visibility:** receive and per-message adjustment are two SQS calls. A failure between them can leave an unreturned message temporarily invisible or increment its receive count.
- **Producer duplicates:** Standard queues have no producer deduplication. Ambiguous transport failures, SDK retries, and publish-before-delete failures can create multiple physical SQS messages carrying the same logical ID.
- **ID uniqueness:** the default store is process-local. Strict cross-process uniqueness requires a distributed `IDStore`.
- **Retention:** SQS removes messages after the queue's configured retention period (at most 14 days), independently of this driver. Active and dead-letter messages are not retained forever.
- **Ordering:** Standard SQS ordering is best-effort. Callers must not rely on FIFO behavior.
- **In-flight quotas:** reaching SQS's in-flight quota can make long polls return empty rather than producing an explicit over-limit error.

Use idempotent handlers. If strict atomic transitions, exact attempts, indefinite retention, or strongly verified stale-settlement rejection are required, use a backend that can store those states transactionally.

## FIFO queues

FIFO queues are rejected with `sqsqueue.ErrFIFOQueueUnsupported`. FIFO does not support the per-message delay used by this contract, and content/deduplication behavior conflicts with republishing updated retry snapshots.

## Testing

The test suite uses a deterministic fake AWS client. It requires no AWS credentials, network access, Docker daemon, LocalStack, or real queues.

```sh
make test
make race
make lint

# Equivalent commands:
go test ./...
go test -race -count=1 ./...
go vet ./...
```

Tests cover interface conformance, input ownership, nil and partial batch errors, ID duplicates and reuse, count/byte batch packing, polling and cancellation, attempt reconstruction, per-message visibility, stale delivery rejection, explicit receipt failures, publish-before-delete retry/dead-letter transitions, staged deletion retry, and manual redrive.
