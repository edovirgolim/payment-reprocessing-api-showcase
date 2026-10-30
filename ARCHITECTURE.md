# Architecture

## Overview

The API accepts a CSV or JSON file, stores it in S3, and returns a job reference immediately
(`202 Accepted`). A background pipeline then ingests the file and processes each payment item
independently. Clients poll two read endpoints to track progress.

The pipeline has three stages. The upload is synchronous; ingestion and item processing are
asynchronous and use SQS. A persistence outbox bridges database writes and queue publication.

```
  POST /reprocess          Outbox → SQS          SQS → Worker
 ┌──────────────┐        ┌───────────────┐      ┌─────────────────────┐
 │  1. Upload   │──────► │  2. Ingest    │────► │  3. Process items   │
 │   (sync)     │        │   (async)     │      │      (async)        │
 └──────────────┘        └───────────────┘      └─────────────────────┘
  returns jobId           reads S3, writes        one message per item,
  (202 Accepted)          items to DB             calls payment gateway
```

The upload request calculates a SHA-256 payload hash before storing the file in S3. No response-time
guarantee is made. PostgreSQL writes the job and its outbox event in one database transaction;
DynamoDB currently writes them in separate calls, so its outbox is not transactionally atomic.

---

## Module map

```
src/main/java/com/virgolim/paymentapi/
│
├── domain/
│   ├── model/          BatchJob, TransactionItem, PaymentType, ProcessingStatus
│   └── exception/      BusinessException, ResourceNotFoundException
│
├── usecase/
│   ├── UploadBatchFileUseCase        (sync, called by controller)
│   ├── IngestBatchJobUseCase         (async, called by SQS consumer)
│   ├── ProcessQueuedItemUseCase      (async, called by SQS consumer)
│   └── ProcessTransactionItemUseCase (called by ProcessQueuedItemUseCase)
│
└── infra/
    ├── web/            REST endpoints + DTOs
    ├── security/       JWT filter + SecurityConfig
    ├── extraction/     CSV and JSON streaming parsers
    ├── storage/        S3-compatible object storage
    ├── queue/          SqsQueueConsumers (polling) + OutboxPublisher (scheduled)
    ├── payment/        PaymentGatewayFactory + gateway adapters
    ├── persistence/
    │   ├── BatchJobRepository          (domain port / interface)
    │   ├── adapter/                    PostgreSQL implementation
    │   └── dynamodb/                   DynamoDB implementation  ← default profile
    └── config/         AwsClientConfig, SecurityConfig, PaymentCloudProperties
```

---

## Security

Every `/api/v1/payments/**` request must carry a Bearer JWT with the `PAYMENT_OPERATOR` role.
The filter runs before Spring Security's username/password authentication filter.

The `Authorization` header must use the `Bearer <JWT>` format. Although the API requires
`ROLE_PAYMENT_OPERATOR`, the current filter assigns that authority to every valid token; it does
not validate a role claim. A production identity integration should validate the caller's
authorization claims rather than granting the role unconditionally.

```
Client ──► JwtAuthenticationFilter ──► Spring Security ──► Controller
              verifies signature,            requires ROLE_PAYMENT_OPERATOR
              extracts sub, grants role
```

The filter grants `ROLE_PAYMENT_OPERATOR` to every successfully validated JWT; it does not read or
validate a role claim. The `sub` claim becomes the `ownerId` that scopes job reads, so a caller can
only read jobs associated with that subject. `GET /actuator/health` and its subpaths are public.
All other paths require authentication.

---

## Flow 1 — Upload (synchronous)

`POST /api/v1/payments/reprocess`

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Controller
    participant UploadUseCase
    participant S3
    participant DB

    Client->>Controller: POST /reprocess<br/>Idempotency-Key + file
    Controller->>UploadUseCase: submit(ownerId, idempotencyKey, file)

    UploadUseCase->>DB: findByIdempotencyKey(ownerId, key)

    alt Key already exists
        DB-->>UploadUseCase: existing BatchJob
        UploadUseCase->>DB: findPayloadHash(ownerId, key)
        DB-->>UploadUseCase: storedHash

        alt Hash matches
            UploadUseCase-->>Controller: Submission(existing=true, status)
            Controller-->>Client: 202 Already queued / 200 Already finalized
        else Hash differs
            UploadUseCase-->>Controller: BusinessException
            Controller-->>Client: 422 Different payload for same key
        end

    else New submission
        UploadUseCase->>S3: put(key, fileStream)
        S3-->>UploadUseCase: ok
        UploadUseCase->>DB: createJob(jobId, QUEUED)<br/>+ outbox event INGEST
        DB-->>UploadUseCase: saved
        UploadUseCase-->>Controller: Submission(existing=false, QUEUED)
        Controller-->>Client: 202 Queued — Job Tracking Reference: {jobId}
    end
```

**What is written:** S3 object + logical batch-job record (status `QUEUED`) + outbox event (type
`INGEST`). PostgreSQL commits the job and outbox event in one transaction. DynamoDB writes them
separately.

**Error handling:** missing/empty `Idempotency-Key` and unsupported file extension → 400;
empty file, missing filename extension, or a reused key with a different payload → 422. The
PostgreSQL implementation handles a unique-key race by deleting the orphaned S3 object and
returning the matching existing submission when its hash is the same. DynamoDB does not use a
conditional write for job creation, so the same race-safety guarantee does not apply there.

---

## Flow 2 — Outbox to SQS (scheduled)

`OutboxPublisher` runs every ~1 s by default (configurable) and bridges the persistence layer with
SQS. It reads at most ten eligible events per poll.

```mermaid
sequenceDiagram
    autonumber
    participant Publisher as OutboxPublisher<br/>(scheduled)
    participant DB
    participant SQS

    loop Every ~1 s
        Publisher->>DB: findPendingOutboxEvents(limit=10)
        DB-->>Publisher: list of unpublished events

        alt Events found
            Publisher->>SQS: sendMessageBatch(INGEST events → ingestion queue)
            Publisher->>SQS: sendMessageBatch(ITEM events → item queue)
            SQS-->>Publisher: successful IDs

            Publisher->>DB: markOutboxEventPublished(eventId) per success
        end
    end
```

**Why it exists:** the outbox allows pending work to be published after database writes. In
PostgreSQL, the job/item records and their outbox rows are committed transactionally. In DynamoDB,
the records and outbox events are separate calls, so a crash between calls can leave them
inconsistent. Failed SQS sends remain pending for a later poll; if sending succeeds but marking the
event as published fails, the event can be sent again. Delivery is therefore at least once, not
exactly once.

---

## Flow 3 — Ingestion (asynchronous)

`SqsQueueConsumers.pollIngestionQueue` → `IngestBatchJobUseCase`

```mermaid
sequenceDiagram
    autonumber
    participant SQS as SQS<br/>ingestion queue
    participant Consumer as SqsQueueConsumers
    participant IngestUC as IngestBatchJobUseCase
    participant S3
    participant Parser as CSV / JSON Parser
    participant DB

    Consumer->>SQS: receiveMessage (long-poll 10 s, visibility timeout 360 s)
    SQS-->>Consumer: message {jobId}

    Consumer->>IngestUC: ingest(jobId)

    IngestUC->>DB: findIngestionDetails(jobId)
    DB-->>IngestUC: details (extension, s3Key, status)

    IngestUC->>DB: claimIngestion(jobId)<br/>QUEUED → INGESTING + lease 5 min
    DB-->>IngestUC: claimed = true / false

    alt Already claimed by another worker
        IngestUC-->>Consumer: return (no-op)
    else Claimed
        IngestUC->>S3: get(s3Key)
        S3-->>IngestUC: file stream

        IngestUC->>Parser: parse (pass 1 — count items only)

        alt Validation error (bad format)
            Parser-->>IngestUC: IllegalArgumentException
            IngestUC->>DB: markJobFailed(jobId)
            IngestUC-->>Consumer: return
        else Valid
            Parser-->>IngestUC: itemCount

            IngestUC->>S3: get(s3Key)
            S3-->>IngestUC: file stream

            IngestUC->>Parser: parse (pass 2 — emit items)

            loop Every 10 items
                Parser-->>IngestUC: batch of TransactionItems
                IngestUC->>DB: saveItemBatch(jobId, batch)<br/>writes items (NEW) + outbox ITEM events
            end

            IngestUC->>DB: finishIngestion(jobId, totalItems)<br/>status → PROCESSING
            IngestUC->>S3: delete(s3Key)
            IngestUC-->>Consumer: return
        end
    end

    Consumer->>SQS: deleteMessage
```

**What is written per item:** transaction-item record (status `NEW`, with `failureReason`
pre-filled for `UNKNOWN` types) + outbox event (type `ITEM`). The item events are not eligible for
publication until ingestion is complete and the job is in `PROCESSING`.

**Unknown payment types (e.g. PIX):** accepted without error. The item is stored with `paymentType = UNKNOWN` and a `failureReason` set at parse time. The gateway is never called for these items.

---

## Flow 4 — Item processing (asynchronous)

`SqsQueueConsumers.pollItemQueue` → `ProcessQueuedItemUseCase` → `ProcessTransactionItemUseCase`

```mermaid
sequenceDiagram
    autonumber
    participant SQS as SQS<br/>item queue
    participant Consumer as SqsQueueConsumers
    participant QueuedUC as ProcessQueuedItemUseCase
    participant ProcessUC as ProcessTransactionItemUseCase
    participant Gateway as PaymentGatewayAdapter<br/>(CB + Bulkhead)
    participant DB

    Consumer->>SQS: receiveMessage (long-poll 10 s, visibility timeout 360 s)
    SQS-->>Consumer: message {itemId}

    Consumer->>QueuedUC: process(itemId)

    QueuedUC->>DB: claimItem(itemId)<br/>NEW → PROCESSING + lease 5 min
    DB-->>QueuedUC: claimed = true / false

    alt Not claimed — already terminal (SUCCESS or FAILED)
        QueuedUC-->>Consumer: return (no-op)
    else Not claimed — still PROCESSING (another worker holds lease)
        QueuedUC-->>Consumer: IllegalStateException → message retried by SQS
    else Claimed
        QueuedUC->>DB: findItem(itemId)
        DB-->>QueuedUC: TransactionItem

        alt paymentType = UNKNOWN
            QueuedUC->>DB: saveItemResult(FAILED, stored failureReason)
        else Duplicate (prior SUCCESS for same clientId + paymentId)
            QueuedUC->>DB: existsSuccessfulTransaction(clientId, paymentId)
            DB-->>QueuedUC: true
            QueuedUC->>DB: saveItemResult: SUCCESS, duplicate gateway call skipped
        else Normal processing
            QueuedUC->>ProcessUC: reprocess(item)
            ProcessUC->>Gateway: process(item)

            alt Gateway returns true
                Gateway-->>ProcessUC: SUCCESS
            else Gateway returns false
                Gateway-->>ProcessUC: FAILED — "gateway rejected"
            else Exception thrown
                Gateway-->>ProcessUC: FAILED — "gateway processing failed"
            else Circuit breaker open (fallback)
                ProcessUC-->>QueuedUC: FAILED — circuit breaker fallback result
            end

            ProcessUC-->>QueuedUC: TransactionItem(result)
            QueuedUC->>DB: saveItemResult(itemId, result)
        end

        Note over DB: saveItemResult:<br/>updates item status + lastProcessingDate<br/>increments job totalSuccess / totalFailed<br/>if all items done → finalise job status
    end

    Consumer->>SQS: deleteMessage
```

The consumer receives one message per poll with a 10-second long poll and 360-second visibility
timeout. A successful return from the use case causes the message to be deleted. Exceptions that
escape the use case are logged and leave the message for SQS redelivery after the visibility
timeout.

**Job finalisation rule (inside `saveItemResult`):**

```
if ingestionComplete AND totalSuccess + totalFailed >= totalItems:
    totalFailed  = 0            → COMPLETED
    totalSuccess = 0            → FAILED
    both > 0                    → PARTIALLY_COMPLETED
```

---

## Flow 5 — Reading results (synchronous)

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Controller
    participant DB

    Client->>Controller: GET /reprocess/{jobId}<br/>Authorization: Bearer <token>
    Controller->>DB: findByJobIdAndOwnerId(jobId, ownerId)

    alt Job not found or different owner
        DB-->>Controller: empty
        Controller-->>Client: 404 Not Found
    else Found
        DB-->>Controller: BatchJob (with failedItems list)
        Controller-->>Client: 200 BatchJobResultResponse<br/>{jobId, status, totalItems, reprocessedSuccessfully,<br/>notReprocessed, failedItems, failureReason}
    end

    Client->>Controller: GET /reprocess/{jobId}/items<br/>Authorization: Bearer <token>
    Controller->>DB: findByJobIdAndOwnerId(jobId, ownerId)

    alt Job not found or different owner
        DB-->>Controller: empty
        Controller-->>Client: 404 Not Found
    else Found
        Controller->>DB: findItemsByJobId(jobId)
        DB-->>Controller: all TransactionItems ordered by position
        Controller-->>Client: 200 List<TransactionItemResponse><br/>[{paymentId, paymentType, status, failureReason, …}]
    end
```

---

## Job and item lifecycle

**Batch job lifecycle**

```mermaid
stateDiagram-v2
    direction LR
    [*] --> QUEUED : createJob
    QUEUED --> INGESTING : claimIngestion
    INGESTING --> PROCESSING : finishIngestion, items greater than zero
    INGESTING --> COMPLETED : finishIngestion, no items
    INGESTING --> FAILED : validation error
    PROCESSING --> COMPLETED : all items succeeded
    PROCESSING --> PARTIALLY_COMPLETED : mix of successes and failures
    PROCESSING --> FAILED : all items failed
```

**Transaction item lifecycle**

```mermaid
stateDiagram-v2
    direction LR
    [*] --> NEW : saveItemBatch
    NEW --> PROCESSING : claimItem
    PROCESSING --> SUCCESS : gateway succeeded or duplicate skipped
    PROCESSING --> FAILED : gateway error, CUSTOM, or UNKNOWN type
```

---

## Persistence and profiles

`BatchJobRepository` is the single persistence port. Two implementations are provided:

| Profile | Implementation | Tables / collections |
|---|---|---|
| `dynamodb` (default) | `DynamoBatchJobRepository` | `payment-api-batch-jobs`, `payment-api-transaction-items`, `payment-api-outbox-events` |
| `postgresql` | `BatchJobRepositoryAdapter` (JPA) | `batch_jobs`, `transaction_items`, `outbox_events` |

The DynamoDB adapter uses three GSIs: `IdempotencyKeyIndex` (idempotency lookup), `BatchJobIndex`
(item-by-job queries), and `LedgerIndex` (duplicate transaction check).

The DynamoDB implementation does not make related writes across its tables atomic. PostgreSQL wraps
related persistence operations in database transactions, but neither backend makes database updates
and SQS publication atomic; the outbox does not provide exactly-once delivery end to end.

---

## Error handling summary

| Stage | Error | Outcome |
|---|---|---|
| Upload | Missing or empty `Idempotency-Key` | 400 — `IllegalArgumentException` |
| Upload | Empty file, missing extension, or different payload for an existing key | 422 — `BusinessException` |
| Upload | Unsupported file extension | 400 — `IllegalArgumentException` |
| Upload | PostgreSQL unique-key race with matching payload | Orphaned S3 object deleted; existing submission returned |
| Upload | DynamoDB concurrent creation with the same idempotency key | No conditional write; duplicate job creation is not prevented |
| Ingest | File format invalid (bad headers / dates) | `markJobFailed` → job `FAILED`, message deleted |
| Ingest | S3 read error | `UncheckedIOException` propagated; message retried by SQS |
| Item | `paymentType = UNKNOWN` | Item `FAILED` with original type in `failureReason`; no gateway call |
| Item | `paymentType = CUSTOM` (no adapter registered) | Item `FAILED` — "Payment gateway processing failed." |
| Item | Gateway returns false | Item `FAILED` — "Payment gateway rejected reprocessing." |
| Item | Exception in gateway | Item `FAILED` — "Payment gateway processing failed." |
| Item | Circuit breaker open | Fallback records an `FAILED` item result; use case returns normally and the message is deleted (not retried by SQS) |
| Item | Item is not claimed and is not terminal | `IllegalStateException`; message remains for SQS redelivery after the visibility timeout |
| Item | Already terminal (SUCCESS/FAILED) | Silent no-op; message deleted |
| Read | Job not found or wrong owner | 404 — `ResourceNotFoundException` |

---

## Production limitations (showcase scope)

- **No DLQs.** Messages that keep failing are retried until SQS retention expires. Add DLQs and
  redrive policies to both queues before production use.
- **Non-atomic DynamoDB writes.** Job and outbox events, item records and item outbox events, and
  item results and job counters are written in separate calls. A crash between calls can cause
  inconsistency. In particular, DynamoDB `markJobFailed` does not update or fail already-created
  item records, unlike the PostgreSQL implementation. Production deployments should use DynamoDB
  transactions and conditional updates.
- **Outbox polling.** `findPendingOutboxEvents` scans the full table in DynamoDB. PostgreSQL uses
  an eligibility query joining outbox events, jobs, and (for item events) transaction items.
  At scale, assess a time-partitioned GSI or a CDC-based publisher.
- **Simulated gateways.** `CreditCardGatewayAdapter`, `DebitCardGatewayAdapter`, and
  `BankTransferGatewayAdapter` always return `true`. Real integrations must use provider-supported
  idempotency keys and reconcile uncertain outcomes.
- **Single EC2 instance.** The AWS environment runs one host. There is no horizontal scaling,
  auto-recovery, or scheduled shutdown.
