# Architecture

## Overview

The API accepts a CSV or JSON file, stores it in S3, and returns a job reference immediately
(`202 Accepted`). A background pipeline then ingests the file and processes each payment item
independently. Clients poll two read endpoints to track progress.

The pipeline has three asynchronous stages connected by the Transactional Outbox pattern and SQS:

```
  POST /reprocess          Outbox → SQS          SQS → Worker
 ┌──────────────┐        ┌───────────────┐      ┌─────────────────────┐
 │  1. Upload   │──────► │  2. Ingest    │────► │  3. Process items   │
 │   (sync)     │        │   (async)     │      │      (async)        │
 └──────────────┘        └───────────────┘      └─────────────────────┘
  returns jobId           reads S3, writes        one message per item,
  in < 1s                 items to DB             calls payment gateway
```

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
The filter runs before Spring Security's authentication filter.

```
Client ──► JwtAuthenticationFilter ──► Spring Security (role check) ──► Controller
              extracts sub as ownerId       hasRole(PAYMENT_OPERATOR)
```

The `sub` claim becomes the `ownerId` that scopes all DB reads — a user can only see their own jobs.
Actuator health endpoints are public. All other paths require authentication.

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
            Controller-->>Client: 400 Different payload for same key
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

**What is written:** S3 object + `batch_job` row (status `QUEUED`) + `outbox_event` row (type `INGEST`).  
**Error handling:** empty file or missing extension → `BusinessException` → 400. Race condition on unique key → S3 orphan deleted, idempotent retry.

---

## Flow 2 — Outbox to SQS (scheduled)

`OutboxPublisher` runs every ~1 s and bridges the local DB with SQS.

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

**Why it exists:** writing the outbox row in the same DB transaction as the job/item data means no
message is lost if the process crashes between DB commit and SQS send. Failed SQS sends are retried
on the next poll cycle (at-least-once delivery).

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

    Consumer->>SQS: receiveMessage (long-poll 10 s)
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

**What is written per item:** `transaction_item` row (status `NEW`, with `failureReason` pre-filled for `UNKNOWN` types) + `outbox_event` row (type `ITEM`).  
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

    Consumer->>SQS: receiveMessage (long-poll 10 s)
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
            QueuedUC->>DB: saveItemResult(SUCCESS, "duplicate skipped")
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
                ProcessUC-->>QueuedUC: FAILED — "circuit breaker open"
            end

            ProcessUC-->>QueuedUC: TransactionItem(result)
            QueuedUC->>DB: saveItemResult(itemId, result)
        end

        Note over DB: saveItemResult:<br/>updates item status + lastProcessingDate<br/>increments job totalSuccess / totalFailed<br/>if all items done → finalise job status
    end

    Consumer->>SQS: deleteMessage
```

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
        Controller-->>Client: 200 BatchJobResultResponse<br/>{status, totalItems, reprocessedSuccessfully,<br/>notReprocessed, failedItems, failureReason}
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

```mermaid
stateDiagram-v2
    direction LR

    state "BatchJob" as JOB {
        [*] --> QUEUED      : createJob
        QUEUED --> INGESTING : claimIngestion
        INGESTING --> PROCESSING : finishIngestion (items > 0)
        INGESTING --> COMPLETED  : finishIngestion (items = 0)
        INGESTING --> FAILED     : markJobFailed (validation error)
        PROCESSING --> COMPLETED          : all items SUCCESS
        PROCESSING --> PARTIALLY_COMPLETED : mix SUCCESS + FAILED
        PROCESSING --> FAILED             : all items FAILED
    }

    state "TransactionItem" as ITEM {
        [*] --> NEW        : saveItemBatch
        NEW --> PROCESSING  : claimItem
        PROCESSING --> SUCCESS : gateway ok
        PROCESSING --> FAILED  : gateway error / UNKNOWN type
    }
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

Neither implementation makes all writes atomic/conditional across multiple tables. The outbox model
reduces the risk of lost messages but does not provide exactly-once delivery end to end.

---

## Error handling summary

| Stage | Error | Outcome |
|---|---|---|
| Upload | Missing or empty `Idempotency-Key` | 400 — `IllegalArgumentException` |
| Upload | Empty file | 400 — `BusinessException` |
| Upload | Unsupported file extension | 400 — `BusinessException` |
| Upload | Same key, different payload hash | 400 — `BusinessException` |
| Upload | Race condition (duplicate DB key) | S3 orphan deleted; idempotent response returned |
| Ingest | File format invalid (bad headers / dates) | `markJobFailed` → job `FAILED`, message deleted |
| Ingest | S3 read error | `UncheckedIOException` propagated; message retried by SQS |
| Item | `paymentType = UNKNOWN` | Item `FAILED` with original type in `failureReason`; no gateway call |
| Item | Gateway returns false | Item `FAILED` — "gateway rejected reprocessing" |
| Item | Exception in gateway | Item `FAILED` — "gateway processing failed" |
| Item | Circuit breaker open | Item `FAILED` — "circuit breaker open; will retry" |
| Item | Concurrent worker holds lease | `IllegalStateException`; message retried by SQS |
| Item | Already terminal (SUCCESS/FAILED) | Silent no-op; message deleted |
| Read | Job not found or wrong owner | 404 — `ResourceNotFoundException` |

---

## Production limitations (showcase scope)

- **No DLQs.** Messages that keep failing are retried until SQS retention expires. Add DLQs and
  redrive policies to both queues before production use.
- **Non-atomic DynamoDB writes.** Job and outbox events are written in separate calls; a crash
  between them can cause inconsistency. Production deployments should use DynamoDB transactions.
- **Outbox table scan.** `findPendingOutboxEvents` scans the full table (DynamoDB) or uses an
  index (PostgreSQL). At scale, replace with a time-partitioned GSI or a CDC-based publisher.
- **Simulated gateways.** `CreditCardGatewayAdapter`, `DebitCardGatewayAdapter`, and
  `BankTransferGatewayAdapter` always return `true`. Real integrations must use provider-supported
  idempotency keys and reconcile uncertain outcomes.
- **Single EC2 instance.** The AWS environment runs one host. There is no horizontal scaling,
  auto-recovery, or scheduled shutdown.
