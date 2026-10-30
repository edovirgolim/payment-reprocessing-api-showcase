# Payment Reprocessing API

[![Architecture & Diagrams](https://img.shields.io/badge/docs-Architecture%20%26%20Diagrams-1976D2?style=for-the-badge&logo=gitbook&logoColor=white)](docs/ARCHITECTURE.md)

## Introduction

Processing transaction batches from files introduces reliability challenges: uploads can be retried after transient failures, clients can submit the same batch more than once, and asynchronous workers can receive messages more than once. Without suitable safeguards, duplicate work can waste resources and, in a real payment integration, could contribute to duplicate operations.

This Java 21 and Spring Boot showcase demonstrates an asynchronous processing pipeline with a clear separation between file submission and background execution. It accepts CSV and JSON files, parses records incrementally using format-specific strategies, stores uploads in S3-compatible storage, and dispatches ingestion and item work through SQS. A persistence outbox coordinates work publication, while job-level idempotency keys and item state/payment keys help reduce duplicate processing.

The project is intentionally a learning and presentation showcase, not an enterprise-grade or production-ready payment system. Its DynamoDB implementation does not make all persistence and outbox operations atomic or conditional; SQS provides at-least-once delivery; the payment gateways are simulated; and the local JWT setup is not connected to a production identity provider. Therefore, the system is not immune to duplicate operations and does not provide an exactly-once payment guarantee. Production use would require stronger transactional and concurrency controls, provider-supported idempotency and reconciliation, operational monitoring, and DLQs with controlled redrive.

The default persistence profile is DynamoDB; PostgreSQL remains available as a selectable relational implementation. Both local options can run against LocalStack for S3 and SQS, so a presentation does not require an AWS account or live cloud resources.

## What is implemented

- Accepts CSV and JSON files up to 10 MB.
- Streams uploads to S3-compatible storage, validates records as a stream, and persists at most ten parsed items per ingestion batch.
- Uses an ingestion SQS queue and an item-processing SQS queue, with a persistence outbox for publishing work.
- Persists job/item state and idempotency metadata in DynamoDB by default, or PostgreSQL when the `postgresql` Spring profile is selected.
- Requires a JWT with the `PAYMENT_OPERATOR` role. Job reads are scoped to the JWT subject.
- Uses placeholder payment gateways; they are for demonstration only and do not initiate real payments.
- Exposes a per-job item listing endpoint (`GET /api/v1/payments/reprocess/{jobId}/items`) that returns all transaction items ordered by position, including their status and failure reason.
- Accepts any `paymentType` string at ingestion; unrecognised types (e.g. `PIX`) are stored as `UNKNOWN` and marked `FAILED` with a descriptive reason at processing time without affecting other items in the same batch.

This is a showcase, not a production payment system. In particular, DynamoDB updates are not all transactional/conditional, the outbox currently scans its table, gateway calls are simulated, and the JWT setup is not connected to a production identity provider.

## Repository map

```text
src/main/java/com/virgolim/paymentapi/
├── domain/                 # Domain models and exceptions
├── usecase/                # Upload, ingestion, and item-processing workflows
└── infra/
    ├── extraction/         # Streaming CSV/JSON parsers
    ├── persistence/        # DynamoDB and PostgreSQL adapters
    ├── queue/              # SQS consumers and outbox publisher
    ├── storage/            # S3-compatible object storage
    ├── security/           # JWT authentication
    └── web/                # REST endpoints and response DTOs
terraform/                  # Single-environment AWS resources
scripts/localstack-init.sh  # Local S3/SQS/DynamoDB bootstrap
docs/ARCHITECTURE.md        # Pipeline diagrams and component reference
```

## Architecture

The three-stage async pipeline — upload, ingestion, and item processing — is fully documented with
sequence diagrams, a state diagram for the job/item lifecycle, and a complete error-handling
reference in **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

| Section | What it covers |
|---|---|
| [Overview](docs/ARCHITECTURE.md#overview) | Three-stage pipeline at a glance |
| [Module map](docs/ARCHITECTURE.md#module-map) | Package structure with annotations |
| [Flow 1 — Upload](docs/ARCHITECTURE.md#flow-1--upload-synchronous) | Idempotency, SHA-256, S3 upload, race condition handling |
| [Flow 2 — Outbox → SQS](docs/ARCHITECTURE.md#flow-2--outbox-to-sqs-scheduled) | How the outbox bridges DB writes and SQS delivery |
| [Flow 3 — Ingestion](docs/ARCHITECTURE.md#flow-3--ingestion-asynchronous) | Two-pass S3 parsing, lease claim, item batch writes |
| [Flow 4 — Item processing](docs/ARCHITECTURE.md#flow-4--item-processing-asynchronous) | Gateway call, circuit breaker, UNKNOWN type, deduplication |
| [Flow 5 — Reading results](docs/ARCHITECTURE.md#flow-5--reading-results-synchronous) | GET job status and item list endpoints |
| [Job and item lifecycle](docs/ARCHITECTURE.md#job-and-item-lifecycle) | State diagrams for `BatchJob` and `TransactionItem` |
| [Error handling summary](docs/ARCHITECTURE.md#error-handling-summary) | Every failure path mapped to its outcome |


## Run locally with DynamoDB and LocalStack

Prerequisites: Java 21, Docker with the Compose plugin, and Python 3 for the local JWT helper.

1. Create a local JWT secret in the ignored `.env` file:

   ```bash
   if [ ! -f .env ]; then
     (umask 077; printf 'JWT_SECRET=%s\n' "$(python3 -c 'import secrets; print(secrets.token_hex(32))')" > .env)
   fi
   ```

2. Start the API and LocalStack. The LocalStack init script creates the S3 bucket, two SQS queues, and the three DynamoDB tables/indexes:

   ```bash
   docker compose up --build -d
   docker compose ps
   docker compose logs -f payment-api
   ```

   The API is at `http://localhost:8080`; LocalStack is at `http://localhost:${LOCALSTACK_PORT:-4566}` (set `LOCALSTACK_PORT` in `.env` if port 4566 is already in use). The default Spring profile is `dynamodb`. The readiness endpoint is `http://localhost:8080/actuator/health/readiness`.

3. Generate a short-lived local JWT:

   ```bash
   set -a
   . ./.env
   set +a
   TOKEN="$(python3 scripts/generate-local-test-jwt.py)"
   ```

4. Submit a small CSV and poll the returned job ID:

   ```bash
   printf 'paymentId,clientId,paymentDate,paymentType\np-001,c-001,2026-10-24,CREDIT_CARD\n' \
     > /tmp/payment-test.csv

   RESPONSE="$(curl -sS -X POST http://localhost:8080/api/v1/payments/reprocess \
     -H "Authorization: Bearer $TOKEN" \
     -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" \
     -F "file=@/tmp/payment-test.csv;type=text/csv")"
   printf '%s\n' "$RESPONSE"
   JOB_ID="$(printf '%s\n' "$RESPONSE" | sed -n 's/.*Job Tracking Reference: \([[:alnum:]_-]*\).*/\1/p')"
   test -n "$JOB_ID" || { echo "Could not read the job ID from the API response." >&2; exit 1; }
   ```

   Expected submission response (`202 Accepted`):

   ```
   Batch successfully queued for processing. Job Tracking Reference: <jobId>
   ```

   Poll the job status:

   ```bash
   curl -i "http://localhost:8080/api/v1/payments/reprocess/$JOB_ID" \
     -H "Authorization: Bearer $TOKEN"
   ```

   Expected job status response (`200 OK`) once processing completes:

   ```json
   {
       "jobId": "<jobId>",
       "status": "COMPLETED",
       "totalItems": 1,
       "reprocessedSuccessfully": 1,
       "notReprocessed": 0,
       "failedItems": [],
       "failureReason": null
   }
   ```

   List all processed items for the job:

   ```bash
   curl -i "http://localhost:8080/api/v1/payments/reprocess/$JOB_ID/items" \
     -H "Authorization: Bearer $TOKEN"
   ```

   Expected items response:

   ```json
   [
       {
           "paymentId": "p-001",
           "clientId": "c-001",
           "paymentDate": "2026-10-24T00:00:00",
           "lastProcessingDate": "<timestamp>",
           "paymentType": "CREDIT_CARD",
           "status": "SUCCESS",
           "failureReason": null
       }
   ]
   ```

   JSON uploads must contain an array of records with `paymentId`, `clientId`, `paymentDate`, and `paymentType`. Supported types are `CREDIT_CARD`, `DEBIT_CARD`, `BANK_TRANSFER`, and `CUSTOM`; `CUSTOM` has no gateway adapter and fails processing. Any other `paymentType` value (e.g. `PIX`) is accepted at ingestion but the item is immediately marked `FAILED` with the reason `"Unsupported payment type: <value>"` when the worker picks it up — the rest of the batch is unaffected. Date values accept ISO dates and date-times.

5. Test the full reprocessing pipeline with a mixed batch of twelve items:

   The file below covers all four recognised payment types (`CREDIT_CARD`, `DEBIT_CARD`,
   `BANK_TRANSFER`, `CUSTOM`) and three items of each type, for a total of twelve items. `CUSTOM`
   has no gateway adapter and each `CUSTOM` item is expected to finish with `FAILED`.

   ```bash
   cat > /tmp/payment-batch-full.csv << 'EOF'
   paymentId,clientId,paymentDate,paymentType
   p-001,c-001,2026-10-24,CREDIT_CARD
   p-002,c-002,2026-10-24,CREDIT_CARD
   p-003,c-003,2026-10-24,CREDIT_CARD
   p-004,c-004,2026-10-24,DEBIT_CARD
   p-005,c-005,2026-10-24,DEBIT_CARD
   p-006,c-006,2026-10-24,DEBIT_CARD
   p-007,c-007,2026-10-24,BANK_TRANSFER
   p-008,c-008,2026-10-24,BANK_TRANSFER
   p-009,c-009,2026-10-24,BANK_TRANSFER
   p-010,c-010,2026-10-24,CUSTOM
   p-011,c-011,2026-10-24,CUSTOM
   p-012,c-012,2026-10-24,CUSTOM
   EOF

   RESPONSE="$(curl -sS -X POST http://localhost:8080/api/v1/payments/reprocess \
     -H "Authorization: Bearer $TOKEN" \
     -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" \
     -F "file=@/tmp/payment-batch-full.csv;type=text/csv")"
   printf '%s\n' "$RESPONSE"
   JOB_ID="$(printf '%s\n' "$RESPONSE" | sed -n 's/.*Job Tracking Reference: \([[:alnum:]_-]*\).*/\1/p')"
   test -n "$JOB_ID" || { echo "Could not read the job ID from the API response." >&2; exit 1; }
   ```

   Poll the job status until it reaches a terminal state (`COMPLETED`, `PARTIALLY_COMPLETED`, or `FAILED`):

   ```bash
   curl -s "http://localhost:8080/api/v1/payments/reprocess/$JOB_ID" \
     -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
   ```

   Expected job status response:

   ```json
   {
       "jobId": "<jobId>",
       "status": "PARTIALLY_COMPLETED",
       "totalItems": 12,
       "reprocessedSuccessfully": 9,
       "notReprocessed": 3,
       "failedItems": [
           { "paymentId": "p-010", "reason": "Payment gateway processing failed." },
           { "paymentId": "p-011", "reason": "Payment gateway processing failed." },
           { "paymentId": "p-012", "reason": "Payment gateway processing failed." }
       ],
       "failureReason": "3 item(s) failed to reprocess."
   }
   ```

   The three `CUSTOM` items have no gateway adapter; each fails at processing time with
   `"Payment gateway processing failed."`. The nine `CREDIT_CARD`, `DEBIT_CARD`, and
   `BANK_TRANSFER` items succeed because their simulated gateways always return `true`.

   Retrieve the full item list with individual statuses:

   ```bash
   curl -s "http://localhost:8080/api/v1/payments/reprocess/$JOB_ID/items" \
     -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
   ```

   Expected items response (twelve entries; abbreviated for readability):

   ```json
   [
       {
           "paymentId": "p-001",
           "clientId": "c-001",
           "paymentDate": "2026-10-24T00:00:00",
           "lastProcessingDate": "<timestamp>",
           "paymentType": "CREDIT_CARD",
           "status": "SUCCESS",
           "failureReason": null
       },
       {
           "paymentId": "p-010",
           "clientId": "c-010",
           "paymentDate": "2026-10-24T00:00:00",
           "lastProcessingDate": "<timestamp>",
           "paymentType": "CUSTOM",
           "status": "FAILED",
           "failureReason": "Payment gateway processing failed."
       }
   ]
   ```

   Items are ordered by their position in the original file. `CREDIT_CARD`, `DEBIT_CARD`, and
   `BANK_TRANSFER` items carry `status: SUCCESS` and `failureReason: null`. `CUSTOM` items carry
   `status: FAILED` and `failureReason: "Payment gateway processing failed."`.

6. Test ingestion with an unsupported payment type (PIX):

   Unsupported payment types such as `PIX` are accepted at ingestion time. The item is stored
   with `paymentType: UNKNOWN` and a pre-set `failureReason`. When the item worker picks it up, it
   skips the gateway call and immediately marks the item `FAILED` with the original reason
   (`"Unsupported payment type: PIX"`). The rest of the batch continues processing normally; only
   the individual item with the unknown type fails.

   ```bash
   cat > /tmp/payment-batch-pix.csv << 'EOF'
   paymentId,clientId,paymentDate,paymentType
   p-101,c-101,2026-10-24,CREDIT_CARD
   p-102,c-102,2026-10-24,PIX
   p-103,c-103,2026-10-24,BANK_TRANSFER
   EOF

   RESPONSE_PIX="$(curl -sS -X POST http://localhost:8080/api/v1/payments/reprocess \
     -H "Authorization: Bearer $TOKEN" \
     -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" \
     -F "file=@/tmp/payment-batch-pix.csv;type=text/csv")"
   printf '%s\n' "$RESPONSE_PIX"
   JOB_PIX="$(printf '%s\n' "$RESPONSE_PIX" | sed -n 's/.*Job Tracking Reference: \([[:alnum:]_-]*\).*/\1/p')"
   ```

   Poll until terminal:

   ```bash
   curl -s "http://localhost:8080/api/v1/payments/reprocess/$JOB_PIX" \
     -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
   ```

   Expected job status response:

   ```json
   {
       "jobId": "<jobId>",
       "status": "PARTIALLY_COMPLETED",
       "totalItems": 3,
       "reprocessedSuccessfully": 2,
       "notReprocessed": 1,
       "failedItems": [
           { "paymentId": "p-102", "reason": "Unsupported payment type: PIX" }
       ],
       "failureReason": "1 item(s) failed to reprocess."
   }
   ```

   Retrieve all three items with individual statuses:

   ```bash
   curl -s "http://localhost:8080/api/v1/payments/reprocess/$JOB_PIX/items" \
     -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
   ```

   Expected items response:

   ```json
   [
       {
           "paymentId": "p-101",
           "clientId": "c-101",
           "paymentDate": "2026-10-24T00:00:00",
           "lastProcessingDate": "<timestamp>",
           "paymentType": "CREDIT_CARD",
           "status": "SUCCESS",
           "failureReason": null
       },
       {
           "paymentId": "p-102",
           "clientId": "c-102",
           "paymentDate": "2026-10-24T00:00:00",
           "lastProcessingDate": "<timestamp>",
           "paymentType": "UNKNOWN",
           "status": "FAILED",
           "failureReason": "Unsupported payment type: PIX"
       },
       {
           "paymentId": "p-103",
           "clientId": "c-103",
           "paymentDate": "2026-10-24T00:00:00",
           "lastProcessingDate": "<timestamp>",
           "paymentType": "BANK_TRANSFER",
           "status": "SUCCESS",
           "failureReason": null
       }
   ]
   ```

   `p-102` carries `paymentType: UNKNOWN` because the original value `PIX` is not a recognised
   type. The `failureReason` records the original value so the cause is traceable. `p-101` and
   `p-103` are unaffected and complete with `SUCCESS`.

7. Stop and discard the local environment:

   ```bash
   docker compose down
   ```

   LocalStack has no persistent volume in this setup; removing its container discards the local S3, SQS, and DynamoDB data. The local `.env` secret remains on disk and is ignored by Git.

## Run and debug from IntelliJ IDEA

This setup runs LocalStack in Docker and the Spring Boot application as a local Java process, so IntelliJ breakpoints work normally.

The default Spring profile is `dynamodb`, and this guide uses that backend. PostgreSQL is optional and is not required for this setup.

1. Import the project as a Maven project and configure the Project SDK and the run configuration JRE to Java 21. The application main class is `com.virgolim.paymentapi.PaymentApiApplication`.
2. Start only LocalStack and wait for it to become healthy:

   ```bash
   docker compose up -d localstack
   docker compose ps localstack
   ```

   Do not start a second LocalStack with `lstk start` on the same port. If the full Compose stack is already running, stop its API container before starting the IDE instance: `docker compose stop payment-api`. This prevents a port conflict and avoids having two workers consume the same queues.
3. Create a local `.env` secret as shown in the previous section. IntelliJ does not automatically load Compose's `.env`, so add the environment variables below to the Run/Debug configuration. Set `LOCALSTACK_PORT` to the host port published by Compose (normally `4566`, or the value configured in `.env`):

   ```text
   SPRING_PROFILES_ACTIVE=dynamodb
   JWT_SECRET=<the same local secret from .env>
   AWS_REGION=us-east-1
   AWS_ACCESS_KEY_ID=test
   AWS_SECRET_ACCESS_KEY=test
   AWS_ENDPOINT_URL=http://localhost:4566
   DYNAMODB_ENDPOINT_URL=http://localhost:4566
   PAYMENT_S3_BUCKET=payment-api-local
   PAYMENT_INGESTION_QUEUE_URL=http://localhost:4566/000000000000/payment-api-ingestion
   PAYMENT_ITEM_QUEUE_URL=http://localhost:4566/000000000000/payment-api-items
   DYNAMODB_BATCH_JOBS_TABLE=payment-api-batch-jobs
   DYNAMODB_TRANSACTION_ITEMS_TABLE=payment-api-transaction-items
   DYNAMODB_OUTBOX_EVENTS_TABLE=payment-api-outbox-events
   ```

   If `LOCALSTACK_PORT` is not `4566`, replace the port in all four endpoint/queue URL values. If port `8080` is already occupied, add `SERVER_PORT=18080` and use port `18080` in the API URLs.
4. Choose an **Application** run configuration (or **Spring Boot** if available), select the main class above and the Java 21 runtime, then click **Debug**. Check `http://localhost:8080/actuator/health/readiness` (or the configured `SERVER_PORT`) for readiness. Set breakpoints in the controller, use cases, queue consumers, or persistence adapter.
5. To generate a test token in a terminal, load the same secret and run the helper:

   ```bash
   set -a
   . ./.env
   set +a
   TOKEN="$(python3 scripts/generate-local-test-jwt.py)"
   ```

   Use that token with the upload and job-status requests in the preceding section, changing the API port if `SERVER_PORT` was set.

The host Java process connects to LocalStack through `localhost`; the `localstack` hostname is only appropriate for containers on the Compose network. Stop the IDE process with IntelliJ and stop LocalStack with `docker compose down` when finished.

## Select PostgreSQL locally

PostgreSQL is retained to demonstrate an alternative persistence implementation, not as the AWS runtime database. Start the same API and LocalStack services with an additional local PostgreSQL container:

```bash
docker compose -f docker-compose.yml -f docker-compose.postgresql.yml up --build -d
```

The override selects Spring profile `postgresql`; Flyway applies the SQL migrations and Hibernate validates the schema. To discard both PostgreSQL and LocalStack data, stop the stack with:

```bash
docker compose -f docker-compose.yml -f docker-compose.postgresql.yml down --volumes
```

The persistence selection is profile-based: `dynamodb` (default) or `postgresql`. `local` is also available for PostgreSQL development against `localhost:5432`; `test` is used by the PostgreSQL integration tests.

## Processing and delivery behavior

The API stores the uploaded object and job/outbox metadata before returning `202 Accepted`. The outbox publisher sends ingestion work to SQS. An ingestion worker validates the object, then reads it again and writes item records/events in batches of ten. Item messages are released after ingestion completes. Workers update each item result and the job counters; clients retrieve status from the owner-scoped GET endpoint.

SQS Standard queues deliver at least once and do not guarantee ordering. Retries can therefore repeat work; the code uses job/item state and payment keys to reduce duplicate processing. This is not an exactly-once payment guarantee. A real gateway integration must use provider-supported idempotency keys and reconcile uncertain outcomes.

**No DLQs or redrive policies are configured in this showcase.** Messages that keep failing are retried by SQS until they are successfully processed or expire under the queue's retention policy; a job can remain incomplete during that period. Before using this design for production traffic, implement dedicated DLQs and redrive policies for both queues, monitoring/alerts, an operator investigation process, and a controlled replay procedure. Terraform has an explanatory comment at the queue definitions; deliberately unused DLQ polling code/configuration has been removed.

The 10 MB upload limit and streaming parser target a small showcase instance. Larger files need a separate direct-to-S3 upload policy and chunked/batch processing, which are not implemented.

## Tests

Run unit and integration tests with:

```bash
./mvnw --batch-mode clean verify
```

PostgreSQL end-to-end tests use Testcontainers and require Docker. They are skipped when Docker is unavailable. The test profile does not use LocalStack.

## AWS showcase environment

Terraform remains the AWS infrastructure definition. The AWS showcase has **one environment only**, labelled `production`, and is restricted by Terraform to `us-east-1`. There is no AWS staging environment: `deploy-staging.yml` runs verification on `main` but does not publish an image or deploy. Features are developed and demonstrated locally, merged to `main`, and released to AWS only by creating a version tag.

Terraform creates a VPC, EC2 host, ECR repository, private S3 bucket, two primary SQS queues, and three on-demand DynamoDB tables. The deployed API uses the `dynamodb` profile. PostgreSQL/RDS is not provisioned in AWS. The EC2 instance is created running; start/stop are manual, and no daily automatic shutdown is configured.

### Provision, run, stop, and remove

Use the default Terraform workspace only. Review the current AWS pricing and Free Tier/credit eligibility for the account; neither is guaranteed by this repository. A budget alert is a notification, not a spending cap. Applying creates billable resources; stopping EC2 only stops compute. Public IPv4, EBS, ECR, S3, DynamoDB, SQS retention, data transfer, and other retained resources can still incur charges.

```bash
cd terraform
terraform init
terraform plan
terraform apply
terraform output
```

Start and stop the sole EC2 instance manually from the repository root:

```bash
./terraform/bash/start-infrastructure.sh
./terraform/bash/stop-infrastructure.sh
```

Create a SecureString parameter `/payment-api/JWT_SECRET` in SSM Parameter Store before deployment. Configure the GitHub `production` Environment with `AWS_REGION=us-east-1`, `EC2_INSTANCE_ID`, `ECR_REPOSITORY`, `SSM_PARAMETER_PATH=/payment-api`, `PAYMENT_S3_BUCKET`, both primary queue URLs, and the three DynamoDB table names from `terraform output`; set `AWS_ROLE_ARN` as an environment secret. Set repository secret `ECR_PUSH_ROLE_ARN` for the tag workflow's ECR push role. OIDC trust and IAM permissions must be restricted to this repository and the required ECR/SSM actions.

After manually starting EC2 and confirming it is available through Systems Manager, create a version tag pointing at a commit already on `main`:

```bash
git tag v1.2.3 <commit-on-main>
git push origin v1.2.3
```

The tagged workflow verifies the commit is on `main`, runs tests, builds/scans/pushes the immutable SHA-tagged image, and deploys it to the single AWS environment. It does not create or start infrastructure. Stop the instance manually after the presentation.

To purge stored S3 objects and destroy the Terraform-managed AWS resources, run the destructive script manually from the repository root. It confirms the account and requires typing `PURGE-AND-DESTROY`; Terraform then asks for its own destroy confirmation:

```bash
./terraform/bash/destroy-infrastructure.sh
```

This permanently removes the S3 upload objects and destroys managed DynamoDB tables, queues, ECR, EC2, and networking resources. Review the selected account and Terraform state before confirming. Data loss is irreversible.

AWS charges depend on account eligibility, region, resource configuration, usage, and retained data. Always verify billing directly in AWS; “on demand” and “stopped” do not mean “free”.

## Profiles and runtime configuration

| Spring profile | Persistence | Intended use |
|---|---|---|
| `dynamodb` | DynamoDB | Default; AWS and LocalStack |
| `postgresql` | PostgreSQL via `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD` | Local relational showcase or supplied PostgreSQL |
| `local` | PostgreSQL on `localhost:5432` | Local development |
| `test` | PostgreSQL test datasource | Testcontainers integration tests |

S3/SQS settings use `PAYMENT_S3_BUCKET`, `PAYMENT_INGESTION_QUEUE_URL`, and `PAYMENT_ITEM_QUEUE_URL`. `AWS_ENDPOINT_URL` redirects S3/SQS clients to LocalStack; `DYNAMODB_ENDPOINT_URL` redirects the DynamoDB client. The AWS deployment leaves both endpoint overrides empty and relies on the EC2 instance role.
