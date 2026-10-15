# Payment Event Processor

A small, production-style backend service in Go that ingests payment events over a REST API, processes them asynchronously with safe concurrent workers, retries failures with exponential backoff, and delivers signed webhooks to registered endpoints. Every write that matters is guarded against duplication: idempotent ingestion, exactly-once claiming of work, and a transactional outbox for webhook delivery.

[![CI](https://github.com/PMooney03/payment-event-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/PMooney03/payment-event-processor/actions/workflows/ci.yml)

The goal is to demonstrate the fundamentals that matter in payment-style systems: reliability under concurrency, idempotency, retries, observability, testing, and a codebase that stays readable as it grows. It is deliberately not a CRUD tutorial.

## Contents

- [Architecture](#architecture)
- [Features](#features)
- [Technology stack](#technology-stack)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API](#api)
- [Testing](#testing)
- [Design decisions](#design-decisions)
  - [Why asynchronous processing](#why-asynchronous-processing)
  - [Idempotency](#idempotency)
  - [Concurrency and exactly-once processing](#concurrency-and-exactly-once-processing)
  - [Retry strategy](#retry-strategy)
  - [Webhooks as a transactional outbox](#webhooks-as-a-transactional-outbox)
  - [Observability](#observability)
  - [Project layout](#project-layout)
- [Future improvements](#future-improvements)

## Architecture

```
 Client
   │  POST /api/events  (+ Idempotency-Key)
   ▼
┌──────────────┐   insert event (pending)          ┌────────────────────┐
│   REST API   │ ────────────────────────────────▶ │                    │
│  net/http    │   reserve idempotency key (same tx)│     PostgreSQL     │
└──────────────┘                                   │                    │
                                                   │  events            │
┌──────────────┐   UPDATE … FOR UPDATE SKIP LOCKED │  idempotency_keys  │
│   Worker(s)  │ ◀──────────────────────────────── │  webhook_endpoints │
│  N goroutines│   succeed / retry / fail           │  webhook_attempts  │
│  per replica │ ────────────────────────────────▶ │                    │
└──────────────┘   + enqueue webhook (same tx)      └────────────────────┘
                                                            │
┌──────────────┐   claim due attempt (SKIP LOCKED)          │
│  Dispatcher  │ ◀──────────────────────────────────────────┘
│              │   HTTP POST, HMAC-signed
└──────────────┘ ────────────────────────────────▶  Your webhook endpoint
                   record status / schedule retry
```

Request flow:

1. A client POSTs an event. The API validates it, atomically reserves the `Idempotency-Key` (if supplied) and inserts the event as `pending`, then returns `201` immediately. Nothing slow happens on the request path.
2. Worker goroutines poll for due `pending` events and claim them with `FOR UPDATE SKIP LOCKED`, so any number of workers across any number of replicas never collide. The claimed event is run through a (simulated) downstream call.
3. On success the event is marked `succeeded` and, in the same transaction, one `webhook_attempts` row is queued per active endpoint. On failure the event is rescheduled with exponential backoff, or marked `failed` after `MAX_RETRIES`.
4. The webhook dispatcher claims due attempts the same way, POSTs the event payload with an HMAC-SHA256 signature, records the HTTP status and timestamp, and schedules retries on non-2xx responses or transport errors.

All coordination lives in PostgreSQL. The process holds no state that matters, so it can be restarted or scaled horizontally without ceremony.

## Features

- `POST /api/events` with strict JSON decoding and per-field validation errors.
- `Idempotency-Key` support that is race-safe at the database level: concurrent duplicates yield exactly one event, and all callers receive the same response. Reusing a key with a different payload returns `422`.
- Asynchronous processing by a pool of workers using `SELECT … FOR UPDATE SKIP LOCKED`; safe with multiple goroutines and multiple service instances.
- Exponential backoff retries (1s, 2s, 4s, …) with a configurable cap, then a terminal `failed` state with the last error recorded.
- Stale-claim recovery: events or webhook attempts left in flight by a crashed worker are automatically returned to the queue.
- Graceful shutdown: in-flight HTTP requests finish; an event interrupted mid-processing is released back to `pending` without consuming a retry.
- Webhook endpoint registration, fan-out to all active endpoints, HMAC-signed payloads, per-attempt audit log with response status and timestamps, and retries with backoff.
- `GET /api/events` with filtering by status and type and keyset (cursor) pagination; `GET /api/events/{id}`; `GET /api/events/{id}/webhook-attempts`.
- `/health` (process liveness) and `/ready` (database reachability).
- Structured JSON logs via `log/slog` carrying `request_id`, `event_id`, `payment_id`, attempt counts, durations and errors. Internal error details are logged, never returned to clients.
- Versioned SQL migrations applied by a separate one-shot command, never by the API at startup.
- Comprehensive tests: unit, HTTP, database integration, idempotency races, concurrent-worker exactly-once, retry schedules, webhook delivery, and full end-to-end flows, all run under the race detector in CI.

## Technology stack

| Concern | Choice | Why |
| --- | --- | --- |
| Language | Go 1.27 | Simple concurrency model, static binaries, strong standard library |
| HTTP | `net/http` with Go 1.22+ method/path routing | No framework needed for a small API; fewer dependencies to audit |
| Database | PostgreSQL 16 via `jackc/pgx/v5` | Row-level locking (`SKIP LOCKED`), transactional guarantees, the best Go driver |
| Migrations | `golang-migrate/migrate` with embedded SQL | Plain SQL files, versioned, runs as its own command |
| Logging | `log/slog` JSON handler | Standard library, structured, context-friendly |
| IDs | `google/uuid` | UUID v4 primary keys generated by the application |
| Packaging | Multi-stage Dockerfile on a distroless, non-root base | Small, minimal attack surface |
| Orchestration | Docker Compose | `docker compose up --build` starts Postgres, runs migrations, starts the API |
| CI | GitHub Actions | Formatting, vet, tidy check, race-enabled tests against a Postgres service, build |

Total runtime dependencies: three (pgx, migrate, uuid).

## Quick start

Prerequisites: Docker with Compose v2. Go 1.27 is only needed to run the tests or binaries outside Docker.

```bash
git clone https://github.com/PMooney03/payment-event-processor.git
cd payment-event-processor
docker compose up --build
```

This starts PostgreSQL, runs the `migrate` job to completion, and then starts the API on `http://localhost:8080`. Logs are JSON lines on stdout.

Submit an event:

```bash
curl -s -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1001' \
  -d '{"type":"payment.created","payment_id":"pay_123","amount":4999,"currency":"EUR","customer_id":"cus_123"}'
```

Within a second or two the worker will have processed it (with the default 30% simulated failure rate you may see it retry). Watch it:

```bash
curl -s http://localhost:8080/api/events/<id>
```

Register a webhook endpoint and watch deliveries. Any HTTP endpoint will do; a quick way to get one is [webhook.site](https://webhook.site) or a local server reachable from the container via `host.docker.internal`:

```bash
curl -s -X POST http://localhost:8080/api/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://webhook.site/<your-id>","secret":"whsec_demo"}'

curl -s http://localhost:8080/api/events/<id>/webhook-attempts
```

To force the retry path deterministically, include `fail` anywhere in the `payment_id` (for example `pay_fail_1`): the simulated downstream will fail every attempt and the event will end up `failed` after `MAX_RETRIES`.

Running outside Docker:

```bash
docker compose up -d postgres          # just the database
make migrate                           # apply migrations
make run                               # API + worker + dispatcher on :8080
```

Stop everything and delete the database volume with `make down`.

## Configuration

All configuration is via environment variables. Every value has a sensible local default; invalid values fail fast at startup with a message listing every problem.

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/payments?sslmode=disable` | PostgreSQL connection string |
| `PORT` | `8080` | HTTP listen port |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `MAX_RETRIES` | `3` | Retries before an event is marked `failed` (0 disables retries) |
| `RETRY_BASE_DELAY` | `1s` | First retry delay; doubles each attempt (1s, 2s, 4s, …) |
| `WORKER_INTERVAL` | `1s` | How often an idle worker polls for work |
| `WORKER_CONCURRENCY` | `2` | Worker goroutines (and dispatcher goroutines) per process |
| `FAILURE_RATE` | `0.3` | Probability in `[0,1]` that the simulated downstream fails |
| `WEBHOOK_TIMEOUT` | `5s` | Per-request timeout for outbound webhook POSTs |
| `WEBHOOK_MAX_RETRIES` | `5` | Retries per webhook delivery before giving up |

See [`.env.example`](.env.example).

## API

All responses are JSON. Errors share one envelope:

```json
{"error":{"code":"validation_failed","message":"one or more fields are invalid","fields":{"amount":"must be a positive integer in minor units (e.g. cents)"}}}
```

Every response carries an `X-Request-ID` header (client-supplied values are honoured if they are short and printable) which also appears in every log line for that request.

### Create an event

`POST /api/events`

Headers: `Content-Type: application/json`, optional `Idempotency-Key` (up to 255 characters).

```json
{
  "type": "payment.created",
  "payment_id": "pay_123",
  "amount": 4999,
  "currency": "EUR",
  "customer_id": "cus_123"
}
```

- `type`: one of `payment.created`, `payment.succeeded`, `payment.failed`, `payment.refunded`
- `payment_id`, `customer_id`: 1–255 characters from `[A-Za-z0-9_.-]`
- `amount`: positive integer in minor units
- `currency`: 3-letter ISO 4217 code (normalised to upper case)

Unknown fields and trailing data are rejected.

| Status | Meaning |
| --- | --- |
| `201 Created` | Event stored as `pending`; `Location` header points at it |
| `200 OK` | Replayed from an earlier request with the same `Idempotency-Key`; `Idempotent-Replayed: true` is set |
| `400 Bad Request` | Malformed JSON or validation failure (`fields` lists each problem) |
| `409 Conflict` | A request with this key is still in progress; `Retry-After` is set |
| `422 Unprocessable Entity` | The key was previously used with a different payload |

Response body (201/200):

```json
{
  "id": "0ca48c0d-0179-416c-9ae8-f2a4e1c49788",
  "type": "payment.created",
  "payment_id": "pay_123",
  "amount": 4999,
  "currency": "EUR",
  "customer_id": "cus_123",
  "status": "pending",
  "retry_count": 0,
  "next_attempt_at": "2026-10-15T13:07:47.746042Z",
  "created_at": "2026-10-15T13:07:47.746042Z",
  "updated_at": "2026-10-15T13:07:47.746042Z"
}
```

Once processed, `status` becomes `processing`, then `succeeded` or `failed`; `last_error` is populated on failure.

### List events

`GET /api/events?status=succeeded&type=payment.created&limit=50&cursor=<event id>`

Returns `{"data":[…],"next_cursor":"<id>"}`, newest first. Pass `next_cursor` back as `cursor` to fetch the next page. `limit` is 1–200 (default 50).

### Get one event

`GET /api/events/{id}` → `200` with the event, or `404`.

### Webhooks

| Method and path | Purpose |
| --- | --- |
| `POST /api/webhooks` | Register `{"url":"https://…","secret":"optional"}` → `201` |
| `GET /api/webhooks` | List endpoints (secrets are never returned; `has_secret` is) |
| `DELETE /api/webhooks/{id}` | Remove an endpoint and its attempt history → `204` |
| `GET /api/events/{id}/webhook-attempts` | Per-attempt audit log for an event |

Each delivery is a `POST` to the endpoint URL with:

```http
Content-Type: application/json
User-Agent: payment-event-processor/1.0
X-Webhook-Event-ID: <event id>
X-Webhook-Attempt: <n>
X-Webhook-Signature: sha256=<hex HMAC-SHA256 of the body using the endpoint secret>
```

```json
{
  "id": "<attempt id, unique per delivery; use it to deduplicate>",
  "type": "payment.created",
  "attempt": 1,
  "created_at": "2026-10-15T15:00:40Z",
  "event": { "...": "the full event object" }
}
```

Any `2xx` response is a success. Anything else, including timeouts, is recorded as a failed attempt and retried with backoff. Redirects are not followed.

### Health

- `GET /health` → `200 {"status":"ok"}` whenever the process is serving.
- `GET /ready` → `200` when PostgreSQL answers a ping within 2s, otherwise `503`. Suitable for a Kubernetes readiness probe.

## Testing

```bash
make test                # unit + HTTP tests, no database needed, with -race
make test-integration    # everything, against payments_test on localhost:5432
make test-race-docker    # same, inside a Linux container (for Windows hosts)
make ci                  # fmt-check + vet + test + build, as CI runs it
```

Database-backed tests are skipped unless `TEST_DATABASE_URL` is set, so `go test ./...` always works offline. `docker compose up` creates a separate `payments_test` database on first boot so tests never interfere with the running application. Integration tests share that database and therefore run with `-p 1` (one package at a time).

What is covered, and where:

| Area | Tests |
| --- | --- |
| Validation and config | `internal/events/event_test.go`, `internal/config/config_test.go`, `internal/webhook/webhook_test.go` |
| HTTP contract (status codes, error envelope, request IDs, body limits, 404/405, panic recovery) | `internal/http/*_test.go` using in-memory fakes |
| Store correctness (CRUD, filtering, keyset pagination, claim semantics, guarded transitions) | `internal/events/store_test.go` |
| Idempotency: replay, payload mismatch, key not burned by invalid input, **50 concurrent duplicates produce exactly one event** | `internal/events/service_test.go` |
| Worker: **two worker pools × 4 goroutines process 60 events exactly once**, backoff schedule timing, terminal failure, success hook atomicity, release on shutdown, panic containment, stale reclaim | `internal/worker/worker_test.go` |
| Webhooks: signed payload, outcome recording, retry chain with spacing, eventual success, timeouts, fan-out, inactive endpoints skipped, concurrent dispatchers deliver once, cascade delete | `internal/webhook/dispatcher_test.go` |
| End-to-end through the real HTTP API with real worker and dispatcher | `tests/e2e_test.go` |

CI runs the whole suite with `-race` against a PostgreSQL service container on every push and pull request.

## Design decisions

### Why asynchronous processing

Accepting an event and processing it are separated on purpose. The API's only job is to validate, persist and acknowledge. That keeps request latency low and predictable, lets the API stay available when a downstream dependency is slow or down (events simply queue), and means a crash mid-processing can never lose an accepted event: it is already durable in PostgreSQL before the client sees `201`. In payment systems the cost of dropping or double-applying an event is high, so durability-before-acknowledgement is the right default.

The queue is a PostgreSQL table rather than a message broker. For this scale a broker would add operational surface without adding a guarantee we do not already have: the database gives us durability, ordering by `created_at`, atomic claim-and-update, and the ability to enqueue work in the same transaction as the business change that caused it.

### Idempotency

Clients retry. Networks drop responses after the server has committed. Without idempotency, "create payment event" retried three times creates three events.

The implementation, in `internal/idempotency` and `events.Service.createIdempotent`:

1. Hash the validated, normalised input (SHA-256 of its canonical JSON). Hashing after normalisation means `"eur"` and `"EUR"` are the same request; hashing the raw body would not.
2. In one transaction: `INSERT INTO idempotency_keys … ON CONFLICT DO NOTHING`. The primary key is the arbiter. If the insert affects a row, this request owns the key: insert the event, store the serialised response on the key row, commit.
3. If the insert affects zero rows, someone else owns the key. Compare hashes: a different payload is a client bug (`422`); the same payload replays the stored response (`200` plus `Idempotent-Replayed: true`); a key with no stored response yet means the owner is still mid-transaction (`409` with `Retry-After`).

Why this is race-safe: PostgreSQL makes a conflicting `INSERT` wait until the first inserter's transaction commits or rolls back. So of N simultaneous identical requests, one wins, the other N-1 block briefly and then see a completed key and replay it. The test `TestServiceConcurrentDuplicatesCreateOneEvent` fires 50 goroutines at once and asserts exactly one event exists and every caller saw the same ID; `TestE2E_ConcurrentIdempotentRequestsOverHTTP` does the same through the HTTP server. Validation runs before reservation so a rejected request does not burn its key.

Storing the response body rather than just the event ID gives a faithful replay even after the worker has moved the event to `succeeded`: the client gets back what it would have seen the first time.

### Concurrency and exactly-once processing

Workers claim work with a single statement:

```sql
UPDATE events SET status = 'processing', updated_at = now()
WHERE id = (
  SELECT id FROM events
  WHERE status = 'pending' AND next_attempt_at <= now()
  ORDER BY next_attempt_at, created_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

`FOR UPDATE` locks the chosen row; `SKIP LOCKED` makes other workers skip rows that are already locked instead of waiting on them. Because the claim and the status change are one atomic statement, two workers can never both receive the same row, and no worker ever blocks another. This holds across goroutines, processes and machines, because the coordination point is the database rather than memory. `TestConcurrentWorkersProcessEachEventExactlyOnce` runs two independent worker pools against the same table and asserts every one of 60 events was handed to the processor exactly once.

Every status transition is guarded (`WHERE status = 'processing'`) and treats "0 rows affected" as an error rather than silently succeeding. If the claim protocol were ever violated, it would surface loudly instead of corrupting state.

Two failure modes get specific handling:

- Shutdown mid-attempt: the worker runs the downstream call under the shutdown context. If it is interrupted, the event is released back to `pending` without counting a retry. The status write itself uses a context detached from shutdown (`context.WithoutCancel` with its own timeout) so stopping the process cannot strand a row in `processing`.
- Crash mid-attempt: a reaper goroutine periodically returns rows stuck in `processing` (or webhook attempts stuck in `delivering`) for longer than a threshold back to the queue. The downstream call is assumed idempotent on retry, which is the contract a real payment provider integration would also need.

### Retry strategy

Failures are classified by the processor returning an error. The worker then either schedules a retry or gives up:

- `retry_count < MAX_RETRIES`: set `status = 'pending'`, increment `retry_count`, set `next_attempt_at = now() + RETRY_BASE_DELAY × 2^retry_count`, store the error. With the defaults that is 1s, 2s, 4s.
- otherwise: `status = 'failed'` with `last_error` set. Failed events are not retried automatically; they are retained for inspection, which is what you want for a payments dead-letter path.

Backoff is capped at one hour and the shift arithmetic is overflow-safe (`internal/retry`). The schedule is deliberately deterministic so tests can assert on timing; see [Future improvements](#future-improvements) for jitter. Webhook retries reuse the same function with their own `WEBHOOK_MAX_RETRIES`.

The retry path is exercised by `TestRetryWithBackoffThenPermanentFailure`, which asserts the exact number of attempts and that the gaps between them respect the schedule, and `TestEventuallySucceedsAfterTransientFailures`, which checks that `last_error` is cleared once an event finally succeeds.

### Webhooks as a transactional outbox

Marking an event `succeeded` and queuing its webhooks happen in one transaction: the worker's success hook inserts one `webhook_attempts` row per active endpoint inside the same transaction that updates the event. If either write fails, both roll back and the event is retried. This is the transactional outbox pattern, and it closes the gap where a process could crash between "recorded success" and "told anyone about it". `TestSuccessHookRunsAtomicallyWithStatusChange` proves a failing hook leaves the event un-succeeded.

Each row in `webhook_attempts` is exactly one HTTP attempt: it records status, response code, error text, when it was attempted and when it was due. A failed attempt with retries remaining inserts the next pending row in the same transaction that records the failure, so the table is simultaneously the delivery queue and a complete audit log. The dispatcher claims due rows with `SKIP LOCKED` just like the worker.

Payloads are signed with HMAC-SHA256 over the exact bytes sent when the endpoint has a secret, so receivers can authenticate deliveries. Secrets are stored but never returned by the API. The HTTP client does not follow redirects, so a signed payload cannot be bounced to a host the operator did not register. Response bodies are drained up to a small limit and discarded; only the status code is kept.

Webhook delivery is fully separated from the API: the handlers only register endpoints and read the audit log. The dispatcher is its own component with its own loops and configuration.

### Observability

Logs are JSON lines from `log/slog`. Middleware assigns or honours an `X-Request-ID`, attaches a request-scoped logger to the context, and emits one access-log line per request with method, path, status, bytes and duration. The worker and dispatcher log every outcome with `event_id`, `payment_id`, `attempt`, `duration_ms`, and on failure the error and the scheduled retry. Internal errors are logged with full detail and returned to clients as an opaque `500`, so stack traces and connection strings never leak. No card data or secrets exist in this system, and identifiers are validated to a conservative character set so they are safe to log.

`/ready` pings the database with a short timeout and returns `503` when it fails, which is what a load balancer or orchestrator should use to stop routing traffic to an unhealthy replica.

### Project layout

```
cmd/
  api/          wires config, database, HTTP server, worker and dispatcher; graceful shutdown
  migrate/      applies/rolls back migrations (up | down | version)
internal/
  config/       environment parsing with defaults and aggregated validation errors
  database/     pgx pool, transaction helper, error classification, embedded migrator
    dbtest/     test helper: connect, migrate, truncate (skips without TEST_DATABASE_URL)
  events/       domain model, validation, PostgreSQL store, ingestion service
  idempotency/  key reservation / completion store and payload hashing
  worker/       claim loop, outcome handling, stale reclaim, simulated downstream
  retry/        exponential backoff
  webhook/      endpoints, attempts, transactional enqueue, signed dispatcher
  http/         router, handlers, middleware, JSON helpers (no business logic)
  logging/      slog setup and context propagation
migrations/     numbered .up.sql / .down.sql pairs, embedded into the binaries
tests/          end-to-end tests over the real HTTP API, worker and dispatcher
```

Business logic lives in `events`, `worker` and `webhook`; `http` translates between HTTP and those packages and knows nothing about SQL; stores know nothing about HTTP. Interfaces are used in exactly two places where they buy something concrete: the HTTP handlers depend on small `EventService` and `WebhookService` interfaces so they can be tested with in-memory fakes, and store methods accept a `database.DB` (satisfied by both the pool and a transaction) so the same query code runs standalone or inside a caller's transaction. Everything else is concrete.

Errors are wrapped with context at each layer (`fmt.Errorf("…: %w", err)`), sentinel and typed errors (`events.ErrNotFound`, `*events.ValidationError`, `idempotency.ErrPayloadMismatch`) cross package boundaries so handlers can map them to status codes, and nothing is discarded silently: even a transaction rollback failure is joined onto the original error.

## Future improvements

- Jitter on backoff to avoid synchronised retry storms when many events fail at once.
- Metrics (Prometheus): queue depth, processing latency, retry and failure rates, webhook delivery outcomes.
- Distributed tracing with OpenTelemetry, propagating the request ID into worker and dispatcher spans.
- Authentication for the API (API keys per tenant) and tenant scoping of events and endpoints.
- Dead-letter management: an endpoint to requeue `failed` events after a downstream outage is resolved.
- Per-endpoint delivery ordering and circuit breaking for endpoints that fail persistently.
- `LISTEN/NOTIFY` to wake workers immediately instead of polling, reducing latency at low volume.
- Partitioning or archiving of `events` and `webhook_attempts` for long retention.
- An OpenAPI specification generated from the handlers.
