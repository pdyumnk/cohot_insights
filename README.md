# Cohort Insights API

A FastAPI service that ingests documents and runs each one through a simulated two-stage
pipeline (**processing** → summary, **enriching** → tags). It serves versioned results to
the submitting user and to a partner system that refers to documents by its own
`client_doc_ref`.

**Stack:** Python 3.12 · FastAPI · MongoDB 7 (PyMongo's native async API) · Redis 7 · Docker Compose

```
            ┌──────────── api (FastAPI) ────────────┐        ┌──── worker (N tasks) ────┐
 client ──► │ routes → DocumentService → Repository │ ─────► │ claim (lease + token)    │
            │            │  ActiveJobLimiter ──┐     │  Mongo │ processing → enriching   │
            │            │  ContentCache ──────┤     │ ◄───── │ guarded writes           │
            └────────────┼─────────────────────┼─────┘        └──────────────────────────┘
                         ▼                     ▼
                      MongoDB  (source of truth)   Redis (active-job sets, result cache)
```

---

## Quick start

```bash
cp .env.example .env            # optional; every variable has a default
docker compose up -d --build    # api + worker + mongo + redis
curl localhost:8000/health      # {"status":"ok","checks":{"mongodb":"ok","redis":"ok"}}
```

Swagger UI: http://localhost:8000/docs. If port 8000 is taken, set `API_PORT=8010` in
`.env`. To watch the pipeline faster, set `STAGE_TIME_SCALE=0.1`. To add processing
capacity, run `docker compose up -d --scale worker=3`.

```bash
H='-H Content-Type:application/json -H X-User-Id:u1'
curl -X POST localhost:8000/documents $H \
  -d '{"user_id":"u1","title":"Q3","content":"Revenue grew 12% ...","client_doc_ref":"cms-123"}'
curl localhost:8000/documents/<id>          -H X-User-Id:u1
curl -X PATCH localhost:8000/documents/<id> $H -d '{"content":"new text","expected_version":1}'
curl localhost:8000/documents/by-ref/cms-123 -H X-User-Id:u1
curl "localhost:8000/users/u1/documents?page=1&page_size=20&status=completed" -H X-User-Id:u1
```

### Tests

The tests are integration tests (pytest + `httpx.AsyncClient` over ASGI) against a real
MongoDB and Redis, plus a few pure unit tests:

```bash
docker run -d --rm --name test-mongo -p 27018:27017 mongo:7
docker run -d --rm --name test-redis -p 6380:6379 redis:7-alpine
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest            # 55 tests, about 7 s
```

Override the connections with `TEST_MONGO_URL` and `TEST_REDIS_URL` if needed. The tests
advance the pipeline explicitly (`worker.run_once()`), so they're deterministic. One stress
test instead runs 3 real concurrent workers while PATCHing repeatedly, and checks the
staleness invariant on every poll.

---

## Endpoints

Identity comes from the `X-User-Id` header (see Assumptions).

| Method & path | Success | Errors |
|---|---|---|
| `POST /documents` | **201** `{document_id, status, content_version, client_doc_ref}`; **200** on an idempotent replay | 403 body `user_id` ≠ caller · 409 ref conflict · 422 · 429 |
| `PATCH /documents/{id}` | 200 document | 404 · 409 version conflict · 422 · 429 |
| `GET /documents/{id}` | 200 document | 404 |
| `GET /documents/by-ref/{client_doc_ref}` | 200 document | 404 · 422 |
| `GET /users/{user_id}/documents?page&page_size&status` | 200 `{items, page, page_size, has_more}` | 404 if not the caller · 422 |
| `POST /documents/{id}/retry` | **202**, resumes a failed document at the failed stage | 404 · 409 not failed · 429 |
| `GET /health` | 200 `ok` / `degraded` (Redis down) | 503 (MongoDB down) |

A POST whose content is already cached returns `201` with `status: "completed"`.

Document response (abridged):

```jsonc
{
  "document_id": "6b05…", "status": "enriching", "failed_stage": null,
  "content_version": 2, "content_hash": "sha256:9c1e…",
  "stages": {
    "processing": {"state": "completed", "content_version": 2, "attempts": 1, "error": null, …},
    "enriching":  {"state": "running",   "content_version": null, "attempts": 1, "error": null, …}
  },
  "result_is_current": false,
  "result": null,                                        // summary+tags for THIS content version only
  "stale_result": {"content_version": 1, "content_hash": "sha256:51aa…",
                   "summary": "…", "tags": ["…"], "source": "pipeline", "completed_at": "…"}
}
```

---

## Schema & Staleness Design

### Stored shape

One MongoDB document per submitted document:

```js
{
  _id, user_id, title, client_doc_ref?,            // identity, immutable after POST
  content, content_hash, content_version,          // the CURRENT content (version starts at 1)
  status,                                          // queued | processing | enriching | completed | failed
  stages: {
    processing: { state, content_version, attempts, error, started_at, finished_at, summary },
    enriching:  { state, content_version, attempts, error, started_at, finished_at }
  },
  result: null | { content_version, content_hash, summary, tags, source, completed_at },
  lease_token, lease_until,                        // worker claim: fencing token + expiry
  created_at, updated_at
}
```

- `status` is the pipeline position. `stages.<name>.state` and `.error` say which stage
  failed and why. `failed_stage` in the response is *derived* from these. There are no
  ad-hoc `"failed_at_x"` strings.
- `stages.processing.summary` is internal working state. Enriching reads it, and keeping it
  is what lets an enriching failure skip re-running processing. **It's never returned to
  readers.**
- **`result` is the only place a reader-visible summary or tags exist.** It's a single
  sub-document that carries the `content_version` and `content_hash` of the content it was
  derived from.

### The mechanism

`content_version` is a counter owned by the content: PATCH increments it. Everything derived
from content is stamped with the version (and hash) it came from. A reader compares
numbers:

> `result` is current ⇔ `result.content_version == content_version` **and**
> `result.content_hash == content_hash`.

The API does that comparison and puts the result in either `result` (current) or
`stale_result` (an older version, clearly labelled). The raw numbers are in every response,
so a client can verify this itself, or check `content_hash` against its own sha256 of the
content.

### Why a mismatch is unobservable, not just unlikely

Each rule below depends only on MongoDB's guarantee that a write to a **single document** is
atomic and that a read sees a whole document.

1. **Summary and tags are published together, or not at all.** No code path writes a
   reader-visible summary without its tags. `result` is set by exactly two writes, and both
   set the whole sub-document in one `$set`:
   - enriching completion (`complete_enriching`)
   - a cache hit (`content_fields`)

   A reader can't see new tags with an old summary, because they're the same field.
2. **PATCH is one atomic write.** New `content`, `content_hash`, `content_version + 1`,
   `status: queued`, both stages reset, and the lease cleared all go out as one
   `find_one_and_update`. No snapshot has the new content with the old version number, so
   the old `result`'s version stops matching at the same instant the content changes.
3. **Workers can't publish for content that has moved on.** Every pipeline write filters on
   `{_id, content_version: <claimed>, lease_token: <claimed>}`. PATCH changed the version and
   nulled the token, so a run that started on v1 and finishes after a PATCH matches zero
   documents, and its result is discarded. (Tested for both stages:
   `test_in_flight_*_for_old_version_is_dropped`.)
4. **Tags are derived from a summary of the same version.** Enriching reads the summary and
   `content_version` from the same claimed snapshot, asserts
   `stages.processing.content_version == content_version`, and publishes with that version.
   Rule 2 makes the assertion always true: PATCH resets the processing stage in the same
   write that bumps the version.
5. **The cache can't cross versions.** A cache entry is keyed by content hash, and on a hit
   the result is written with the document's *new* version in the PATCH or POST write itself.

Together: every `result` a reader can observe was computed from exactly the content whose
version and hash it carries. Whether that's the *current* content is a comparison of two
fields from the same atomic snapshot, never a guess and never a free-floating boolean.

**Mid-reprocessing**, a poller gets `result: null`, `result_is_current: false` and the
previous output under `stale_result` with its older version. So the caller is never told the
old summary is current, and the partner still has something to show while the new version
processes.

### Concurrent PATCHes (optimistic concurrency)

PATCH filters on `content_version: expected_version`. If `expected_version` is omitted, it
uses the version just read, so a lost update is never silent. When two PATCHes race from v1,
exactly one matches. The other gets **409** with the current `content_version` to re-read
and retry (`test_racing_patches_with_expected_version_one_wins`). A PATCH with content
identical to the current content is a no-op (200, no reprocessing), so partner retries are
idempotent.

---

## Pipeline

```
queued → processing → enriching → completed
              ↓            ↓
            failed       failed      (POST /retry resumes at the failed stage)
```

- **Separate worker process** (`python -m app.worker`, the `worker` compose service), running
  `WORKER_CONCURRENCY` loops. The API never runs stages, unless `EMBEDDED_WORKERS=true`
  for local development.
- **Claiming** is an atomic `find_one_and_update` that sets the stage to `running` with a
  `lease_until` and a fresh random `lease_token`. A job is claimable when it's waiting for a
  stage, or when its lease has expired. Two workers can't win the same claim
  (`test_two_workers_cannot_claim_the_same_stage`).
- **Crash recovery via leases**, not a startup reset. A dead worker's job is re-claimed after
  `STAGE_LEASE_S`, under a new token, so the zombie's late writes are fenced off
  (`test_expired_lease_is_reclaimed_and_old_holder_is_fenced`). This is why any number of
  worker processes can run. The lease is renewed at each attempt.
- **Retry with backoff**: up to `MAX_STAGE_ATTEMPTS` attempts per stage, waiting
  `base · 2^(n-1)` ±50% jitter between them. The attempt count is persisted, so a takeover
  continues counting. Only then is the stage marked `failed`.
- **Partial progress is preserved.** If enriching fails, the processing stage stays
  `completed` with its summary, and `POST /retry` (or a lease takeover) runs enriching only
  (`test_enriching_failure_keeps_summary_and_retry_skips_processing`).
- Workers claim enriching jobs before new processing jobs, which finishes in-flight work
  and frees rate-limit slots sooner.
- `text_digest` is deterministic: the summary is the first 40 words, and the tags are the
  most frequent non-stopwords of the summary. The cache depends on this.

## Crosswalk (`client_doc_ref`)

- Optional, and **globally unique when present**. Index `client_doc_ref_unique`:
  `{client_doc_ref: 1}`, `unique: true`,
  `partialFilterExpression: {client_doc_ref: {$type: "string"}}`, so the many documents
  without a ref don't collide.
- The lookup is `{client_doc_ref: {$eq: ref, $type: "string"}, user_id: caller}`. The `$type`
  term matters: a partial index is only eligible when the query provably falls inside its
  filter, and a bare equality doesn't, so it would fall back to a COLLSCAN. A test asserts the
  winning plan uses `client_doc_ref_unique` (`test_by_ref_uses_the_partial_unique_index`).

**Repeat submission policy.** Content changes go through PATCH; POST never mutates.

| `POST` with a ref that already exists | Outcome |
|---|---|
| Same owner, content equal to the document's **current** content | **200**, the existing document (idempotent replay) |
| Same owner, different content | **409** `{document_id, content_version}`: use `PATCH` with `expected_version` |
| Different owner | **409** without naming any document |
| Simultaneous first submissions of a new ref | The unique index picks one winner (201). The others become replays (200), or a retryable 429 if they lost the race for a rate-limit slot before the winner's insert was visible. Never two documents. |

Why reject instead of "treat as a new version": the partner may send out of order, and a
POST carries no ordering information. If a delayed retry of an *old* POST were accepted as
a new version, it would silently overwrite newer content (`test_out_of_order_old_retry_cannot_revert_a_patch`).
PATCH with `expected_version` gives updates an explicit order, and identical retries stay
safe because they're idempotent.

## Per-user rate limiting (Redis)

`active:{user_id}` is a **sorted set of active document ids**, scored by acquisition time.
It's not a counter. A Lua script does check-and-add atomically, with the limit of 3 checked
against `ZCARD`.

- **Idempotent acquire and release.** Re-acquiring a member is a no-op, so PATCHing an
  in-flight document doesn't take a second slot. `ZREM` can't push the count below the truth
  the way a duplicate `DECR` can.
- **Covers all active states.** A slot is taken at POST (or at PATCH or retry of an inactive
  document), and released only by the guarded write that moves the document to `completed`
  or `failed`. A cache hit never takes one.
- **Self-healing.** If the key is missing (TTL, Redis restart), it's re-seeded from MongoDB's
  active documents. When a user is at the limit, members older than 30 s whose documents are
  no longer active are pruned. That heals releases lost during a Redis outage. Pruning runs
  at most once per user per 5 s, so a client hammering at its limit can't turn every 429 into
  a MongoDB query.
- **Redis down:** the limiter falls back to `count_documents({user_id, status ∈ active})` on
  the `user_status_created` index. That count is bounded by 3, so it's cheap. The fallback
  isn't atomic, so concurrent submits during an outage can overshoot by a request or two:
  a deliberate availability-over-strictness trade-off. Redis calls fail fast (0.5 s timeout,
  1 retry), so an outage doesn't turn into API latency.

## Content cache (Redis)

- `cache:v1:{sha256(content)}` → `{summary, tags}`, TTL 7 days, written when enriching
  completes. **Keyed by content, never by document**, so after a PATCH the new hash can't
  hit the old entry (`test_cache_is_keyed_by_content_not_document`). `v1` versions the
  `text_digest` logic: changing the algorithm means bumping it, with no flush needed.
- **MongoDB is the source of truth.** On a Redis miss or outage, the cache looks up any
  published `result` with that hash (index `result_content_hash`), then refills Redis. This
  is safe because a published result carries its own content hash, even when it's now stale
  for its own document.
- Shared across users: the output is a pure function of the content.

## Ownership

Every read and write puts `user_id: caller` *inside the MongoDB filter*. A foreign id, a
missing id and a malformed id all return the identical **404** `{"detail":"Document not found"}`.
Listing another user's documents is also 404. I chose 404 over 403 because a 403 confirms the
document exists.

## Indexes

| Index | Serves |
|---|---|
| `{user_id: 1, created_at: -1, _id: -1}` | Listing newest first (`_id` is the tiebreaker and future keyset cursor) |
| `{user_id: 1, status: 1, created_at: -1, _id: -1}` | Listing with a status filter; rate-limiter fallback count and seeding |
| `{client_doc_ref: 1}` unique, partial (`$type: string`) | `GET /documents/by-ref/{ref}`, ref uniqueness |
| `{result.content_hash: 1}` partial | Content-cache fallback / refill |
| `{status: 1, updated_at: 1}` | Worker claims, oldest first (each `$or` branch is a status equality, so a merge sort) |

`_id` lookups use the default index. Every id lookup also includes `user_id`.

---

## At 100×

**One user with 500K documents.** Lookups stay O(log n): `{user_id, created_at, _id}` finds
the user's range instantly. Three things degrade:
- skip-based deep pages walk that range linearly;
- `status=completed` barely narrows `{user_id, status, …}`, because nearly all 500K are
  completed;
- every one of that user's inserts lands on the right edge of the same key range.

Two things are safe by design:
- the rate-limit count matches at most 3 documents;
- the listing returns `has_more` from a `limit+1` fetch, never a `count()` over 500K.

**Shard key: `{user_id: "hashed", _id: 1}`.** The hot queries (list, get, PATCH, by-ref,
rate-limit fallback) all carry `user_id`, so they're single-shard. Hashing spreads users
evenly, even when ids share prefixes like `tenant-a-*`. The `_id` suffix gives a whale
cardinality inside its hash value, so its 500K documents can split across chunks. With
`user_id` alone, that user is one unsplittable jumbo chunk, a permanent hot partition.
`_id` or `created_at` alone would send every insert to the last chunk and make listings
scatter-gather. Two consequences:
- A unique index on a sharded collection must be prefixed by the shard key, so global
  `client_doc_ref` uniqueness moves to a small `refs` collection sharded by
  `client_doc_ref`. Inserting there first acts as the claim.
- The worker claim query has no shard key, so every poll would be scatter-gather. I'd move
  the queue to Redis Streams consumer groups and keep MongoDB for state and guarded writes.

**Rate limiter at 100× QPS.** Each submit is one Lua call on a ≤3-member set, so throughput
isn't the issue: single-key scripts are Redis Cluster-safe and users spread across hash
slots. What breaks is the failure path:
- During an outage, 100× submits fall back to MongoDB counts at once and hammer the
  primary. I'd add a circuit breaker that fails closed (429) past a threshold.
- The `EXISTS` and seeding round-trip should fold into the script.
- Rejection-time reconcile is already throttled. At this scale I'd rather have slot leases
  tied to the worker lease, so slots expire by themselves.

**skip/limit pagination.** It doesn't hold. `skip(N)` still walks N index keys (page 10,000
× 50 = 500K keys), and newest-first ordering shifts when new documents arrive, so pages
repeat or drop items. I'd use keyset pagination: an opaque cursor `(created_at, _id)` of the
last item and `{user_id, (created_at,_id) < cursor}` with `limit+1`, which is a bounded seek
on the existing index at any depth. Today `page` is capped at 10,000 as a guard.

---

## Assumptions

- **Identity:** `X-User-Id` stands in for a verified token subject (JWT `sub` in
  production). The POST body's `user_id` must equal it, otherwise 403.
- **Partner access:** `by-ref` is scoped to the owner like every read, so the partner calls
  as the owning user (or a service account). A partner principal with cross-user read
  would be an explicit scope, not an open lookup.
- `client_doc_ref` is unique across the whole service, not per user, because the partner
  CMS has one namespace.
- PATCH changes `content` only. `title` and `client_doc_ref` are immutable.
- Identical content shares cached results across users. The output is a pure function of
  the content, so nothing leaks beyond what the submitter already has.
- Identifiers match `^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`. Content is 1–100,000 chars, not
  blank, with no control characters. Unknown body fields are rejected.

## Configuration

All settings are environment variables. They're documented in [.env.example](.env.example)
and loaded with `pydantic-settings`. The key ones are `MAX_ACTIVE_PER_USER`,
`WORKER_CONCURRENCY`, `STAGE_LEASE_S`, `STAGE_TIME_SCALE`, `FAILURE_RATE`,
`MAX_STAGE_ATTEMPTS`, `RETRY_BASE_DELAY_S`, `CACHE_TTL_S` and `EMBEDDED_WORKERS`.

## Production readiness

- JSON-lines logs with `request_id` (from an `X-Request-Id` header, or generated and echoed
  back).
- Typed domain errors mapped centrally to HTTP status codes; MongoDB failures return 503.
  There's no `except Exception` anywhere. Only `PyMongoError` and `RedisError` are caught,
  each with a specific fallback.
- Layers: routes → service → repository, wired by a small container and injected with
  FastAPI `Depends`.
- The containers run as a non-root user, with compose healthchecks and `depends_on:
  service_healthy`. Redis uses AOF persistence.

## What I'd do differently with more time

- Keyset cursor pagination in the API, not only in the design.
- Redis Streams (or arq) as the job queue, with MongoDB keeping state and guarded writes.
  Polling is simple and correct, but it's wasted work at idle.
- Rate-limit slots as leases tied to the worker lease, plus an outage circuit breaker.
- A transactional outbox or change stream for the "result published" events the partner
  would want, instead of polling.
- Real auth (JWT) and a scoped partner principal for `by-ref`.
- Metrics (queue depth, stage latency, failure and retry rates, cache hit ratio) and
  tracing; load tests for the limiter and claim path.
- Keep a bounded content-version history, if partners need to audit which content produced
  which result.
