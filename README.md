# TaskRunner

An HTTP service that accepts tasks, spreads them across two priority queues, and processes them with a pool of workers behind a rate limiter. Results are cached in memory. It shuts down without dropping work that has already been accepted.

I wrote it to get hands-on with Go concurrency: channels, `select`, mutexes, atomics, `context` cancellation and shutdown ordering. 

Only two non-stdlib dependencies: `gorilla/mux` for routing and `google/uuid` for IDs. The rate limiter and the worker pool are hand-written.

## Running

```bash
go run ./cmd/app
```

Listens on `:8080`. The default mode simulates work, so no network access is needed.

```bash
# submit
curl -s -X POST localhost:8080/tasks \
  -d '{"url":"https://example.com","priority":"high"}'
# {"task_id":"8f14e45f-..."}

# check on it
curl -s localhost:8080/tasks/8f14e45f-...
# {"status":"done","result":"simulated check of https://example.com: ok"}

curl -s localhost:8080/metrics
# {"processed":1,"failed":0,"cancelled":0,"active_workers":8,"average_process_time_ms":201}
```

To actually fetch the submitted URL instead of simulating:

```bash
TR_MODE=http go run ./cmd/app
# {"status":"done","result":"GET https://example.com -> 200 OK"}
```

Logs are JSON via `log/slog`, with `task_id` on anything task-related.

## API

| | | |
|---|---|---|
| `POST` | `/tasks` | takes `{"url": "...", "priority": "high"\|"low"}`, returns `{"task_id": "..."}` with `202` |
| `GET` | `/tasks/{id}` | returns `{"status": "...", "result": "..."}`, or `404` |
| `GET` | `/metrics` | counter snapshot |
| `GET` | `/_info` | health check |

A task goes `pending` → `done`, `failed` or `cancelled`. For the last two the reason is in `result`.

Failures return `400` (bad JSON, missing `url`, priority outside `high`/`low`), `503` with `Retry-After` (queue full or enqueue timed out), or `404`. The body is always `{"error": "...", "code": "..."}`.

## Configuration

Environment variables, loaded once via `sync.Once` and validated at startup. See `.env.example`.

| Variable | Default | |
|---|---|---|
| `TR_PORT` | `8080` | |
| `TR_ENV` | `dev` | `dev` turns on debug logging |
| `TR_N_WORKERS` | `8` | worker goroutines |
| `TR_CHAN_CAP` | `64` | buffer per priority queue |
| `TR_SEM_CAP` | `4` | max concurrent operations |
| `TR_CAPACITY` | `10` | token bucket burst |
| `TR_TICKER_TIME` | `100ms` | token refill interval, so 10/s |
| `TR_TOKEN_WAIT` | `500ms` | how long a worker waits for a token |
| `TR_ENQUEUE_TIMEOUT` | `5s` | must be less than `TR_SHUTDOWN_HTTP` |
| `TR_TASK_TIMEOUT` | `3s` | HTTP client timeout in `http` mode |
| `TR_SIMULATE_FOR` | `200ms` | simulated work duration |
| `TR_SHUTDOWN_HTTP` | `10s` | |
| `TR_SHUTDOWN_WORKERS` | `15s` | |
| `TR_OVERFLOW` | `reject` | `reject` or `block` |
| `TR_MODE` | `sleep` | `sleep` or `http` |
| `TR_STORE` | `mutex` | `mutex` or `syncmap` |

Bad values are reported together via `errors.Join` and the process refuses to start. Unparseable values keep the default rather than silently becoming zero.

One of the checks is a relationship, not a range: `TR_ENQUEUE_TIMEOUT` has to be smaller than `TR_SHUTDOWN_HTTP`. A handler can sit on the enqueue for that long, and the queues get closed as soon as `Shutdown` returns, so the other way round gives you an occasional "send on closed channel" panic under load.

## How it works

**Priorities.** Two buffered channels. A worker first does a non-blocking read on the high-priority one, and only if that comes up empty does it block on a `select` over both. The `default` in the first `select` is what keeps low priority alive: without it the worker would park on an empty high queue and never look at low.

**Concurrency limit.** Eight worker goroutines does not mean eight simultaneous operations. Before doing work a worker takes a slot from a `chan struct{}` of size `TR_SEM_CAP`, released in a `defer` right after acquiring. Goroutines are cheap and can sit waiting; the thing being hammered downstream is what needs the cap.

**Overflow.** `reject` mode uses `select` with `default` and fails instantly on a full queue. `block` mode drops the `default` and waits until the request context expires. These have to be separate branches — `default` means "never wait", so putting it next to `ctx.Done()` would make the cancellation branch dead code.

**Rate limiter.** A goroutine drops tokens into a buffered channel on a ticker. The bucket starts full so the burst is available immediately. Taking a token has a fast path that skips allocating a timer, and the slow path selects over the token, a timer and `ctx.Done()`. Timeout and cancellation return different errors, because one means the task failed and the other means it was cancelled.

**Cancellation.** Both work modes select on `ctx.Done()`. This matters: `time.Sleep` cannot be interrupted, so a sleep-based stub makes cancellation impossible to test no matter how carefully the context is threaded.

**Store.** A `map` behind a `sync.RWMutex`, plus a `sync.Map` version behind the same interface. `TR_STORE` picks one.

**Shutdown.** Signal arrives, then: stop the HTTP server, close the queues, let the workers drain them, wait on the `WaitGroup` with a timeout, cancel the app context to stop the limiter, return from `main`.

The order is load-bearing in two places. Closing the queues before `Shutdown` returns would panic on any handler still mid-send. And the workers exit when both queues are closed and drained, not on `ctx.Done()` — a worker that returned on cancellation would throw away whatever was still buffered. Anything that can't be finished is written to the store as `cancelled` with a reason and logged.

## Tests

```bash
go test ./... -race
```

34 tests. The ones worth knowing about: the store gets hammered by 52 goroutines released through a barrier channel; `LoadConfig` is called from 200 goroutines and the results compared by pointer; the priority test submits low-priority tasks first and asserts high ones still run first; the shutdown test queues 40 tasks, stops immediately, and checks every one of them reached a terminal status; the cancellation test interrupts a 5-second task and measures how long that took.

The race detector is the point of most of these, not an afterthought.

## RWMutex vs sync.Map

```bash
go test ./internal/domain -bench=BenchmarkStorage -benchmem
```

On 4 cores, `sync.Map` is about 1.4x faster at 100% reads and roughly even at 90%. At 50% writes it is 1.3x slower, at 90% writes 1.6x slower, and its allocations climb from 13 to 100 B/op as writes grow while `RWMutex` stays flat.

That matches what `sync.Map` is for: a mostly-fixed key set read from many goroutines, where it can serve reads without a shared lock. Once writes are a real share of the load, boxing every value into an `interface{}` and touching the dirty map costs more than just taking a mutex. This service does two writes per task plus some reads, which puts it near the break-even point, so the `RWMutex` version is the default — it's simpler and type-safe without an assertion.

## Layout

```
cmd/app/            entry point, shutdown ordering
internal/api/       handlers, routing, DTOs
internal/cfg/       config singleton and validation
internal/domain/    worker pool, store, limiter, metrics
internal/errors/    sentinel errors
```
