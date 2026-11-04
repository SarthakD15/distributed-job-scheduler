# Distributed Job Scheduler

A small distributed job scheduler written in C++20. A client submits jobs (commands to run),
a central **broker** queues them and dispatches them to a pool of **worker** processes over
**ZeroMQ**, and workers run each command and report the result. Job state is stored in
**PostgreSQL**, so nothing is lost if the broker crashes, and failed jobs can be diagnosed by
an **LLM** through `jobctl explain`.

The focus of the project is failure handling: detecting dead workers, requeueing their jobs,
and recovering cleanly after a broker restart.

## Features

- Broker–worker architecture over ZeroMQ (ROUTER/DEALER), any number of workers
- FIFO scheduling: each job goes to the longest-idle worker
- Heartbeats with acknowledgements: dead workers are detected in about 3 seconds and their
  jobs are requeued
- PostgreSQL persistence with crash recovery: queued and running jobs survive a broker restart
- Per-job log files, with exit code, duration and output reported back to the broker
- AI failure analysis: `jobctl explain` asks an LLM (local via Ollama, or any
  OpenAI-compatible API) for the likely cause of a failed job
- Command-line client: `submit`, `status`, `list`, `explain`

## Architecture

```
                         ┌──────────────────────────────────────┐
  jobctl (CLI)           │                broker                │
  submit/status/list/    │                                      │
  explain        REQ ───►│ ROUTER :5555            ROUTER :5556 │◄──► worker w1 (DEALER)
                         │                                      │◄──► worker w2 (DEALER)
                         │  job table · FIFO queue · registry   │◄──► ...
                         │               │                      │
                         │           JobStore ──► PostgreSQL    │
                         └──────────────────────────────────────┘
                                  each worker runs jobs as OS processes
                                  and writes logs/<job-id>.log
```

| Component | Responsibility |
|---|---|
| **jobctl** | Sends one request to the broker and prints the reply. `explain` also calls the LLM. |
| **broker** | Owns all job state: queues jobs, dispatches them, records results, tracks worker health, recovers from the database after a restart. Single-threaded event loop. |
| **worker** | Receives one job at a time, runs it as a separate process without blocking, sends heartbeats, reports the result. |
| **common** | Shared protocol: message types, job structs, JSON conversion, ZeroMQ framing helpers. |

### Life of a job

```
QUEUED ──dispatch──► RUNNING ──exit 0──► SUCCEEDED
   ▲                    │
   └── worker died ─────┤
                        └──exit ≠ 0 / could not start──► FAILED
```

1. `jobctl submit ...` sends the command; the broker saves it in PostgreSQL and replies with an ID.
2. The broker marks the job RUNNING in the database, then sends it to an idle worker.
3. The worker runs the command, writing stdout and stderr to `logs/<job-id>.log`.
4. The worker sends back the exit code, duration and the first 64 KB of output.
5. The broker stores the result; `jobctl status <job-id>` shows it.

## Failure handling

| Failure | What happens |
|---|---|
| Worker dies while running a job | No message for 3 s → worker removed, job requeued at the front of the queue |
| Broker crashes, workers keep running | Workers keep heartbeating; ZeroMQ reconnects automatically. The restarted broker reloads jobs from PostgreSQL and accepts results from workers that reclaim their jobs |
| Broker crashes and no worker reclaims a running job | After a 3 s grace period the job is requeued |
| A worker wrongly declared dead sends a late result | Rejected: results are only accepted from the worker the job is currently assigned to |
| Program not found / non-zero exit | Job marked FAILED with the exit code and error |
| Database unavailable | Broker stops with a clear error (fail fast) and recovers on restart |

Delivery is **at-least-once**: a job may run twice in rare cases (a worker that was slow, not
dead), so jobs should be idempotent.

## AI failure analysis

```
$ jobctl explain job-17
job-17 FAILED (exit 1), asking model 'llama3.2'...

AI suggestion (may be wrong - check the log):
Likely cause: the input file C:\jobdata\in\missing.txt does not exist.
Suggested fix: check the path is correct on the worker's machine.
```

`jobctl explain` sends the command, exit code and the last 4 KB of the job's log to a
language model using the OpenAI-compatible chat API. It defaults to a **local model through
Ollama**, so logs never leave the machine. Any compatible cloud provider works by changing
environment variables. The call is made from the CLI, never from the broker, so the
scheduling loop never blocks. The log is treated as untrusted input to guard against prompt
injection.

## Tech stack

C++20 · ZeroMQ (libzmq, cppzmq) · PostgreSQL (libpqxx) · jsoncons · reproc++ · libcurl ·
Ollama · CMake · MSYS2/MinGW-w64

## Project structure

```
common/   protocol definitions (util.h), JSON conversion, ZeroMQ framing helpers
broker/   Broker, WorkerRegistry, JobStore interface, PostgresJobStore
worker/   Worker, ProcessExecutor (wraps reproc++)
jobctl/   JobCtl client, FailureExplainer (LLM over HTTP)
```

## Building (Windows, MSYS2 MinGW64)

**Dependencies**

- From pacman:
  ```
  pacman -S mingw-w64-x86_64-postgresql mingw-w64-x86_64-libpqxx mingw-w64-x86_64-curl
  ```
- Built and installed into an external folder (`include/`, `lib/`, `bin/`): libzmq, cppzmq,
  jsoncons, reproc/reproc++ (with `-DREPROC++=ON`). By default CMake looks two levels above
  the project; set `-DEXT_ROOT=<path>` to change it.

**Build**

```
cmake -S . -B build -G "MSYS Makefiles"
cmake --build build
```

Executables are copied to `run/bin`.

## Running

**1. PostgreSQL** (first time only: create the data folder and database)

```
initdb -D ~/pgdata -U postgres -A trust -E UTF8 --no-locale
pg_ctl -D ~/pgdata -l ~/pgdata/server.log start
createdb -U postgres jobsched
```

The broker creates its table on first start.

**2. Broker and workers** (each in its own terminal, from `run/bin`)

```
./broker                       # optional argument: PostgreSQL connection string
./worker w1                    # optional argument: worker name
./worker w2
```

**3. Submit and inspect jobs**

```
./jobctl submit ping -n 4 127.0.0.1
./jobctl list
./jobctl status job-1
./jobctl explain job-1         # for a FAILED job; needs Ollama running
```

**Example: failover**

```
[broker] dispatched job-1 -> w1
[broker] w1 is dead (no message for 3000 ms)
[broker] re-queued job-1 (worker w1 is gone)
[broker] dispatched job-1 -> w2
[broker] job-1 SUCCEEDED on w2 (exit 0, 14071 ms)
```

## Configuration

| Setting | Where | Default |
|---|---|---|
| Database connection | broker's first argument | `host=localhost port=5432 dbname=jobsched user=postgres` |
| Ports | `common/include/util.h` | 5555 (clients), 5556 (workers) |
| Heartbeat interval / timeout | `common/include/util.h` | 1 s / 3 s |
| LLM endpoint | `JOBSCHED_LLM_URL` | `http://localhost:11434/v1/chat/completions` |
| LLM model | `JOBSCHED_LLM_MODEL` | `llama3.2` |
| LLM API key | `JOBSCHED_LLM_API_KEY` | empty (not needed for Ollama) |
