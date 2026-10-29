# Local Airflow task-start configuration comparison

## Scope and reproducibility boundary

This bundle tests the API/Task SDK configuration transport change. It does not
include a scheduler optimization, change PostgreSQL's JSONB schema, connect to
ADME, or create a pull request.

- Baseline: Apache Airflow commit
  `750f849cc2372ee42dc6910541d4b6a866102ac9`.
- Candidate: that exact baseline plus [source.diff](source.diff).
- Source digest and pinned image digest:
  [source-manifest.json](source-manifest.json).
- The baseline and candidate use the same Linux CI image and installed
  dependencies. No different Airflow release is substituted for the baseline.
- Generated Task SDK models and supervisor schema are included in the patch;
  they were produced using the official generators.

The uncommitted source diff is the review artifact. No remote branch or PR is
required to reproduce the experiment.

## Tests and evidence layers

### 1. Focused and adjacent regression tests

[Earlier local validation](local-regression-results/validation-summary.json)
contains the exact selected suites and dependency versions:

| Layer | Successful final cases |
| --- | ---: |
| SDK configuration parsing, IPC/migration, context and impersonation | 441 |
| Execution API startup on SQLite | 90 |
| Execution API startup on PostgreSQL 16 JSONB | 90 |
| Older Execution API responses on PostgreSQL | 25 |
| Execution API startup on MySQL 8.4 JSON | 90 |
| Total | 736 |

Final JUnit XML files are in the same directory. Earlier exploratory runs are
not counted again. Ruff, formatting, whitespace validation, and targeted mypy
checks of seven changed production modules also passed.

### 2. Isolated phase measurements

[The phase report](local-regression-results/memory-benchmark.json) measures
PostgreSQL JSONB retrieval, API/SDK models and MessagePack transport separately.
It is diagnostic evidence, not a simultaneous application-total measurement.
Do not sum its independently observed phase peaks into a production total.

### 3. Real local DAG-run comparison

[The harness](harness/run_local_e2e.py) starts:

1. PostgreSQL 16.
2. Redis 7.
3. Airflow API server, including the Execution API.
4. Airflow scheduler.
5. Dag processor.
6. Celery worker, including real supervisors and child task processes.
7. Triggerer for actual task deferral/resume.

All components use private Docker networking. No host ports are published.
Test-only all-admin mode is used for the public REST driver. Workload/task
startup still uses Airflow's normal scoped Execution API tokens.
Never deploy this authentication configuration to a shared or production
environment.

The synthetic [probe Dag](harness/conf_transport_probe.py) is parsed by the
real Dag processor. Runs are admitted over the public API, dispatched by the
scheduler through Redis/Celery, started through the Execution API, and executed
in task child processes. Results are checked through the public XCom endpoint.
This is not a direct operator call or a mocked supervisor test.

## Workload

The payload is deterministic nested JSON. Each record contains an integer
identifier, 27 small string attributes, booleans, null and ordinary numeric
values. No customer data, OSDU service credentials, or external ingestion
services are involved.

Each arm runs these scenarios in the same order:

| Scenario | Submitted DagRuns | Records per configuration | Purpose |
| --- | ---: | ---: | --- |
| Small | 4 | 20 | Small-input behavior and fixed-cost overhead |
| Medium | 8 | 2,000 | Intermediate size/concurrency |
| Large | 20 | 23,000 | Approximately 12-MB inline configuration workload |
| Contracts | 5 | 0 or 5 | Empty conf, false parameter, real retry, real deferral, expected failure |

The worker concurrency is four, and the Dag allows 24 active runs. Twenty
submitted runs therefore test backlog plus overlapping execution, not twenty
simultaneously executing child tasks. Successful performance tasks hold their
configuration for two seconds after verification so the same overlap window is
observable in each arm. These settings are identical for baseline and candidate.

The task checks:

- Canonical SHA256 equality of the complete payload, not just record count.
- Expected number of records.
- Parameter override value, including false.
- Dictionary/null shape and mutable dictionary identity.
- Real second-attempt success in the retry case.
- Successful triggerer-driven resume in the deferral case.
- A declared negative hash case ending in failed state instead of succeeding.

Every submitted run must reach its declared terminal state. Unexpected
failures, missing runs, result mismatches, component exits, incomplete memory
samples and timeouts fail the experiment.
This functional pass condition does not require every individual HTTP attempt
to succeed: SDK-recovered transport attempts are separately retained in the
access logs and must be reported, not counted as clean startup requests.

## A/B/A design

The arm order is:

1. **A1**: unpatched baseline.
2. **B**: patched candidate.
3. **A2**: the same unpatched baseline again.

Each arm gets fresh PostgreSQL/Redis data, fresh Airflow processes, identical
limits and the same workload. This provides a rollback/repeat-baseline check.
It is not a large-sample statistical study or a rolling-upgrade compatibility
test.

The Dag is paused during admission. To avoid confusing public trigger-body
allocations with the changed Execution API path, the API process is
intentionally restarted after admission and before execution sampling in
every scenario. All arms use that same procedure. Admission time is recorded
separately. The resulting memory claim is specifically about scheduling,
task startup and execution after admission, not about eliminating memory in
the public DagRun-create endpoint.

No separate scheduler fix, batch-size tuning, compression, by-reference
payload, different replica count or resource increase is introduced in B.

## Resource and sampling definitions

| Component | Memory limit | CPU limit |
| --- | ---: | ---: |
| API server | 3 GiB | 2 |
| Scheduler | 4 GiB | 2 |
| Celery worker and its children | 4 GiB | 4 |
| Dag processor | 1.5 GiB | 1 |
| Triggerer | 768 MiB | 1 |
| PostgreSQL | 1 GiB | 1 |
| Redis | 256 MiB | 0.5 |

The generous identical limits avoid deliberately provoking OOMs as a proxy
for measuring the optimization.

Memory is sampled through the Docker Engine cgroup-statistics endpoint in
parallel for all seven containers, approximately every 200 milliseconds.
Each raw row records collection duration, timestamps, per-component memory,
per-component cumulative CPU and aggregate memory.

- `memory_bytes`: Docker cgroup memory usage.
- `working_set_bytes`: usage minus `inactive_file` (or its cgroup-v1 equivalent).
- `group_working_set_bytes`: the sum of all seven working sets in that
  complete sampling row.
- **Group peak**: the maximum of those time-aligned group sums.
- `components_at_group_peak_mib`: component values at that exact group sample.
- Individual component maxima are also saved, but are explicitly not a total
  and must not be added together.

These are container working sets, not Python-only allocations, process PSS,
or an exact host-wide memory total. The observer container, Docker daemon
and operating-system memory outside the selected containers are excluded.
Sampling can miss shorter transients; the recorded collection timing makes
the measurement resolution inspectable.

## Artifacts

- [Exact source patch](source.diff).
- [Source/image/resource manifest](source-manifest.json).
- [Machine-readable final results](e2e-results.json).
- Per-arm `small/`, `medium/`, `large/` and `contracts/` directories:
  - `memory-samples.jsonl`: complete raw memory/CPU observations.
  - `run-status-samples.json`: observed run/task state transitions.
  - `summary.json`: timings, verified results, simultaneous group peak,
    component-at-peak values and individual component maxima.
- Per-arm service logs and `container-state.json`.
- [Prior unit/database results](local-regression-results/validation-summary.json).
- `setup-attempts/`: failed harness initialization attempts, retained rather
  than silently discarded.

Inspect the final `status` in `e2e-results.json`; a directory's existence is
not evidence that its workload completed.

## Reproduction

Requirements are Docker with a working Linux engine and enough free memory
for one stack. The harness is Linux-only and uses the pinned official CI
image; it is not intended to execute directly with Windows Python.

The same Docker volume names are explicit in the harness. Run it in an
isolated local Docker environment or change all those names consistently.
Do not remove similarly named resources belonging to another experiment.

Create a private bridge and two empty source volumes, mount them into a
controller at `/snapshots/baseline` and `/snapshots/patched`, and bind this
evidence directory at `/evidence`. The controller also needs the local Docker
Engine socket. Run:

```text
python /evidence/harness/run_local_e2e.py
```

The harness fetches only the pinned public baseline, applies `source.diff`
locally, and starts the application stacks. It does not fetch another
contributor's PR, push code, or connect to a cloud instance. It preserves
results/logs and removes its per-arm containers/data. Remove the two source
volumes and private network after the controller exits.

## Limits on conclusions

- This is a local synthetic transport test, not ADME/AKS/Helm or OSDU
  record-ingestion validation.
- A/B/A is evidence of repeatability, not a guarantee of no regressions.
- Mixed core/SDK deployment at the same unreleased API epoch is not tested
  or claimed supported here.
- Historical released API response conversion is covered by the separate
  compatibility tests, not by substituting a released SDK into these stacks.
- Non-finite SQLite numeric cases are outside the demonstrated normal-JSON
  parity scope.
- An API-server allocation improvement does not imply the separate scheduler
  configuration-loading or heartbeat/probe issue is fixed.
- The executed comparison observed 16 recovered HTTP 400 task-start attempts
  in each arm. A subsequent unchanged-baseline control verified four cold and
  four health-warmed runs: cold startup reproduced four HTTP 400s, whereas
  warming `/execution/health` produced zero. This narrows the effect to
  readiness/cold startup without establishing the precise transport failure
  mechanism. The full A/B/A was not rerun with the warm-up.
- The diagnostic's first OpenAPI warm-up attempt failed on the unchanged
  baseline with a response-content-length error. Its logs and the four
  earlier completed cold runs are retained outside the final comparisons.
- The CI source snapshots contain static-UI directory placeholders, not
  built frontend assets; UI functionality and production image packaging are
  not part of this backend/API test.
