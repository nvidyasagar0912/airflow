# Task-start configuration transport: review evidence

This is **fork-only supporting evidence**, not additional files proposed for
the Apache Airflow source tree.

The change keeps large `DagRun.conf` values serialized through the Execution
API and Python supervisor, then restores the task-visible dictionary in the
Python task process. It does not change the scheduler or database schema.

## What the measurements actually show

The historical local comparison used the same Airflow baseline and dependency
image for an unpatched / patched / unpatched-again sequence:

- Baseline: `750f849cc2372ee42dc6910541d4b6a866102ac9`.
- Candidate: that baseline plus
  [the exact tested patch](e2e-20261028/source.diff).
- Patch SHA256:
  `1b953037b828adcd32fef174c2e3da3d1f45910488c5e9c25f8b79bb2b4f65c4`.
- Large scenario: 20 admitted DagRuns, 23,000 synthetic records and roughly
  11.9 MB of nested configuration per run, with four tasks executing at once.

| Large-workload metric | Baseline A1 | Candidate | Baseline A2 |
| --- | ---: | ---: | ---: |
| Simultaneous seven-container working-set peak, MiB | 5,091.7 | 5,031.3 | 4,795.5 |
| Workload completion time, seconds | 126.6 | 105.2 | 124.3 |

**No full-stack peak-memory reduction was established.** The candidate peak
was 1.8% above the mean of the two baselines, within their 6.0% spread.
Do not add independent component or isolated-phase peaks into a total.

The candidate's individual API-server peak was 24.1% lower and the worker
peak 9.8% lower than the two-baseline means. PostgreSQL's own peak was 35.4%
higher; the cause was not established. Completion was 16.1% shorter in the
single candidate observation, not a statistically established production
speedup.

## Test evidence and exact revision boundaries

| Evidence layer | Revision / scope | Result |
| --- | --- | --- |
| Historical regression suites | Original `750f849c`-based candidate | 736 final passing cases across SDK, SQLite, PostgreSQL and MySQL |
| Historical real local pipeline | A1 / candidate / A2 on that exact baseline | 111 declared DagRun outcomes verified: 108 successes and 3 intentional negative failures |
| Startup-readiness control | Unpatched baseline only, cold versus health-warmed API | 8 completed runs; warming `/execution/health` removed the observed HTTP 400s in the small control |
| Current publication regression suites | `6056deb572e1e8fd5a51654ad0a22c963e5a6a69` plus minor review corrections and genuine SQL-NULL coverage | 780 passing cases; see [current-head validation](current-head-validation/) |

The performance measurements were **not rerun on the rebased publication
head**. They are explicitly historical measurements of the pinned candidate,
not a claim that the current head has identical measured memory or timing.

The current-head changes reuse the existing dialect helper, specify a test
mock, move new test imports, clarify the preserved rollback behavior and the
500-to-422 status change, and test actual SQL NULL separately from JSON null.

## Start here

- [Readable end-to-end report](e2e-20261028/REVIEW-EVIDENCE.md).
- [Configurations, workload and reproduction protocol](e2e-20261028/TEST-PROTOCOL.md).
- [Machine-readable comparison](e2e-20261028/comparison.json).
- [Raw verified run results and timings](e2e-20261028/e2e-results.json).
- [Synchronized memory chart](e2e-20261028/large-memory-timeseries.svg).
- [Pinned images and resource settings](e2e-20261028/source-manifest.json).
- [Reproduction and analysis scripts](e2e-20261028/harness/).
- [Current-head validation](current-head-validation/).

Per-arm folders retain raw memory/CPU samples, state transitions and service
logs. Bind `e2e-20261028/` at `/evidence` when using its reproduction harness.
Read its resource and private-network requirements first.

## Limitations and remaining work

- The original A/B/A comparison had 16 recovered task-start HTTP 400s per
  arm. The separate unchanged-baseline control reproduced four cold errors
  and zero after execution-health warm-up. The exact transport mechanism was
  not established, and the large comparison was not rerun with the warm-up.
- The upstream performance protocol still needs at least three alternating
  baseline and three candidate runs with an unaffected control.
- Empty/small configuration, legacy/foreign compatibility costs and actual
  HTTP/IPC byte counts require further measurement.
- The event-collection read deliberately adds a database round trip to avoid
  repeating large configuration text across joined event rows.
- This is a local synthetic Airflow transport test, not a production image,
  cluster rollout, cloud-instance or external ingestion/search test.
- Boundary numeric behavior and minimum-supported SQLite non-finite handling
  are not established by the ordinary-JSON pipeline comparison.
- This evidence supports discussion of a component optimization, not a
  claim of universal memory reduction or regression-free operation.

## Public-export note

The data is synthetic and the reproduction configuration is local-only.
There are no customer payloads or cloud credentials in this bundle.
The historical validation metadata's local repository path was replaced with
`/workspace/airflow`; numerical measurements, payload hashes and the tested
source patch were not changed.
