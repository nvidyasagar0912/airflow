# Task-start configuration transport: review evidence

This is **fork-only supporting evidence**, not additional files proposed for
the Apache Airflow source tree.

Source proposal: [Apache Airflow PR #75935](https://github.com/apache/airflow/pull/75935).
Historical reports retain their time-of-test status notes; they do not
describe the current publication state.

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
| Final publication-base rerun | Publication head `8b8ef50a7914183f766690b83d5bc92146ad00ca` on `ddb89d5e006ebabea298dd22396b73ee0a0e463c` | 538 previously selected SDK/SQLite cases passed again; this is a rerun, not additional distinct coverage |
| Latest review-readiness verification | Final source `448ca150662cb9c5f8da0ee1655227de20186073` on `24f3370758227ce1bb13e5df251d26a9271ceeee` | 2,213 non-overlapping Python/backend cases passed, 7 expected skips; 935 TypeScript tests and 3 Go test packages passed; applicable fast/manual hooks passed |
| Current-base isolated performance protocol | Three actual base/head pairs at `24f3370` / `81fe003`; final commit only synchronizes Go/TypeScript generated exports | All unchanged controls within 10%; null/empty/small/large phases and byte counts recorded in [readiness results](review-readiness-20261029/) |

The performance measurements were **not rerun on the rebased publication
head**. They are explicitly historical measurements of the pinned candidate,
not a claim that the current head has identical measured memory or timing.

The current-head changes reuse the existing dialect helper, specify a test
mock, move new test imports, clarify the preserved rollback behavior and the
500-to-422 status change, and test actual SQL NULL separately from JSON null.
All 16 candidate file blobs in the final-base test checkout were verified
against the committed publication head. The last rebase changed no candidate
files or application production code; PostgreSQL/MySQL were not rerun after it.
That statement describes the earlier rebase. The latest readiness run repeated
both native-backend selections on the newer base and broadened SDK/SQLite
coverage. All 20 final changed file blobs were verified against the tested
Linux checkout; see [source verification](review-readiness-20261029/source-verification.json).

## Latest isolated findings and trade-offs

These are not a repeat of the live full-stack memory experiment.
Current-base isolated Python allocation peaks for four retained starts were
285.20 to 127.64 MiB in the JSONB/API-model phase and 488.48 to 127.69 MiB in
the supervisor phase. Task allocation peak increased from 234.76 to 239.71 MiB.
Task startup decode time increased 38.7%; API-model JSON grew 34.7% and actual
MessagePack frames grew 50.8%. Simulated gzip level-6 model size grew 5.7%;
it is not a measurement of negotiated HTTP transfer.

The direct JSONB projection excludes the full route's ORM/state logic and the
extra asset query. It does not establish common-path endpoint cost or a
full-stack benefit. See [all medians and controls](review-readiness-20261029/benchmark-summary.json)
and [chronological run output](review-readiness-20261029/benchmark-run.log).

## Start here

- [Readable end-to-end report](e2e-20261028/REVIEW-EVIDENCE.md).
- [Configurations, workload and reproduction protocol](e2e-20261028/TEST-PROTOCOL.md).
- [Machine-readable comparison](e2e-20261028/comparison.json).
- [Raw verified run results and timings](e2e-20261028/e2e-results.json).
- [Synchronized memory chart](e2e-20261028/large-memory-timeseries.svg).
- [Pinned images and resource settings](e2e-20261028/source-manifest.json).
- [Reproduction and analysis scripts](e2e-20261028/harness/).
- [Current-head validation](current-head-validation/).
- [Latest readiness logs, broader JUnit reports and alternating phase samples](review-readiness-20261029/).

Per-arm folders retain raw memory/CPU samples, state transitions and service
logs. Bind `e2e-20261028/` at `/evidence` when using its reproduction harness.
Read its resource and private-network requirements first.

## Limitations and remaining work

- The original A/B/A comparison had 16 recovered task-start HTTP 400s per
  arm. The separate unchanged-baseline control reproduced four cold errors
  and zero after execution-health warm-up. The exact transport mechanism was
  not established, and the large comparison was not rerun with the warm-up.
- The isolated changed-code protocol now has three alternating pairs with
  an unaffected control. A warmed, three-pair live full-stack comparison
  remains pending; historical component peaks are not newly repeated results.
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
