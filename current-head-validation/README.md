# Publication-candidate regression validation

## Latest review-readiness verification

Final source: `448ca150662cb9c5f8da0ee1655227de20186073`, based on community
main `24f3370758227ce1bb13e5df251d26a9271ceeee`.
The original Python startup code is unchanged from `81fe003`; the final commit
adds only the four officially generated Go/TypeScript schema/export updates
required by full static checks. All 20 changed file blobs match the actual
Linux test checkout.

| Latest scope | Result | Evidence |
| --- | --- | --- |
| Entire Task SDK `execution_time` directory | 1,588 passed; 1 platform skip | [JUnit](../review-readiness-20261029/sdk-execution-time.xml) |
| Entire SQLite head task-instance API file | 383 passed; 6 backend/row-lock skips | [JUnit](../review-readiness-20261029/api-sqlite-full-head.xml) |
| PostgreSQL startup and older-version selections | 121 passed | [JUnit](../review-readiness-20261029/api-postgres.xml) |
| MySQL startup and older-version selections | 121 passed | [JUnit](../review-readiness-20261029/api-mysql.xml) |
| Non-overlapping Python/backend total | 2,213 passed; 7 expected skips; no failures/errors | Broader SDK/SQLite runs replace overlapping 780-case selections |
| TypeScript SDK | 935 tests passed in 32 files; typecheck/build/format passed | [Tests](../review-readiness-20261029/ts-tests.log), [typecheck](../review-readiness-20261029/ts-typecheck.log), [build](../review-readiness-20261029/ts-build.log) |
| Go execution, generated models and SDK | 3 test-bearing packages passed | [Output](../review-readiness-20261029/go-execution.log) |
| Applicable fast pre-commit hooks, all 20 changed files | Passed | [Output](../review-readiness-20261029/precommit-final.log) |
| Applicable manual hooks | Passed; only 3 opt-in server/environment hooks excluded as documented | [Output](../review-readiness-20261029/manual-final.log) |
| Full Airflow-core and Task SDK mypy | Passed; devel-common mypy also passed in the full fast stage | [Output](../review-readiness-20261029/mypy.log) |
| Ruff lint/format | Passed | [Output](../review-readiness-20261029/ruff.log) |
| `breeze verify --json` | Plan generated, not execution of its commands | [Plan](../review-readiness-20261029/breeze-verify.json) |

Package access was restored through the host-configured package mirror.
No dependency manifests or lockfiles changed. The required Go/TypeScript
exports were regenerated, formatted and committed with normal Linux hooks.
No generated source was edited by hand.

The remaining full Breeze-selected core/provider/docs matrix and upstream CI
are not claimed passed. `breeze build-docs` could not reach
`unix:///var/run/docker.sock` in the validation container, and upstream CI
requires maintainer workflow approval.

Three alternating actual base/head phase benchmarks covered null, empty,
small and large configuration. All controls stayed within 10%.
See [raw samples and medians](../review-readiness-20261029/benchmark-summary.json).
They explicitly report task-side latency/allocation and wire-size increases;
they are not live container peaks or a rerun of the historical full stack.

## Initial publication regression run (historical)

The source baseline for these runs is
`6056deb572e1e8fd5a51654ad0a22c963e5a6a69`, plus
[validated-candidate.patch](validated-candidate.patch).
These results are separate from the historical performance comparison.

| Suite | Passing cases | JUnit report |
| --- | ---: | --- |
| Task SDK configuration, IPC, migrator, runtime context and impersonation | 442 | [sdk.xml](sdk.xml) |
| Execution API startup on SQLite | 96 | [api-sqlite.xml](api-sqlite.xml) |
| Execution API startup and older API versions on PostgreSQL | 121 | [api-postgres.xml](api-postgres.xml) |
| Execution API startup and older API versions on MySQL | 121 | [api-mysql.xml](api-mysql.xml) |
| **Total** | **780** | **0 failures, 0 errors** |

This includes explicit SQL NULL separately from JSON null at four negotiated
API versions, in addition to empty and nested configuration.

## Earlier rebased publication run (historical)

The source commit for this earlier rerun was
`8b8ef50a7914183f766690b83d5bc92146ad00ca`, rebased onto
`ddb89d5e006ebabea298dd22396b73ee0a0e463c`.
The 16 candidate file blobs in the tested CI checkout were independently
verified to match that committed head. Neither the candidate files nor
application production code changed across the final rebase; upstream test
infrastructure and unrelated tests did change.

| Repeated suite | Passing cases | JUnit report |
| --- | ---: | --- |
| Same Task SDK selection | 442 | [final-sdk.xml](final-sdk.xml) |
| Same SQLite API selection | 96 | [final-api-sqlite.xml](final-api-sqlite.xml) |
| **Final-base rerun total** | **538** | **0 failures, 0 errors** |

These are repeat executions of cases already counted in the 780-case table,
not additional distinct coverage. PostgreSQL/MySQL were not rerun on this
last base; their 121/121 results above remain tied to the earlier base.
`git diff --check` also passed on the final-base test checkout.

## Initial publication static checks (historical)

The list below records the original draft-publication status. The latest
readiness results above supersede its pending fast/manual-check entries.

- Official Task SDK model generation: passed.
- Official supervisor schema generation: passed.
- Generator version: `datamodel-code-generator==0.83.0`, matching the source lock.
- Ruff lint and formatting: passed for the changed Python files.
- `git diff --check`: passed.
- Targeted `mypy --follow-imports=silent`: passed for seven changed production
  modules. This is not the complete upstream type-check matrix.
- `prek install`: completed in the Linux CI validation checkout.
- Explicit-file fast `prek` run: **blocked during dependency/environment
  installation**, not a passing hook suite. PyPI requests for hook build
  dependencies timed out. The retry did not complete.
- Full manual hooks, `breeze verify`, the three-baseline/three-candidate
  benchmark protocol and complete CI checks remain pending.

The draft publication was explicitly authorized with those checks pending.
Do not interpret the commit or draft PR as a claim that the blocked hook
suite passed.

## Commands and runtime

All application tests ran inside the pinned official Linux CI image, not
Windows Python:

```text
ghcr.io/apache/airflow/main/ci/python3.12@sha256:42cd6b3d711b3a67bf3047d7c93dcefb4bd997b6bd059ac6a28acac11c99eab7
```

The runner was:

```text
bash scripts/in_container/run_ci_tests.sh <selectors> -q --disable-warnings --junitxml=<report>
```

SDK selectors:

```text
task-sdk/tests/task_sdk/execution_time/test_dag_run_conf.py
task-sdk/tests/task_sdk/execution_time/schema/test_migrator.py
task-sdk/tests/task_sdk/execution_time/test_comms.py
task-sdk/tests/task_sdk/execution_time/test_task_runner.py::TestRuntimeTaskInstance
task-sdk/tests/task_sdk/execution_time/test_task_runner.py::test_task_run_with_user_impersonation
```

Execution API selectors:

```text
airflow-core/tests/unit/api_fastapi/execution_api/versions/head/test_task_instances.py::TestSerializedTaskStartConf
airflow-core/tests/unit/api_fastapi/execution_api/versions/head/test_task_instances.py::TestTIRunState
```

PostgreSQL and MySQL additionally ran:

```text
airflow-core/tests/unit/api_fastapi/execution_api/versions/v2025_09_23/test_task_instances.py
airflow-core/tests/unit/api_fastapi/execution_api/versions/v2026_04_06/test_task_instances.py
```

The databases were task-owned local containers with no published host ports.
`_AIRFLOW__AS_LIBRARY` was unset for application tests so normal ORM
initialization and migration ran. The generated outputs were copied from the
official generators; they were not edited by hand.
