# Publication-candidate regression validation

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

## Final publication-base rerun

The final source commit is
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

## Other checks actually executed

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
