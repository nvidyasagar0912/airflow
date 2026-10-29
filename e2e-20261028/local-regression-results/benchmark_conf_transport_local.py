# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# /// script
# requires-python = ">=3.12"
# ///

"""Measure isolated real JSONB/model/IPC phases, not live API-server or pod RSS."""

from __future__ import annotations

import gc
import hashlib
import json
import subprocess
import sys
import time
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from uuid import UUID

import msgspec
import psutil
import psycopg2

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext as ServerContext
from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance, TIRunContext
from airflow.sdk.execution_time.comms import StartupDetails, _ResponseFrame
from airflow.sdk.execution_time.dag_run_conf import materialize_dag_run_conf

ROOT = Path("/tmp/api-conf-benchmark-19e673db")
DSN = "postgresql://airflow:local-test-only@api-conf-postgres-19e673db:5432/airflow"
NOW = datetime(2026, 10, 28, tzinfo=UTC)
RUN = {
    "dag_id": "benchmark",
    "run_id": "run",
    "logical_date": NOW,
    "data_interval_start": None,
    "data_interval_end": None,
    "run_after": NOW,
    "start_date": NOW,
    "end_date": None,
    "run_type": "manual",
    "state": "running",
    "consumed_asset_events": [],
    "partition_key": None,
}


def build_startup(context: TIRunContext) -> StartupDetails:
    return StartupDetails(
        ti=TaskInstance(
            id=UUID(int=1),
            dag_version_id=UUID(int=2),
            task_id="task",
            dag_id="benchmark",
            run_id="run",
            try_number=1,
        ),
        dag_rel_path="benchmark.py",
        bundle_info=BundleInfo(name="benchmark", version=None),
        start_date=NOW,
        ti_context=context,
        sentry_integration="",
    )


def measure(phase: str, mode: str, count: int) -> dict:
    wire = (ROOT / f"{mode}.json").read_bytes()
    frame_bytes = (ROOT / f"{mode}.msgpack").read_bytes()
    retained = []
    connection = psycopg2.connect(DSN) if phase == "api" else None
    gc.collect()
    process = psutil.Process()
    starting_rss = process.memory_info().rss
    rss_peak = [starting_rss]
    stop_sampling = Event()

    def sample_rss() -> None:
        while not stop_sampling.wait(0.002):
            rss_peak[0] = max(rss_peak[0], process.memory_info().rss)

    sampler = Thread(target=sample_rss)
    sampler.start()
    tracemalloc.start()
    started = time.perf_counter()
    context = None
    try:
        for _ in range(count):
            if phase == "api":
                with connection.cursor() as cursor:
                    projection = "conf" if mode == "baseline" else "conf::text, jsonb_typeof(conf)"
                    cursor.execute(f"SELECT {projection} FROM api_conf_bench_19e673db WHERE id = 1")
                    row = cursor.fetchone()
                run = dict(RUN, conf=row[0] if mode == "baseline" else None)
                kwargs = {"serialized_dag_run_conf": row[0]} if mode == "patched" else {}
                context = ServerContext(dag_run=run, max_tries=0, **kwargs)
                retained.append((context, context.model_dump_json(exclude_unset=True)))
            elif phase == "supervisor":
                context = TIRunContext.model_validate_json(wire)
                startup = build_startup(context)
                body = startup.model_dump()
                retained.append((context, body, _ResponseFrame(id=0, body=body).as_bytes()))
            else:
                frame = msgspec.msgpack.decode(frame_bytes[4:], type=_ResponseFrame)
                startup = StartupDetails.model_validate(frame.body)
                context = startup.ti_context
                materialize_dag_run_conf(context)
                retained.append(startup)
        elapsed = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
        rss_peak[0] = max(rss_peak[0], process.memory_info().rss)
    finally:
        tracemalloc.stop()
        stop_sampling.set()
        sampler.join()
        if connection is not None:
            connection.close()

    if phase == "api" and mode == "patched":
        if context.dag_run.conf is not None or not context.serialized_dag_run_conf:
            raise RuntimeError("API benchmark materialized or lost opaque conf")
    if phase == "supervisor" and mode == "patched":
        if context.dag_run.conf is not None or not context.serialized_dag_run_conf:
            raise RuntimeError("Supervisor benchmark materialized or lost opaque conf")
    digest = None
    if phase == "task":
        canonical = json.dumps(context.dag_run.conf, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(canonical).hexdigest()
    return {
        "phase": phase,
        "mode": mode,
        "retained_starts": count,
        "python_peak_mib": round(peak / 1024**2, 2),
        "starting_rss_mib": round(starting_rss / 1024**2, 2),
        "sampled_process_peak_rss_mib": round(rss_peak[0] / 1024**2, 2),
        "elapsed_seconds": round(elapsed, 3),
        "task_conf_sha256": digest,
    }


def main() -> None:
    ROOT.mkdir(exist_ok=True)
    if len(sys.argv) > 1:
        print("BENCH_RESULT " + json.dumps(measure(sys.argv[1], sys.argv[2], int(sys.argv[3]))))
        return
    payload = {
        "records": [
            {
                "id": index,
                "data": {f"v{key}": f"{index:05d}-{key:02d}" for key in range(27)},
                "flags": [True, False, None],
                "values": [index, 1.25],
            }
            for index in range(23000)
        ]
    }
    compact = json.dumps(payload, separators=(",", ":"))
    with psycopg2.connect(DSN) as connection, connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS api_conf_bench_19e673db (id integer PRIMARY KEY, conf jsonb)"
        )
        cursor.execute("TRUNCATE api_conf_bench_19e673db")
        cursor.execute("INSERT INTO api_conf_bench_19e673db VALUES (1, %s::jsonb)", (compact,))
        cursor.execute("SELECT conf::text FROM api_conf_bench_19e673db WHERE id = 1")
        persisted_text = cursor.fetchone()[0]
    for mode in ("baseline", "patched"):
        run = dict(RUN, conf=payload if mode == "baseline" else None)
        kwargs = {"serialized_dag_run_conf": persisted_text} if mode == "patched" else {}
        context = ServerContext(dag_run=run, max_tries=0, **kwargs)
        wire = context.model_dump_json(exclude_unset=True).encode()
        (ROOT / f"{mode}.json").write_bytes(wire)
        startup = build_startup(TIRunContext.model_validate_json(wire))
        (ROOT / f"{mode}.msgpack").write_bytes(_ResponseFrame(id=0, body=startup.model_dump()).as_bytes())
    del payload, compact, persisted_text, context, startup, wire
    results = []
    for count in (1, 4):
        for phase in ("api", "supervisor", "task"):
            for mode in ("baseline", "patched"):
                output = subprocess.run(
                    [sys.executable, __file__, phase, mode, str(count)],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                line = next(line for line in output.stdout.splitlines() if line.startswith("BENCH_RESULT "))
                result = json.loads(line.removeprefix("BENCH_RESULT "))
                results.append(result)
                print(json.dumps(result), flush=True)
    task_digests = {result["task_conf_sha256"] for result in results if result["phase"] == "task"}
    if len(task_digests) != 1:
        raise RuntimeError("Baseline and patched task configurations differ")
    report = {
        "scope": "Isolated phases using actual PostgreSQL JSONB driver, Airflow models and MessagePack IPC.",
        "limitations": (
            "Not HTTP-server, multi-pod, Celery or live ADME load; retained starts simulate overlap. "
            "RSS is sampled per process; Python allocation peaks use tracemalloc."
        ),
        "baseline": "Dictionary JSONB retrieval and dictionary model/IPC pipeline.",
        "patched": "Text JSONB retrieval, opaque model/IPC pipeline, task-side materialization.",
        "records": 23000,
        "serialized_conf_bytes": len((ROOT / "baseline.json").read_bytes()),
        "results": results,
    }
    (ROOT / "results.json").write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
