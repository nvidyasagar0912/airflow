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

"""Run real, isolated baseline/patched/rollback DAG workloads; never connect to ADME."""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import secrets
import socket
import statistics
import subprocess
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import psycopg2

EVIDENCE = Path("/evidence")
BASE = "750f849cc2372ee42dc6910541d4b6a866102ac9"
IMAGE = (
    "ghcr.io/apache/airflow/main/ci/python3.12"
    "@sha256:42cd6b3d711b3a67bf3047d7c93dcefb4bd997b6bd059ac6a28acac11c99eab7"
)
NETWORK = "api-conf-e2e-net-19e673db"
BASE_VOLUME = "api-conf-e2e-base-19e673db"
PATCH_VOLUME = "api-conf-e2e-patch-19e673db"
POSTGRES_IMAGE = (
    "postgres@sha256:1beaa3084eee2676f20a9cd9928f0bf2c5bf674f8e49f81017912183d377c61c"
)
REDIS_IMAGE = (
    "redis@sha256:6ba19c50b56777f5c11c58eff8ddb69d0d5f27ba131cffb63f180ccfd43c66af"
)
DAG = "conf_transport_probe"
PREFIX = "api-conf-e2e"
CREATED_CONTAINERS: list[str] = []
CREATED_VOLUMES: list[str] = []
RESOURCES = {
    "api": ("3g", "2"),
    "scheduler": ("4g", "2"),
    "worker": ("4g", "4"),
    "dag-processor": ("1536m", "1"),
    "triggerer": ("768m", "1"),
    "postgres": ("1g", "1"),
    "redis": ("256m", "0.5"),
}


def now() -> str:
    return datetime.now(UTC).isoformat()


def command(*args: str, capture: bool = True, timeout: int = 180) -> str:
    try:
        completed = subprocess.run(
            list(args), check=False, text=True, capture_output=capture, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Command timed out: {args[0]} {args[1]}") from None
    if completed.returncode:
        detail = (completed.stderr or completed.stdout or "")[-4000:]
        raise RuntimeError(
            f"{args[0]} {args[1]} failed ({completed.returncode}): {detail}"
        )
    return completed.stdout.strip() if capture else ""


def docker(*args: str, **kwargs) -> str:
    return command("docker", *args, **kwargs)


class EngineConnection(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("localhost", timeout=10)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/var/run/docker.sock")


def engine_json(path: str) -> dict:
    connection = EngineConnection()
    try:
        connection.request("GET", f"/v1.54{path}")
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(
                f"Docker stats returned HTTP {response.status}: {body[:200]!r}"
            )
        return json.loads(body)
    finally:
        connection.close()


class Sampler:
    def __init__(self, containers: dict[str, str], output: Path):
        self.containers = containers
        self.output = output
        self.rows: list[dict] = []
        self.errors: list[str] = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            raise RuntimeError("Memory sampler did not stop")
        if self.errors:
            raise RuntimeError(f"Memory sampling was incomplete: {self.errors}")

    def _sample(self):
        try:
            with (
                self.output.open("w") as stream,
                ThreadPoolExecutor(max_workers=7) as pool,
            ):
                while not self.stop_event.is_set():
                    started = time.monotonic()
                    futures = {
                        role: pool.submit(
                            engine_json,
                            f"/containers/{name}/stats?stream=false&one-shot=true",
                        )
                        for role, name in self.containers.items()
                    }
                    components = {}
                    for role, future in futures.items():
                        raw = future.result()
                        memory = raw["memory_stats"]
                        usage = memory.get("usage", 0)
                        cache = memory.get("stats", {}).get(
                            "inactive_file",
                            memory.get("stats", {}).get("total_inactive_file", 0),
                        )
                        components[role] = {
                            "memory_bytes": usage,
                            "working_set_bytes": max(0, usage - cache),
                            "cpu_total_ns": raw["cpu_stats"]["cpu_usage"].get(
                                "total_usage", 0
                            ),
                            "read": raw["read"],
                        }
                    row = {
                        "timestamp": now(),
                        "monotonic": started,
                        "collection_duration_ms": (time.monotonic() - started) * 1000,
                        "components": components,
                        "group_memory_bytes": sum(
                            item["memory_bytes"] for item in components.values()
                        ),
                        "group_working_set_bytes": sum(
                            item["working_set_bytes"] for item in components.values()
                        ),
                    }
                    self.rows.append(row)
                    stream.write(json.dumps(row) + "\n")
                    stream.flush()
                    self.stop_event.wait(max(0, 0.2 - (time.monotonic() - started)))
        except (
            KeyError,
            OSError,
            RuntimeError,
            ValueError,
            http.client.HTTPException,
        ) as error:
            self.errors.append(f"{type(error).__name__}: {error}")
            self.stop_event.set()


def http_json(
    base_url: str, path: str, method: str = "GET", data=None, timeout: int = 120
):
    encoded = None if data is None else json.dumps(data, separators=(",", ":")).encode()
    request = Request(
        base_url + path,
        data=encoded,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    except HTTPError as error:
        detail = error.read(800).decode(errors="replace")
        raise RuntimeError(f"{method} {path}: HTTP {error.code}: {detail}") from None


def wait_api(base_url: str, path: str, timeout: int = 180):
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            return http_json(base_url, path, timeout=5)
        except (URLError, TimeoutError, RuntimeError) as error:
            last_error = str(error)
            time.sleep(2)
    raise RuntimeError(f"API readiness timed out for {path}: {last_error}")


def start_container(
    name: str, image: str, *, role: str, env: dict | None = None, extra=(), args=()
):
    memory, cpus = RESOURCES[role]
    command_args = [
        "run",
        "-d",
        "--name",
        name,
        "--network",
        NETWORK,
        "--memory",
        memory,
        "--cpus",
        cpus,
        "--label",
        "api-conf-e2e-owner=19e673db",
    ]
    for key, value in (env or {}).items():
        command_args.extend(["--env", f"{key}={value}"])
    command_args.extend(extra)
    command_args.append(image)
    command_args.extend(args)
    docker(*command_args)
    CREATED_CONTAINERS.append(name)


def prepare_sources():
    print("Preparing exact baseline and independent local patch", flush=True)
    baseline = Path("/snapshots/baseline")
    patched = Path("/snapshots/patched")
    command("git", "-C", str(baseline), "init", "-q")
    command(
        "git",
        "-C",
        str(baseline),
        "fetch",
        "--depth=1",
        "--no-tags",
        "https://github.com/apache/airflow.git",
        BASE,
        timeout=300,
    )
    command("git", "-C", str(baseline), "checkout", "--detach", "FETCH_HEAD")
    command("git", "clone", "--no-hardlinks", str(baseline), str(patched), timeout=180)
    command("git", "-C", str(patched), "apply", "--check", "/evidence/source.diff")
    command("git", "-C", str(patched), "apply", "/evidence/source.diff")
    for source in (baseline, patched):
        for directory in (
            "airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui/dist",
            "airflow-core/src/airflow/ui/dist",
        ):
            source.joinpath(directory).mkdir(parents=True, exist_ok=True)
    if command("git", "-C", str(baseline), "rev-parse", "HEAD") != BASE:
        raise RuntimeError("Baseline source changed unexpectedly")
    if command("git", "-C", str(patched), "rev-parse", "HEAD") != BASE:
        raise RuntimeError("Patched source has a different baseline")
    EVIDENCE.joinpath("source-manifest.json").write_text(
        json.dumps(
            {
                "baseline": BASE,
                "patch_sha256": hashlib.sha256(
                    EVIDENCE.joinpath("source.diff").read_bytes()
                ).hexdigest(),
                "image": IMAGE,
                "resources": RESOURCES,
                "source_mode": "Read-only Linux source volume; same pinned dependencies/image for every component.",
            },
            indent=2,
        )
    )


def stack_environment(names: dict[str, str]) -> dict[str, str]:
    return {
        "AIRFLOW_HOME": "/var/airflow",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": "/var/airflow/dags",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
        "AIRFLOW__CORE__PARALLELISM": "8",
        "AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS": "True",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "True",
        "AIRFLOW__CORE__FERNET_KEY": base64.urlsafe_b64encode(
            secrets.token_bytes(32)
        ).decode(),
        "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": f"http://{names['api']}:8080/execution/",
        "AIRFLOW__API__BASE_URL": f"http://{names['api']}:8080",
        "AIRFLOW__API_AUTH__JWT_SECRET": secrets.token_urlsafe(48),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": (
            f"postgresql+psycopg2://airflow:local-test-only@{names['postgres']}:5432/airflow"
        ),
        "AIRFLOW__CELERY__BROKER_URL": f"redis://{names['redis']}:6379/0",
        "AIRFLOW__CELERY__RESULT_BACKEND": (
            f"db+postgresql://airflow:local-test-only@{names['postgres']}:5432/airflow"
        ),
        "AIRFLOW__CELERY__WORKER_CONCURRENCY": "4",
        "AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "5",
        "AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL": "5",
        "AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES": "1",
        "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC": "1",
        "AIRFLOW__LOGGING__LOGGING_LEVEL": "INFO",
        "AIRFLOW__LOGGING__REMOTE_LOGGING": "False",
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "True",
    }


def prepare_arm(
    label: str, source_volume: str, arm_dir: Path, *, warm_execution_api: bool = False
):
    names = {role: f"{PREFIX}-{label}-{role}" for role in RESOURCES}
    state_volume = f"{PREFIX}-{label}-state"
    docker("volume", "create", state_volume)
    CREATED_VOLUMES.append(state_volume)
    start_container(
        names["postgres"],
        POSTGRES_IMAGE,
        role="postgres",
        env={
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "local-test-only",
            "POSTGRES_DB": "airflow",
        },
    )
    start_container(names["redis"], REDIS_IMAGE, role="redis")
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        ready = subprocess.run(
            [
                "docker",
                "exec",
                names["postgres"],
                "pg_isready",
                "-U",
                "airflow",
                "-d",
                "airflow",
            ],
            check=False,
            capture_output=True,
            timeout=5,
        )
        if ready.returncode == 0:
            break
        time.sleep(2)
    else:
        raise RuntimeError("Private PostgreSQL did not become ready")
    env = stack_environment(names)
    volume_args = (
        "--mount",
        f"type=volume,source={source_volume},target=/opt/airflow,readonly",
        "--mount",
        f"type=volume,source={state_volume},target=/var/airflow",
        "--workdir",
        "/opt/airflow",
        "--entrypoint",
        "/bin/bash",
    )
    init_name = f"{PREFIX}-{label}-init"
    init_command = "mkdir -p /var/airflow/dags /var/airflow/logs && airflow db migrate"
    start_container(
        init_name,
        IMAGE,
        role="scheduler",
        env=env,
        extra=volume_args,
        args=("-lc", init_command),
    )
    exit_code = docker("wait", init_name, timeout=180)
    arm_dir.joinpath("db-init.log").write_text(docker("logs", init_name))
    if exit_code != "0":
        raise RuntimeError(f"Database initialization failed: {exit_code}")
    docker(
        "cp",
        "/evidence/harness/conf_transport_probe.py",
        f"{init_name}:/var/airflow/dags/conf_transport_probe.py",
    )
    commands = {
        "api": "exec airflow api-server --host 0.0.0.0 --port 8080 --workers 1",
        "scheduler": "exec airflow scheduler",
        "dag-processor": "exec airflow dag-processor",
        "worker": "exec airflow celery worker -c 4 -q default --without-gossip --without-mingle -s",
        "triggerer": "exec airflow triggerer",
    }
    start_container(
        names["api"],
        IMAGE,
        role="api",
        env=env,
        extra=volume_args,
        args=("-lc", commands["api"]),
    )
    base_url = f"http://{names['api']}:8080"
    wait_api(base_url, "/api/v2/version")
    if warm_execution_api:
        health = http_json(base_url, "/execution/health")
        if health.get("status") != "healthy":
            raise RuntimeError("The Execution API warm-up did not report healthy")
    for role in ("dag-processor", "scheduler", "worker", "triggerer"):
        start_container(
            names[role],
            IMAGE,
            role=role,
            env=env,
            extra=volume_args,
            args=("-lc", commands[role]),
        )
    wait_api(base_url, f"/api/v2/dags/{DAG}", timeout=180)
    http_json(base_url, f"/api/v2/dags/{DAG}", "PATCH", {"is_paused": True})
    connection = psycopg2.connect(
        host=names["postgres"],
        dbname="airflow",
        user="airflow",
        password="local-test-only",
    )
    connection.autocommit = True
    arm_dir.joinpath("settings.json").write_text(
        json.dumps(
            {
                "label": label,
                "source_volume": source_volume,
                "resources": RESOURCES,
                "worker_concurrency": 4,
                "parallelism": 8,
                "api_workers": 1,
                "backend": "PostgreSQL 16 JSONB",
                "broker": "Redis 7",
                "network": "Private Docker bridge; no published host ports; test-only all-admin public API.",
                "execution_api": "Normal scoped workload/execution tokens; no execution authentication bypass.",
            },
            indent=2,
        )
    )
    return names, base_url, connection


def build_payload(records: int) -> dict:
    return {
        "records": [
            {
                "id": index,
                "data": {f"v{key}": f"{index:05d}-{key:02d}" for key in range(27)},
                "flags": [True, False, None],
                "values": [index, 1.25],
            }
            for index in range(records)
        ]
    }


def run_scenario(
    label,
    names,
    base_url,
    connection,
    arm_dir,
    scenario,
    records,
    cases,
    *,
    warm_execution_api=False,
):
    print(
        f"{label}: submitting {scenario}: {len(cases)} runs, {records} records/run",
        flush=True,
    )
    directory = arm_dir / scenario
    directory.mkdir()
    http_json(base_url, f"/api/v2/dags/{DAG}", "PATCH", {"is_paused": True})
    payload = build_payload(records)
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    expected = {}
    admission_started = time.monotonic()
    for index, case in enumerate(cases):
        run_id = f"e2e_{label}_{scenario}_{index:03d}"
        conf = {
            "payload": payload,
            "expected_sha256": digest,
            "case": case,
            "probe": "run",
            "hold_seconds": 2 if scenario != "contracts" else 0,
        }
        if case == "negative":
            conf["expected_sha256"] = "0" * 64
        if case == "empty":
            conf = {}
        if case == "false-param":
            conf["probe"] = False
        response = http_json(
            base_url,
            f"/api/v2/dags/{DAG}/dagRuns",
            "POST",
            {"dag_run_id": run_id, "logical_date": None, "conf": conf},
        )
        if response["dag_run_id"] != run_id:
            raise RuntimeError("Trigger response did not identify the submitted run")
        expected[run_id] = {
            "state": "failed" if case == "negative" else "success",
            "case": "success" if case == "empty" else case,
            "sha256": hashlib.sha256(b'{"records":[]}').hexdigest()
            if case == "empty"
            else digest,
            "record_count": 0 if case == "empty" else records,
            "params_probe": "default" if case == "empty" else conf["probe"],
        }
    admission_seconds = time.monotonic() - admission_started
    del response, conf, payload
    # Admission is unchanged by the patch; reset its retained allocator state before execution sampling.
    docker("restart", names["api"], timeout=60)
    wait_api(base_url, "/api/v2/version")
    if warm_execution_api:
        health = http_json(base_url, "/execution/health")
        if health.get("status") != "healthy":
            raise RuntimeError("The Execution API warm-up did not report healthy")
    sampler = Sampler(names, directory / "memory-samples.jsonl")
    sampler.start()
    time.sleep(3)
    started = time.monotonic()
    http_json(base_url, f"/api/v2/dags/{DAG}", "PATCH", {"is_paused": False})
    terminal = {}
    max_running = 0
    status_rows = []
    try:
        deadline = started + 360
        while time.monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT run_id, state, start_date, end_date FROM dag_run WHERE dag_id = %s AND run_id = ANY(%s)",
                    (DAG, list(expected)),
                )
                runs = cursor.fetchall()
                cursor.execute(
                    "SELECT state, count(*) FROM task_instance WHERE dag_id = %s AND run_id = ANY(%s) GROUP BY state",
                    (DAG, list(expected)),
                )
                task_counts = dict(cursor.fetchall())
            max_running = max(max_running, task_counts.get("running", 0))
            status_rows.append(
                {
                    "timestamp": now(),
                    "task_states": task_counts,
                    "runs": {run_id: state for run_id, state, _, _ in runs},
                }
            )
            terminal = {
                run_id: {"state": state, "start": str(begin), "end": str(end)}
                for run_id, state, begin, end in runs
                if state in ("success", "failed")
            }
            if len(terminal) == len(expected):
                break
            for name in names.values():
                state = engine_json(f"/containers/{name}/json")["State"]
                if not state["Running"]:
                    raise RuntimeError(
                        f"Component exited during the workload: {name}: {state}"
                    )
            time.sleep(1)
        else:
            raise RuntimeError(
                f"Workload timeout; only {len(terminal)}/{len(expected)} runs terminal"
            )
        execution_seconds = time.monotonic() - started
        time.sleep(3)
    finally:
        sampler.stop()
        directory.joinpath("run-status-samples.json").write_text(
            json.dumps(status_rows, indent=2)
        )
    mismatches = {
        run_id: {"expected": item["state"], "actual": terminal.get(run_id)}
        for run_id, item in expected.items()
        if terminal.get(run_id, {}).get("state") != item["state"]
    }
    if mismatches:
        raise RuntimeError(f"Unexpected run outcomes: {mismatches}")
    verified = {}
    for run_id, item in expected.items():
        if item["state"] == "failed":
            verified[run_id] = {"expected_failure": True, **terminal[run_id]}
            continue
        result = http_json(
            base_url,
            f"/api/v2/dags/{DAG}/dagRuns/{quote(run_id)}/taskInstances/validate_conf/xcomEntries/return_value"
            "?deserialize=true",
        )["value"]
        if isinstance(result, str):
            result = json.loads(result)
        for field in ("sha256", "record_count", "params_probe", "case"):
            if result[field] != item[field]:
                raise RuntimeError(f"Task result mismatch for {run_id}: {field}")
        if not result["mutable_identity"]:
            raise RuntimeError(
                "Task-visible mutable dictionary contract was not retained"
            )
        if item["case"] == "retry" and result["try_number"] < 2:
            raise RuntimeError("The retry case did not actually retry")
        verified[run_id] = {**terminal[run_id], "result": result}
    window = [row for row in sampler.rows if row["monotonic"] >= started]
    if not window:
        raise RuntimeError(
            "No complete simultaneous memory samples in execution window"
        )
    peak = max(window, key=lambda row: row["group_working_set_bytes"])
    idle = [
        row["group_working_set_bytes"]
        for row in sampler.rows
        if row["monotonic"] < started
    ]
    durations = [
        (
            datetime.fromisoformat(item["end"]) - datetime.fromisoformat(item["start"])
        ).total_seconds()
        for item in terminal.values()
        if item["start"] != "None" and item["end"] != "None"
    ]
    summary = {
        "scenario": scenario,
        "records_per_run": records,
        "submitted_runs": len(expected),
        "conf_payload_bytes": len(
            json.dumps(build_payload(records), separators=(",", ":")).encode()
        ),
        "admission_seconds": admission_seconds,
        "execution_seconds": execution_seconds,
        "runs_per_minute": len(expected) * 60 / execution_seconds,
        "dag_run_duration_p50_seconds": statistics.median(durations),
        "dag_run_duration_p95_seconds": (
            statistics.quantiles(durations, n=20)[18]
            if len(durations) >= 2
            else durations[0]
        ),
        "max_observed_running_tasks": max_running,
        "expected_failures": sum(
            item["state"] == "failed" for item in expected.values()
        ),
        "verified_terminal_runs": len(verified),
        "sample_count": len(window),
        "sample_collection_p95_ms": statistics.quantiles(
            [row["collection_duration_ms"] for row in window], n=20
        )[18]
        if len(window) >= 20
        else max(row["collection_duration_ms"] for row in window),
        "idle_group_working_set_mib": statistics.median(idle) / 1024**2,
        "group_peak_working_set_mib": peak["group_working_set_bytes"] / 1024**2,
        "group_peak_timestamp": peak["timestamp"],
        "components_at_group_peak_mib": {
            role: item["working_set_bytes"] / 1024**2
            for role, item in peak["components"].items()
        },
        "cpu_seconds_during_sampled_execution": {
            role: (
                window[-1]["components"][role]["cpu_total_ns"]
                - window[0]["components"][role]["cpu_total_ns"]
            )
            / 1e9
            for role in names
        },
        "component_individual_peaks_mib_not_to_be_summed": {
            role: max(row["components"][role]["working_set_bytes"] for row in window)
            / 1024**2
            for role in names
        },
        "group_memory_definition": (
            "Maximum over time of the sum of all seven component working sets in each complete sample. "
            "Working set = Docker cgroup memory usage minus inactive_file; not process RSS or PSS."
        ),
        "verified_runs": verified,
    }
    directory.joinpath("summary.json").write_text(json.dumps(summary, indent=2))
    print(
        f"{label} {scenario}: {len(verified)}/{len(expected)} verified; "
        f"group peak {summary['group_peak_working_set_mib']:.1f} MiB; "
        f"execution {execution_seconds:.1f}s",
        flush=True,
    )
    return summary


def capture_arm_logs(names, arm_dir):
    snapshots = {}
    for role, name in names.items():
        try:
            raw = subprocess.run(
                ["docker", "logs", "--timestamps", name],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            arm_dir.joinpath(f"{role}.log").write_text(raw.stdout + raw.stderr)
            state = engine_json(f"/containers/{name}/json")
            snapshots[role] = {
                "state": state["State"],
                "restart_count": state["RestartCount"],
                "image": state["Image"],
                "memory_limit": state["HostConfig"]["Memory"],
                "nano_cpus": state["HostConfig"]["NanoCpus"],
            }
        except (RuntimeError, subprocess.SubprocessError) as error:
            snapshots[role] = {"inspection_error": str(error)}
    arm_dir.joinpath("container-state.json").write_text(json.dumps(snapshots, indent=2))


def cleanup_containers():
    remaining = []
    for name in reversed(CREATED_CONTAINERS):
        result = subprocess.run(
            ["docker", "rm", "-f", "-v", name],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode:
            print(f"Container cleanup failed for {name}: {result.stderr}", flush=True)
            remaining.append(name)
    CREATED_CONTAINERS[:] = remaining


def main():
    EVIDENCE.mkdir(exist_ok=True)
    report = {
        "started": now(),
        "baseline": BASE,
        "source_patch": "source.diff",
        "design": "A1 baseline -> B patched -> A2 baseline rollback; fresh database/processes each arm.",
        "scope": "Real local API, scheduler, Dag processor, Celery worker/task, triggerer, PostgreSQL and Redis.",
        "arms": [],
        "status": "in_progress",
    }
    try:
        prepare_sources()
        for label, volume in (
            ("a1", BASE_VOLUME),
            ("b", PATCH_VOLUME),
            ("a2", BASE_VOLUME),
        ):
            arm_dir = EVIDENCE / label
            arm_dir.mkdir()
            print(f"Starting arm {label}", flush=True)
            names = {}
            connection = None
            try:
                names, base_url, connection = prepare_arm(label, volume, arm_dir)
                scenarios = []
                for scenario, records, cases in (
                    ("small", 20, ["success"] * 4),
                    ("medium", 2000, ["success"] * 8),
                    ("large", 23000, ["success"] * 20),
                    (
                        "contracts",
                        5,
                        ["empty", "false-param", "retry", "defer", "negative"],
                    ),
                ):
                    scenarios.append(
                        run_scenario(
                            label,
                            names,
                            base_url,
                            connection,
                            arm_dir,
                            scenario,
                            records,
                            cases,
                        )
                    )
                report["arms"].append(
                    {"label": label, "source": volume, "scenarios": scenarios}
                )
            finally:
                if connection is not None:
                    connection.close()
                if names:
                    capture_arm_logs(names, arm_dir)
                else:
                    inferred = {role: f"{PREFIX}-{label}-{role}" for role in RESOURCES}
                    capture_arm_logs(inferred, arm_dir)
                cleanup_containers()
            EVIDENCE.joinpath("e2e-results.json").write_text(
                json.dumps(report, indent=2)
            )
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        EVIDENCE.joinpath("failure-traceback.txt").write_text(traceback.format_exc())
        raise
    finally:
        report["finished"] = now()
        EVIDENCE.joinpath("e2e-results.json").write_text(json.dumps(report, indent=2))
        cleanup_containers()
        for volume in CREATED_VOLUMES:
            result = subprocess.run(
                ["docker", "volume", "rm", volume],
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode:
                print(
                    f"Volume cleanup failed for {volume}: {result.stderr}", flush=True
                )


if __name__ == "__main__":
    main()
