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

"""Synthetic, independently verifiable task for real local API/Celery transport tests."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime, timedelta

from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.sdk import DAG, BaseOperator


class ConfTransportProbe(BaseOperator):
    def execute(self, context):
        conf = context["dag_run"].conf or {}
        case = conf.get("case", "success")
        if case == "retry" and context["ti"].try_number == 1:
            raise ValueError("Intentional first-attempt failure")
        if case == "defer":
            self.defer(
                trigger=DateTimeTrigger(
                    moment=datetime.now(UTC) + timedelta(seconds=2)
                ),
                method_name="execute_complete",
            )
        return self._validate(context)

    def execute_complete(self, context, event=None):
        return self._validate(context)

    def _validate(self, context):
        original_conf = context["dag_run"].conf
        if original_conf is not None and not isinstance(original_conf, dict):
            raise TypeError("Task-visible conf is not a dictionary or null")
        conf = original_conf or {}
        payload = conf.get("payload", {"records": []})
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        actual = hashlib.sha256(canonical).hexdigest()
        if actual != conf.get("expected_sha256", actual):
            raise ValueError("Intentional or unexpected configuration hash mismatch")
        expected_probe = conf.get("probe", "default")
        if context["params"].get("probe") != expected_probe:
            raise ValueError("Task parameters differ from the run configuration")
        if original_conf is not None:
            original_conf["mutation_probe"] = True
            if context["dag_run"].conf is not original_conf:
                raise ValueError("Task-visible configuration lost its mutable identity")
        time.sleep(conf.get("hold_seconds", 0))
        return {
            "sha256": actual,
            "record_count": len(payload["records"]),
            "conf_type": "null" if original_conf is None else "dict",
            "mutable_identity": True,
            "params_probe": expected_probe,
            "try_number": context["ti"].try_number,
            "case": conf.get("case", "success"),
        }


with DAG(
    dag_id="conf_transport_probe",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=24,
    max_active_tasks=4,
    params={"probe": "default"},
) as dag:
    ConfTransportProbe(
        task_id="validate_conf",
        retries=1,
        retry_delay=timedelta(seconds=1),
    )
