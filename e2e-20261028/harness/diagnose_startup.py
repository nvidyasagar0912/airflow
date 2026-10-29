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

"""Control experiment: unchanged baseline, cold versus health-warmed Execution API."""

from __future__ import annotations

import json
import re
import traceback
from pathlib import Path

import run_local_e2e as harness

ROOT = Path("/evidence/startup-diagnostic")


def main():
    ROOT.mkdir(exist_ok=True)
    report = {
        "started": harness.now(),
        "baseline": harness.BASE,
        "change": "Only add an Execution API health warm-up before submitting execution work.",
        "source_change": "None: both variants use the unpatched baseline.",
        "variants": [],
    }
    try:
        for label, warm in (("diagcold", False), ("diagwarm", True)):
            directory = ROOT / label
            directory.mkdir()
            names = {}
            connection = None
            try:
                names, base_url, connection = harness.prepare_arm(
                    label, harness.BASE_VOLUME, directory, warm_execution_api=warm
                )
                summary = harness.run_scenario(
                    label,
                    names,
                    base_url,
                    connection,
                    directory,
                    "small",
                    20,
                    ["success"] * 4,
                    warm_execution_api=warm,
                )
            finally:
                if connection is not None:
                    connection.close()
                if not names:
                    names = {
                        role: f"{harness.PREFIX}-{label}-{role}"
                        for role in harness.RESOURCES
                    }
                harness.capture_arm_logs(names, directory)
                harness.cleanup_containers()
            statuses = {}
            for line in directory.joinpath("api.log").read_text().splitlines():
                if "method=PATCH" not in line or not re.search(
                    r"path=/execution/task-instances/[^ ]+/run(?:\s|$)", line
                ):
                    continue
                if status := re.search(r"\bstatus_code=(\d+)", line):
                    statuses[status.group(1)] = statuses.get(status.group(1), 0) + 1
            report["variants"].append(
                {
                    "name": label,
                    "execution_api_health_warmed": warm,
                    "verified_runs": summary["verified_terminal_runs"],
                    "start_request_statuses": statuses,
                    "execution_seconds": summary["execution_seconds"],
                }
            )
        cold, warm = report["variants"]
        report["warmup_removed_observed_http400"] = (
            cold["start_request_statuses"].get("400", 0) > 0
            and warm["start_request_statuses"].get("400", 0) == 0
        )
        report["status"] = "completed"
        report["interpretation"] = (
            "This controls readiness only, not the memory patch. It does not replace the original A/B/A data "
            "or establish the exact transport failure mechanism."
        )
    except Exception as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        ROOT.joinpath("failure-traceback.txt").write_text(traceback.format_exc())
        raise
    finally:
        report["finished"] = harness.now()
        ROOT.joinpath("results.json").write_text(json.dumps(report, indent=2))
        harness.cleanup_containers()
        for volume in harness.CREATED_VOLUMES:
            harness.docker("volume", "rm", volume)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
