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

"""Validate completed evidence and generate a review report without running a workload."""

from __future__ import annotations

import json
import math
import re
import statistics
from pathlib import Path

ROOT = Path("/evidence")


def plot_large_memory():
    traces = {}
    for label in ("a1", "b", "a2"):
        rows = [
            json.loads(line)
            for line in ROOT.joinpath(label, "large", "memory-samples.jsonl")
            .read_text()
            .splitlines()
        ]
        first = rows[0]["monotonic"]
        traces[label] = [
            (row["monotonic"] - first, row["group_working_set_bytes"] / 1024**2)
            for row in rows
        ]
    max_x = math.ceil(max(points[-1][0] for points in traces.values()) / 20) * 20
    max_y = (
        math.ceil(
            max(value for points in traces.values() for _, value in points) / 1000
        )
        * 1000
    )
    width, height, left, top = 960, 440, 85, 65
    plot_width, plot_height = 820, 300
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="sans-serif" font-size="13" fill="#111827">',
        '<text x="85" y="25" font-size="18">Large workload: simultaneous seven-component working set</text>',
    ]
    for value in range(0, max_y + 1, 1000):
        y = top + plot_height * (1 - value / max_y)
        svg.append(
            f'<path d="M {left} {y:.1f} H {left + plot_width}" stroke="#e5e7eb"/>'
        )
        svg.append(
            f'<text x="{left - 12}" y="{y + 4:.1f}" text-anchor="end">{value}</text>'
        )
    for value in range(0, max_x + 1, 20):
        x = left + plot_width * value / max_x
        svg.append(f'<text x="{x:.1f}" y="387" text-anchor="middle">{value}</text>')
    colors = {"a1": "#6b7280", "b": "#2563eb", "a2": "#16a34a"}
    for index, (label, points) in enumerate(traces.items()):
        rendered = " ".join(
            f"{left + plot_width * x / max_x:.1f},{top + plot_height * (1 - y / max_y):.1f}"
            for x, y in points
        )
        svg.append(
            f'<polyline points="{rendered}" fill="none" stroke="{colors[label]}" stroke-width="1.7"/>'
        )
        x = left + index * 190
        svg.append(
            f'<text x="{x}" y="48" fill="{colors[label]}">{label.upper()}: '
            f"{'patched' if label == 'b' else 'baseline'}</text>"
        )
    svg += [
        '<text x="15" y="225" transform="rotate(-90 15 225)">Working set, MiB</text>',
        '<text x="495" y="417" text-anchor="middle">Seconds from first sample (includes initial idle window)</text>',
        "</g></svg>",
    ]
    ROOT.joinpath("large-memory-timeseries.svg").write_text("\n".join(svg))


def main():
    raw = json.loads(ROOT.joinpath("e2e-results.json").read_text())
    if raw["status"] != "passed":
        raise RuntimeError("The integrated experiment has not passed")
    if [arm["label"] for arm in raw["arms"]] != ["a1", "b", "a2"]:
        raise RuntimeError("The baseline/patched/rollback comparison is incomplete")
    arms = {
        arm["label"]: {scenario["scenario"]: scenario for scenario in arm["scenarios"]}
        for arm in raw["arms"]
    }
    manifest = json.loads(ROOT.joinpath("source-manifest.json").read_text())
    total_runs = sum(
        scenario["verified_terminal_runs"]
        for arm in arms.values()
        for scenario in arm.values()
    )
    expected_failures = sum(
        scenario["expected_failures"]
        for arm in arms.values()
        for scenario in arm.values()
    )
    comparisons = {}
    for scenario in ("small", "medium", "large"):
        first, patched, rollback = (
            arms[label][scenario] for label in ("a1", "b", "a2")
        )
        for arm in (first, patched, rollback):
            if arm["submitted_runs"] != arm["verified_terminal_runs"]:
                raise RuntimeError("A submitted run was not verified")
            for run in arm["verified_runs"].values():
                if run["result"]["try_number"] != 1:
                    raise RuntimeError("A performance run required an unexpected retry")
        base_peak = statistics.mean(
            [
                first["group_peak_working_set_mib"],
                rollback["group_peak_working_set_mib"],
            ]
        )
        base_duration = statistics.mean(
            [first["execution_seconds"], rollback["execution_seconds"]]
        )
        comparisons[scenario] = {
            "baseline_a1_peak_mib": first["group_peak_working_set_mib"],
            "patched_peak_mib": patched["group_peak_working_set_mib"],
            "baseline_a2_peak_mib": rollback["group_peak_working_set_mib"],
            "baseline_mean_peak_mib": base_peak,
            "patched_peak_reduction_percent_vs_mean": (
                100 * (base_peak - patched["group_peak_working_set_mib"]) / base_peak
            ),
            "baseline_spread_percent": (
                100
                * abs(
                    first["group_peak_working_set_mib"]
                    - rollback["group_peak_working_set_mib"]
                )
                / base_peak
            ),
            "baseline_a1_seconds": first["execution_seconds"],
            "patched_seconds": patched["execution_seconds"],
            "baseline_a2_seconds": rollback["execution_seconds"],
            "patched_duration_reduction_percent_vs_mean": (
                100 * (base_duration - patched["execution_seconds"]) / base_duration
            ),
        }
    health = {}
    contracts = {}
    api_access = {}
    for label in arms:
        states = json.loads(ROOT.joinpath(label, "container-state.json").read_text())
        health[label] = {
            role: {
                "oom_killed": item["state"]["OOMKilled"],
                "restart_count": item["restart_count"],
                "running_at_end": item["state"]["Running"],
            }
            for role, item in states.items()
        }
        if any(
            item["oom_killed"] or item["restart_count"] or not item["running_at_end"]
            for item in health[label].values()
        ):
            raise RuntimeError(
                "A component exited, restarted unexpectedly or was OOM-killed"
            )
        samples = json.loads(
            ROOT.joinpath(label, "contracts", "run-status-samples.json").read_text()
        )
        contracts[label] = {
            "deferred_state_observed": any(
                item["task_states"].get("deferred", 0) > 0 for item in samples
            ),
            "runs": arms[label]["contracts"]["verified_runs"],
        }
        log = ROOT.joinpath(label, "api.log").read_text()
        latencies = []
        successful_latencies = []
        statuses = {}
        for line in log.splitlines():
            if "method=PATCH" not in line or not re.search(
                r"path=/execution/task-instances/[^ ]+/run(?:\s|$)", line
            ):
                continue
            duration = re.search(r"\bduration_us=(\d+)", line)
            status = re.search(r"\bstatus_code=(\d+)", line)
            if duration and status:
                latencies.append(int(duration.group(1)) / 1000)
                statuses[status.group(1)] = statuses.get(status.group(1), 0) + 1
                if status.group(1) == "200":
                    successful_latencies.append(int(duration.group(1)) / 1000)
        api_access[label] = {
            "note": "All scenarios combined; server access-log duration, not client end-to-end startup latency.",
            "requests": len(latencies),
            "statuses": statuses,
            "p50_ms": statistics.median(latencies) if latencies else None,
            "p95_ms": statistics.quantiles(latencies, n=20)[18]
            if len(latencies) > 1
            else None,
            "successful_response_p50_ms": statistics.median(successful_latencies)
            if successful_latencies
            else None,
            "successful_response_p95_ms": statistics.quantiles(
                successful_latencies, n=20
            )[18]
            if len(successful_latencies) > 1
            else None,
        }
    output = {
        "baseline": raw["baseline"],
        "patch_sha256": manifest["patch_sha256"],
        "verified_dag_runs": total_runs,
        "expected_negative_runs": expected_failures,
        "performance_comparisons": comparisons,
        "component_health": health,
        "contract_cases": contracts,
        "execution_api_access_log_durations": api_access,
        "qualification": (
            "Functional run outcomes passed. Recovered HTTP400 startup responses occurred in every arm; "
            "their root cause is not conclusively established. Performance/production conclusions remain qualified."
        ),
    }
    component_comparison = {}
    for role in arms["b"]["large"]["component_individual_peaks_mib_not_to_be_summed"]:
        baseline_mean = statistics.mean(
            arms[label]["large"]["component_individual_peaks_mib_not_to_be_summed"][
                role
            ]
            for label in ("a1", "a2")
        )
        candidate = arms["b"]["large"][
            "component_individual_peaks_mib_not_to_be_summed"
        ][role]
        component_comparison[role] = {
            "baseline_mean_individual_peak_mib": baseline_mean,
            "patched_individual_peak_mib": candidate,
            "reduction_percent": 100 * (baseline_mean - candidate) / baseline_mean,
        }
    output["individual_component_peaks_not_application_total"] = component_comparison
    diagnostic_path = ROOT / "startup-diagnostic" / "results.json"
    diagnostic = (
        json.loads(diagnostic_path.read_text()) if diagnostic_path.exists() else None
    )
    if diagnostic is not None:
        output["separate_startup_diagnostic"] = diagnostic
    ROOT.joinpath("comparison.json").write_text(json.dumps(output, indent=2))
    plot_large_memory()
    lines = [
        "# API / Task SDK configuration memory: local review evidence",
        "",
        f"Baseline: `{raw['baseline']}`.",
        f"Candidate: baseline plus [source.diff](source.diff), SHA256 `{manifest['patch_sha256']}`.",
        "",
        "**No PR, push, ADME deployment or scheduler optimization was performed.**",
        "",
        "## Executed tests",
        "",
        "- 736 focused/adjacent regression cases passed across SDK, SQLite, PostgreSQL and MySQL.",
        "- Official SDK and supervisor schema generation passed; targeted mypy and Ruff checks passed.",
        f"- {total_runs} real local DagRuns verified across baseline, patched and rollback arms.",
        f"- {expected_failures} intentionally negative runs ended failed as expected; they are not hidden successes.",
        "- Performance runs succeeded on the first attempt.",
        "- Retry, deferral/resume, empty configuration and false-valued parameters were exercised separately.",
        "- Functional run success does not mean every HTTP attempt succeeded; the recovered errors are listed below.",
        "",
        "## Main interpretation",
        "",
    ]
    large = comparisons["large"]
    if large["patched_peak_reduction_percent_vs_mean"] <= 0:
        lines += [
            "**This experiment did not demonstrate a full-stack peak-memory reduction.**",
            (
                f"The candidate peak was {large['patched_peak_mib']:.1f} MiB, versus baseline peaks of "
                f"{large['baseline_a1_peak_mib']:.1f} and {large['baseline_a2_peak_mib']:.1f} MiB."
            ),
            (
                f"It was {-large['patched_peak_reduction_percent_vs_mean']:.1f}% above the two-baseline mean "
                f"and inside the baseline range (spread {large['baseline_spread_percent']:.1f}%)."
            ),
            "Do not replace this finding with the much larger isolated-phase allocation reductions.",
        ]
    else:
        lines += [
            (
                f"The candidate large-workload group peak was "
                f"{large['patched_peak_reduction_percent_vs_mean']:.1f}% lower than the baseline mean."
            ),
            (
                f"The two baselines differed by {large['baseline_spread_percent']:.1f}%; "
                "one candidate observation does not establish a production guarantee."
            ),
        ]
    lines += [
        "",
        (
            f"Large-workload completion time was {large['patched_seconds']:.1f}s in the candidate "
            f"versus {large['baseline_a1_seconds']:.1f}s and {large['baseline_a2_seconds']:.1f}s in the baselines "
            f"({large['patched_duration_reduction_percent_vs_mean']:.1f}% lower than their mean)."
        ),
        "Component-specific results below must not be presented as the full-stack total.",
        "",
        "## Time-aligned application working-set peaks",
        "",
        "All values are MiB. These peaks are measured totals of seven component working sets at each sample,",
        "**not sums of independent component peaks**. See [the protocol](TEST-PROTOCOL.md) for the accounting.",
        "",
        "| Payload | A1 baseline | B patched | A2 baseline | B reduction vs baseline mean | Baseline spread |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, item in comparisons.items():
        lines.append(
            f"| {name} | {item['baseline_a1_peak_mib']:.1f} | {item['patched_peak_mib']:.1f} | "
            f"{item['baseline_a2_peak_mib']:.1f} | "
            f"{item['patched_peak_reduction_percent_vs_mean']:.1f}% | "
            f"{item['baseline_spread_percent']:.1f}% |"
        )
    lines += [
        "",
        "![Time-aligned seven-component working set](large-memory-timeseries.svg)",
        "",
        "## Workload completion time",
        "",
        "Seconds from unpausing admitted runs to all declared terminal states. Admission and result",
        "verification are recorded separately and excluded from this interval.",
        "",
        "| Payload | A1 baseline | B patched | A2 baseline | B reduction vs baseline mean |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name, item in comparisons.items():
        lines.append(
            f"| {name} | {item['baseline_a1_seconds']:.1f} | {item['patched_seconds']:.1f} | "
            f"{item['baseline_a2_seconds']:.1f} | "
            f"{item['patched_duration_reduction_percent_vs_mean']:.1f}% |"
        )
    lines += [
        "",
        "## Large-payload composition at the actual group-peak sample",
        "",
        "| Component | A1 | B | A2 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for role in arms["a1"]["large"]["components_at_group_peak_mib"]:
        values = [
            arms[label]["large"]["components_at_group_peak_mib"][role] for label in arms
        ]
        lines.append(
            f"| {role} | " + " | ".join(f"{value:.1f}" for value in values) + " |"
        )
    lines += [
        "",
        "## Individual component peaks, not an application total",
        "",
        "Each component's peak can occur at a different time. This table is diagnostic only.",
        "",
        "| Component | Baseline mean own peak, MiB | Patched own peak, MiB | Reduction |",
        "| --- | ---: | ---: | ---: |",
    ]
    for role, item in component_comparison.items():
        lines.append(
            f"| {role} | {item['baseline_mean_individual_peak_mib']:.1f} | "
            f"{item['patched_individual_peak_mib']:.1f} | {item['reduction_percent']:.1f}% |"
        )
    lines += [
        "",
        "## Execution API access-log duration across the full workload mix",
        "",
        "Server-side request durations are not client end-to-end startup latency.",
        "",
        "| Arm | Task-start requests | HTTP statuses | P50, ms | P95, ms |",
        "| --- | ---: | --- | ---: | ---: |",
    ]
    for label, item in api_access.items():
        if item["p50_ms"] is not None and item["p95_ms"] is not None:
            lines.append(
                f"| {label} | {item['requests']} | {item['statuses']} | "
                f"{item['p50_ms']:.1f} | {item['p95_ms']:.1f} |"
            )
    lines += [
        "",
        "**Qualification:** 16 HTTP 400 task-start responses occurred in each arm, followed by successful",
        "recovery without unexpected performance-task retries. They cluster in the first worker batch after",
        "the intentional API restarts. Cold Execution API initialization versus the default five-second SDK",
        "timeout is a hypothesis consistent with the timing, not a conclusively reproduced root cause.",
        "The raw request and worker retry logs are retained. Resolve or isolate this effect before claiming",
        "error-free startup behavior or a clean steady-state request-latency improvement.",
        "",
        "## Successful task-start responses only",
        "",
        "| Arm | P50, ms | P95, ms |",
        "| --- | ---: | ---: |",
    ]
    for label, item in api_access.items():
        if item["successful_response_p50_ms"] is not None:
            lines.append(
                f"| {label} | {item['successful_response_p50_ms']:.1f} | "
                f"{item['successful_response_p95_ms']:.1f} |"
            )
    if diagnostic is not None:
        lines += [
            "",
            "## Separate unchanged-baseline startup control",
            "",
            "This is not another memory-patch result and does not replace the original A/B/A observations.",
            "",
            "| Readiness before execution | Verified DagRuns | Task-start HTTP statuses | Completion, s |",
            "| --- | ---: | --- | ---: |",
        ]
        for variant in diagnostic["variants"]:
            lines.append(
                f"| {variant['name']} | {variant['verified_runs']} | {variant['start_request_statuses']} | "
                f"{variant['execution_seconds']:.1f} |"
            )
        if diagnostic.get("warmup_removed_observed_http400"):
            lines += [
                "",
                "Warming `/execution/health` on the same unpatched baseline removed the observed HTTP 400s",
                "in this small control (4 HTTP 400s cold, zero warm). This supports a readiness/cold-start",
                "explanation rather than a new memory-patch regression. The precise transport failure",
                "mechanism is not established, and the full large A/B/A was not rerun with this warm-up.",
            ]
        lines += [
            "",
            "[Raw startup-control results](startup-diagnostic/results.json).",
            "An earlier OpenAPI warm-up attempt failed on the unchanged baseline with a response-length",
            "error; its logs and four completed cold-control runs are retained in",
            "`startup-diagnostic/attempt-openapi/`, outside the completed comparison counts.",
        ]
    lines += [
        "",
        "## Regressions, trade-offs and limits",
        "",
        "- The asset-event collection read adds one database round trip to avoid repeating the large JSON text.",
        "- Native JSON type checking adds database work but rejects invalid roots before RUNNING is committed.",
        "- The prior isolated task-phase benchmark retained a small allocation increase; it is not concealed.",
        "- Two baseline observations and one candidate observation are not a statistical guarantee.",
        "- Report small-input overhead and baseline variability along with large-input improvements.",
        "- Local cgroup working sets are not production pod RSS, PSS or whole-host usage.",
        "- The worker executes up to four tasks concurrently; the large scenario admits 20 DagRuns.",
        "- The API is intentionally restarted after admission in every scenario to isolate the changed execution path.",
        "- This synthetic workload has no external OSDU services or ingestion/search correctness claim.",
        "- A pre-workload read-only-UI-directory harness failure is preserved in `setup-attempts/`.",
        "- A second UI directory was provisioned during baseline setup; all measured windows used fresh API processes.",
        "- Mixed unreleased core/SDK builds, AKS/Helm rollout and production behavior remain outside this evidence.",
        "",
        "## Review and reproduction",
        "",
        "- [Protocol, configurations and reproduction steps](TEST-PROTOCOL.md)",
        "- [Exact source patch](source.diff)",
        "- [Pinned source/image/resource manifest](source-manifest.json)",
        "- [Machine-readable comparison](comparison.json)",
        "- [Complete verified runs, timings and per-scenario summaries](e2e-results.json)",
        "- Per-arm directories preserve raw memory samples, status samples and service logs.",
        "- [Prior regression-test reports](local-regression-results/validation-summary.json)",
        "",
        "The source remains local and uncommitted. This document is evidence for review, not production approval.",
    ]
    ROOT.joinpath("REVIEW-EVIDENCE.md").write_text("\n".join(lines) + "\n")
    print(
        json.dumps(
            {"verified_dag_runs": total_runs, "comparisons": comparisons}, indent=2
        )
    )


if __name__ == "__main__":
    main()
