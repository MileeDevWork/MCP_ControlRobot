from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

API_KEY_RE = re.compile(r"sk-[^\s'\"}]+")
SCHEMA_ID = "mcp.eval.scenario.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run legal-answer scenario tests and generate standardized reports."
    )
    parser.add_argument("--scenario", default="test/scenario.json", help="Scenario file path.")
    parser.add_argument("--output-dir", default="test/reports", help="Report output directory.")
    parser.add_argument("--top-k", type=int, default=4, help="Retriever top_k for each test case.")
    parser.add_argument(
        "--include-graph",
        action="store_true",
        help="Enable Neo4j expansion during answer generation.",
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Enable answer cache during test run.",
    )
    parser.add_argument("--max-cases", type=int, default=0, help="Limit number of test cases. 0 means all.")
    parser.add_argument("--dry-run", action="store_true", help="Validate dataset and emit report without calling the model.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop after first failed or errored case.")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sanitize_error_text(error: Any) -> str:
    return API_KEY_RE.sub("sk-***", str(error))


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def normalize_for_match(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"\s+", " ", value.lower())
    return value.strip()


def validate_standard_scenario(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if payload.get("schema") != SCHEMA_ID:
        errors.append(f"schema must be '{SCHEMA_ID}'")

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("metadata must be an object")

    cases = payload.get("cases")
    if not isinstance(cases, list):
        errors.append("cases must be an array")
        return errors

    seen_ids = set()
    for idx, case in enumerate(cases, 1):
        if not isinstance(case, dict):
            errors.append(f"cases[{idx}] must be an object")
            continue

        case_id = str(case.get("case_id") or "").strip()
        if not case_id:
            errors.append(f"cases[{idx}] missing case_id")
        elif case_id in seen_ids:
            errors.append(f"duplicate case_id: {case_id}")
        seen_ids.add(case_id)

        query = str((case.get("input") or {}).get("query") or "").strip()
        if not query:
            errors.append(f"cases[{idx}] missing input.query")

        expected = case.get("expected") or {}
        if not isinstance(expected, dict):
            errors.append(f"cases[{idx}] expected must be an object")
        elif not isinstance(expected.get("contains") or [], list):
            errors.append(f"cases[{idx}] expected.contains must be an array")

    return errors


def extract_expected_phrases(case: Dict[str, Any]) -> List[str]:
    expected = case.get("expected") or {}
    contains = expected.get("contains") or []
    return [str(item).strip() for item in contains if str(item).strip()]


def evaluate_case(answer: str, case: Dict[str, Any]) -> Dict[str, Any]:
    expected_phrases = extract_expected_phrases(case)
    normalized_answer = normalize_for_match(answer)

    missing: List[str] = []
    for phrase in expected_phrases:
        if normalize_for_match(phrase) not in normalized_answer:
            missing.append(phrase)

    passed = len(missing) == 0
    return {
        "passed": passed,
        "expected_contains": expected_phrases,
        "missing_phrases": missing,
    }


def write_markdown_report(path: Path, report: Dict[str, Any]) -> None:
    summary = report.get("summary", {})
    lines: List[str] = []
    lines.append("# Test Report")
    lines.append("")
    lines.append(f"- started_at: {report.get('started_at')}")
    lines.append(f"- finished_at: {report.get('finished_at')}")
    lines.append(f"- scenario: {report.get('scenario_path')}")
    lines.append(f"- schema: {report.get('scenario_schema')}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- total: {summary.get('total', 0)}")
    lines.append(f"- passed: {summary.get('passed', 0)}")
    lines.append(f"- failed: {summary.get('failed', 0)}")
    lines.append(f"- errors: {summary.get('errors', 0)}")
    lines.append(f"- pass_rate: {summary.get('pass_rate', 0.0):.2%}")
    lines.append(f"- avg_latency_ms: {summary.get('avg_latency_ms', 0.0):.2f}")
    lines.append("")

    category_stats = report.get("category_stats", {})
    if category_stats:
        lines.append("## Category Breakdown")
        lines.append("")
        lines.append("| category | passed | total | pass_rate |")
        lines.append("|---|---:|---:|---:|")
        for category, stats in sorted(category_stats.items()):
            total = int(stats.get("total", 0))
            passed = int(stats.get("passed", 0))
            rate = (passed / total) if total else 0.0
            lines.append(f"| {category} | {passed} | {total} | {rate:.2%} |")
        lines.append("")

    failures = [
        item
        for item in report.get("results", [])
        if item.get("status") in {"failed", "error"}
    ]
    if failures:
        lines.append("## Failed Cases")
        lines.append("")
        lines.append("| case_id | status | category | detail |")
        lines.append("|---|---|---|---|")
        for item in failures[:50]:
            detail = item.get("error") or ", ".join(item.get("missing_phrases") or [])
            detail = str(detail).replace("|", "\\|")
            lines.append(
                f"| {item.get('case_id')} | {item.get('status')} | {item.get('category')} | {detail} |"
            )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()

    scenario_path = Path(args.scenario)
    if not scenario_path.exists():
        raise FileNotFoundError(f"Scenario file not found: {scenario_path}")

    scenario = load_json(scenario_path)

    errors = validate_standard_scenario(scenario)
    if errors:
        raise ValueError("Scenario validation errors:\n- " + "\n- ".join(errors))

    cases = list(scenario.get("cases") or [])
    if args.max_cases and args.max_cases > 0:
        cases = cases[: args.max_cases]

    report: Dict[str, Any] = {
        "started_at": utc_now(),
        "finished_at": None,
        "scenario_path": str(scenario_path),
        "scenario_schema": str(scenario.get("schema")),
        "metadata": scenario.get("metadata") or {},
        "config": {
            "top_k": args.top_k,
            "include_graph": bool(args.include_graph),
            "use_cache": bool(args.use_cache),
            "dry_run": bool(args.dry_run),
            "case_count": len(cases),
        },
        "summary": {},
        "category_stats": {},
        "results": [],
    }

    latencies: List[float] = []

    if not args.dry_run:
        from legal_answer_server import answer_legal_question

    for case in cases:
        case_id = str(case.get("case_id"))
        category = str(case.get("category") or "unknown")
        query = str((case.get("input") or {}).get("query") or "").strip()

        status = "passed"
        answer = ""
        error = None
        missing_phrases: List[str] = []
        latency_ms = 0.0

        if args.dry_run:
            status = "skipped"
        else:
            started = time.perf_counter()
            try:
                out = answer_legal_question(
                    question=query,
                    top_k=max(1, int(args.top_k)),
                    include_graph=bool(args.include_graph),
                    use_cache=bool(args.use_cache),
                )
                latency_ms = (time.perf_counter() - started) * 1000.0
                latencies.append(latency_ms)

                if not out.get("success"):
                    status = "error"
                    error = sanitize_error_text(out.get("error") or "unknown error")
                else:
                    answer = str(out.get("answer") or "")
                    eval_result = evaluate_case(answer, case)
                    missing_phrases = eval_result.get("missing_phrases") or []
                    status = "passed" if eval_result.get("passed") else "failed"
            except Exception as exc:
                latency_ms = (time.perf_counter() - started) * 1000.0
                latencies.append(latency_ms)
                status = "error"
                error = sanitize_error_text(exc)

        result_item = {
            "case_id": case_id,
            "category": category,
            "status": status,
            "query": query,
            "answer": answer,
            "missing_phrases": missing_phrases,
            "error": error,
            "latency_ms": round(latency_ms, 2),
        }
        report["results"].append(result_item)

        stat = report["category_stats"].setdefault(category, {"total": 0, "passed": 0})
        stat["total"] += 1
        if status == "passed":
            stat["passed"] += 1

        if args.fail_fast and status in {"failed", "error"}:
            break

    total = len(report["results"])
    passed = sum(1 for item in report["results"] if item.get("status") == "passed")
    failed = sum(1 for item in report["results"] if item.get("status") == "failed")
    errors_count = sum(1 for item in report["results"] if item.get("status") == "error")
    pass_rate = (passed / total) if total else 0.0
    avg_latency_ms = (sum(latencies) / len(latencies)) if latencies else 0.0

    report["summary"] = {
        "total": total,
        "passed": passed,
        "failed": failed,
        "errors": errors_count,
        "pass_rate": round(pass_rate, 4),
        "avg_latency_ms": round(avg_latency_ms, 2),
    }
    report["finished_at"] = utc_now()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    json_path = output_dir / f"test-report-{ts}.json"
    md_path = output_dir / f"test-report-{ts}.md"

    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_markdown_report(md_path, report)

    print(f"Report JSON: {json_path}")
    print(f"Report Markdown: {md_path}")
    print(
        "Summary: "
        f"total={total}, passed={passed}, failed={failed}, errors={errors_count}, pass_rate={pass_rate:.2%}"
    )

    return 0 if errors_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
