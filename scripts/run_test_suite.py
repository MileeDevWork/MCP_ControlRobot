from __future__ import annotations

import argparse
import json
import logging
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
GOOGLE_KEY_RE = re.compile(r"AIza[0-9A-Za-z\-_]{20,}")
QUERY_KEY_RE = re.compile(r"(?i)([?&](?:api_)?key=)[^&\s]+")
SCHEMA_ID = "mcp.eval.scenario.v1"
DEFAULT_SCENARIO_PATH = WORKSPACE_ROOT / "test" / "scenario.json"
DEFAULT_OUTPUT_DIR = WORKSPACE_ROOT / "test" / "reports"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("run_test_suite")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run legal-answer scenario tests and generate standardized reports."
    )
    parser.add_argument("--scenario", default=str(DEFAULT_SCENARIO_PATH), help="Scenario file path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Report output directory.")
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
    parser.add_argument(
        "--judge-mode",
        choices=["off", "all", "failed"],
        default="all",
        help="Hybrid evaluation mode: all=judge every successful case, failed=judge only static failures.",
    )
    parser.add_argument(
        "--judge-threshold",
        type=float,
        default=0.7,
        help="Judge pass threshold on score 0..1.",
    )
    parser.add_argument(
        "--judge-max-tokens",
        type=int,
        default=320,
        help="Max output tokens for judge model response.",
    )
    parser.add_argument(
        "--judge-model",
        default="",
        help="Optional override for judge model name.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sanitize_error_text(error: Any) -> str:
    text = API_KEY_RE.sub("sk-***", str(error))
    text = GOOGLE_KEY_RE.sub("AIza***", text)
    text = QUERY_KEY_RE.sub(r"\1***", text)
    return text


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def resolve_existing_path(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()

    from_cwd = (Path.cwd() / candidate).resolve()
    if from_cwd.exists():
        return from_cwd

    return (WORKSPACE_ROOT / candidate).resolve()


def resolve_output_dir(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (WORKSPACE_ROOT / candidate).resolve()


def normalize_for_match(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.replace("đ", "d").replace("Đ", "D")
    value = re.sub(r"[\"'`“”‘’.,;:!?()\[\]{}<>\-_/]+", " ", value)
    value = re.sub(r"\s+", " ", value.lower())
    return value.strip()


def _slug_from_text(value: str) -> str:
    slug = normalize_for_match(value)
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-") or "legacy-dataset"


def normalize_scenario_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if payload.get("schema") == SCHEMA_ID:
        return payload

    legacy_cases = payload.get("test_cases")
    if not isinstance(legacy_cases, list):
        return payload

    dataset_name = str(payload.get("dataset_name") or "Legacy scenario dataset").strip()
    normalized_cases: List[Dict[str, Any]] = []

    for idx, case in enumerate(legacy_cases, 1):
        if not isinstance(case, dict):
            continue

        case_id = str(case.get("id") or f"legacy-{idx}").strip()
        query = str(case.get("input_query") or "").strip()
        success_criteria = str(case.get("success_criteria") or "").strip()
        expected_contains = [success_criteria] if success_criteria else []

        normalized_cases.append(
            {
                "case_id": case_id,
                "input": {"query": query},
                "expected": {"contains": expected_contains},
                "category": str(case.get("test_category") or "legacy"),
                "description": str(case.get("description") or ""),
            }
        )

    logger.info("Detected legacy scenario format and converted it to %s.", SCHEMA_ID)
    return {
        "schema": SCHEMA_ID,
        "metadata": {
            "dataset_id": _slug_from_text(dataset_name),
            "dataset_name": dataset_name,
            "description": str(payload.get("description") or ""),
            "source_format": "legacy.test_cases.v0",
        },
        "cases": normalized_cases,
    }


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


def _extract_json_object(text: str) -> Dict[str, Any]:
    if not text:
        raise ValueError("empty judge response")

    text = text.strip()
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        raise ValueError("judge response does not contain a JSON object")

    payload = json.loads(match.group(0))
    if not isinstance(payload, dict):
        raise ValueError("judge JSON payload must be an object")
    return payload


def judge_case_with_llm(
    judge_client: Any,
    query: str,
    answer: str,
    expected_phrases: List[str],
    category: str,
    threshold: float,
    max_tokens: int,
) -> Dict[str, Any]:
    prompt = {
        "task": "Evaluate answer quality for Vietnamese legal QA.",
        "category": category,
        "query": query,
        "answer": answer,
        "expected_contains": expected_phrases,
        "instructions": [
            "Score factual alignment and coverage from 0.0 to 1.0.",
            "Use only provided query/answer/expected phrases.",
            "Return strict JSON only.",
        ],
        "response_schema": {
            "score": "float in [0,1]",
            "verdict": "pass|partial|fail",
            "reasoning": "short explanation",
            "matched_expected": ["string"],
            "missing_expected": ["string"],
        },
    }

    raw_text, provider = judge_client.generate_text(
        prompt=json.dumps(prompt, ensure_ascii=False),
        system="You are an impartial evaluator. Return only valid JSON.",
        temperature=0.0,
        max_tokens=max(64, int(max_tokens)),
    )
    parsed = _extract_json_object(raw_text)

    score_raw = parsed.get("score", 0.0)
    try:
        score = float(score_raw)
    except Exception:
        score = 0.0
    score = max(0.0, min(score, 1.0))

    verdict = str(parsed.get("verdict") or "").strip().lower()
    if verdict not in {"pass", "partial", "fail"}:
        verdict = "pass" if score >= threshold else "fail"

    matched = [str(item).strip() for item in (parsed.get("matched_expected") or []) if str(item).strip()]
    missing = [str(item).strip() for item in (parsed.get("missing_expected") or []) if str(item).strip()]
    reasoning = str(parsed.get("reasoning") or "").strip()

    pass_by_score = score >= threshold
    pass_by_verdict = verdict == "pass"
    judged_pass = pass_by_score or pass_by_verdict

    return {
        "enabled": True,
        "provider": provider,
        "score": round(score, 4),
        "threshold": threshold,
        "verdict": verdict,
        "pass": judged_pass,
        "reasoning": reasoning,
        "matched_expected": matched,
        "missing_expected": missing,
        "raw": _truncate_text(raw_text, max_chars=900),
        "error": None,
    }


def _truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max(0, max_chars - 3)] + "..."


def write_markdown_report(path: Path, report: Dict[str, Any]) -> None:
    summary = report.get("summary", {})
    judge_summary = report.get("llm_judge_summary") or {}
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

    if judge_summary:
        lines.append("## LLM Judge")
        lines.append("")
        lines.append(f"- mode: {judge_summary.get('mode')}")
        lines.append(f"- evaluated_cases: {judge_summary.get('evaluated_cases', 0)}")
        lines.append(f"- avg_score: {judge_summary.get('avg_score', 0.0):.4f}")
        lines.append(f"- pass_count: {judge_summary.get('pass_count', 0)}")
        lines.append(f"- fail_count: {judge_summary.get('fail_count', 0)}")
        lines.append(f"- disagreement_count: {judge_summary.get('disagreement_count', 0)}")
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

    scenario_path = resolve_existing_path(args.scenario)
    if not scenario_path.exists():
        raise FileNotFoundError(f"Scenario file not found: {scenario_path}")

    scenario = normalize_scenario_payload(load_json(scenario_path))
    output_dir = resolve_output_dir(args.output_dir)

    logger.info("Scenario file: %s", scenario_path)
    logger.info("Output directory: %s", output_dir)

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
            "output_dir": str(output_dir),
            "judge_mode": str(args.judge_mode).lower(),
            "judge_threshold": max(0.0, min(float(args.judge_threshold), 1.0)),
            "judge_max_tokens": max(64, int(args.judge_max_tokens)),
            "judge_model": str(args.judge_model or "").strip() or None,
            "decision_mode": "hybrid" if str(args.judge_mode).strip().lower() != "off" else "static",
        },
        "summary": {},
        "category_stats": {},
        "results": [],
    }

    latencies: List[float] = []
    answer_legal_question = None
    judge_client = None
    judge_mode = str(args.judge_mode).strip().lower()
    judge_threshold = max(0.0, min(float(args.judge_threshold), 1.0))
    judge_max_tokens = max(64, int(args.judge_max_tokens))

    if not args.dry_run:
        from legal_answer_server import answer_legal_question as _answer_legal_question, answer_service_healthcheck
        from provider_fallback import ProviderClientFallback

        answer_legal_question = _answer_legal_question

        try:
            health = answer_service_healthcheck()
            report["healthcheck"] = health
            logger.info(
                "answer_service_healthcheck success=%s missing_env=%s",
                health.get("success"),
                ",".join(health.get("missing_env") or []),
            )
        except Exception as exc:
            safe_error = sanitize_error_text(exc)
            report["healthcheck"] = {"success": False, "error": safe_error}
            logger.error("answer_service_healthcheck failed: %s", safe_error)

        if judge_mode != "off":
            try:
                judge_client = ProviderClientFallback(
                    llm_model=str(args.judge_model).strip() or None,
                )
                judge_client.validate(require_generation=True, require_embeddings=False)
                report["llm_judge"] = {
                    "enabled": True,
                    "mode": judge_mode,
                    "threshold": judge_threshold,
                    "max_tokens": judge_max_tokens,
                    "model_override": str(args.judge_model).strip() or None,
                    "status": judge_client.status(),
                }
            except Exception as exc:
                judge_mode = "off"
                report["llm_judge"] = {
                    "enabled": False,
                    "mode": "off",
                    "error": sanitize_error_text(exc),
                }
                logger.warning("LLM judge disabled: %s", sanitize_error_text(exc))

    for case in cases:
        case_id = str(case.get("case_id"))
        category = str(case.get("category") or "unknown")
        query = str((case.get("input") or {}).get("query") or "").strip()

        logger.info("Case %s started (category=%s)", case_id, category)

        status = "passed"
        status_static = "passed"
        answer = ""
        error = None
        expected_phrases: List[str] = []
        static_pass: Any = None
        missing_phrases: List[str] = []
        latency_ms = 0.0
        llm_judge: Dict[str, Any] = {"enabled": False}

        if args.dry_run:
            status = "skipped"
            status_static = "skipped"
        else:
            started = time.perf_counter()
            try:
                assert answer_legal_question is not None
                out = answer_legal_question(
                    question=query,
                    top_k=max(1, int(args.top_k)),
                    include_graph=bool(args.include_graph),
                    use_cache=bool(args.use_cache),
                )
                latency_ms = (time.perf_counter() - started) * 1000.0
                latencies.append(latency_ms)

                if not isinstance(out, dict):
                    status = "error"
                    status_static = "error"
                    error = f"unexpected tool response type: {type(out).__name__}"
                elif not out.get("success"):
                    status = "error"
                    status_static = "error"
                    error = sanitize_error_text(out.get("error") or "unknown error")
                else:
                    answer = str(out.get("answer") or "")
                    eval_result = evaluate_case(answer, case)
                    expected_phrases = eval_result.get("expected_contains") or []
                    missing_phrases = eval_result.get("missing_phrases") or []
                    static_pass = bool(eval_result.get("passed"))
                    status = "passed" if static_pass else "failed"
                    status_static = status

                    should_judge = bool(judge_client is not None and judge_mode in {"all", "failed"})
                    if should_judge and judge_mode == "failed" and static_pass:
                        should_judge = False

                    if should_judge:
                        try:
                            llm_judge = judge_case_with_llm(
                                judge_client=judge_client,
                                query=query,
                                answer=answer,
                                expected_phrases=expected_phrases,
                                category=category,
                                threshold=judge_threshold,
                                max_tokens=judge_max_tokens,
                            )
                        except Exception as exc:
                            llm_judge = {
                                "enabled": True,
                                "provider": None,
                                "score": None,
                                "threshold": judge_threshold,
                                "verdict": "error",
                                "pass": None,
                                "reasoning": "",
                                "matched_expected": [],
                                "missing_expected": [],
                                "raw": "",
                                "error": sanitize_error_text(exc),
                            }

                    if judge_mode != "off" and isinstance(llm_judge.get("pass"), bool):
                        status = "passed" if bool(llm_judge.get("pass")) else "failed"
            except Exception as exc:
                latency_ms = (time.perf_counter() - started) * 1000.0
                latencies.append(latency_ms)
                status = "error"
                status_static = "error"
                error = sanitize_error_text(exc)

        result_item = {
            "case_id": case_id,
            "category": category,
            "status": status,
            "status_static": status_static,
            "query": query,
            "answer": answer,
            "static_pass": static_pass,
            "expected_phrases": expected_phrases,
            "missing_phrases": missing_phrases,
            "llm_judge": llm_judge,
            "error": error,
            "latency_ms": round(latency_ms, 2),
        }
        report["results"].append(result_item)

        if status == "error":
            logger.error("Case %s -> error (latency_ms=%.2f): %s", case_id, latency_ms, error)
        elif status == "failed":
            logger.warning(
                "Case %s -> failed (latency_ms=%.2f), missing=%s",
                case_id,
                latency_ms,
                ", ".join(missing_phrases),
            )
        else:
            logger.info("Case %s -> %s (latency_ms=%.2f)", case_id, status, latency_ms)

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

    judged_results = [
        item
        for item in report["results"]
        if isinstance(item.get("llm_judge"), dict) and bool(item.get("llm_judge", {}).get("enabled"))
    ]
    judge_scores: List[float] = []
    judge_pass_count = 0
    judge_fail_count = 0
    judge_disagreement_count = 0
    provider_histogram: Dict[str, int] = {}

    for item in judged_results:
        judge = item.get("llm_judge") or {}
        provider = str(judge.get("provider") or "").strip() or "unknown"
        provider_histogram[provider] = provider_histogram.get(provider, 0) + 1

        score = judge.get("score")
        if isinstance(score, (int, float)):
            judge_scores.append(float(score))

        if judge.get("pass") is True:
            judge_pass_count += 1
        elif judge.get("pass") is False:
            judge_fail_count += 1

        if item.get("status") in {"passed", "failed"} and isinstance(judge.get("pass"), bool):
            static_result = item.get("status") == "passed"
            if bool(judge.get("pass")) != static_result:
                judge_disagreement_count += 1

    if judge_mode != "off" or report.get("llm_judge"):
        report["llm_judge_summary"] = {
            "mode": judge_mode,
            "evaluated_cases": len(judged_results),
            "avg_score": round(sum(judge_scores) / len(judge_scores), 4) if judge_scores else 0.0,
            "pass_count": judge_pass_count,
            "fail_count": judge_fail_count,
            "disagreement_count": judge_disagreement_count,
            "providers": provider_histogram,
        }

    report["finished_at"] = utc_now()

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
