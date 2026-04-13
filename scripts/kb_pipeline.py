from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from dotenv import load_dotenv
from provider_fallback import ProviderClientFallback

try:
    from pymilvus import MilvusClient
except Exception:  # pragma: no cover
    MilvusClient = None  # type: ignore

try:
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover
    GraphDatabase = None  # type: ignore


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("kb_pipeline")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

STATE_SCHEMA = "mcp.ingestion.state.v1"
API_KEY_RE = re.compile(r"sk-[^\s'\"}]+")
SOURCE_EXTENSION_PRIORITY = {
    ".md": 4,
    ".markdown": 3,
    ".txt": 2,
    ".pdf": 1,
}
CHUNKING_STRATEGIES = ("semantic", "static")
DEFAULT_CHUNKING_STRATEGY = os.getenv("MCP_CHUNKING_STRATEGY", "semantic").strip().lower() or "semantic"
if DEFAULT_CHUNKING_STRATEGY not in CHUNKING_STRATEGIES:
    DEFAULT_CHUNKING_STRATEGY = "semantic"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sanitize_error_message(error: Any) -> str:
    return API_KEY_RE.sub("sk-***", str(error))


def source_priority(path_or_source: str) -> int:
    return SOURCE_EXTENSION_PRIORITY.get(Path(path_or_source).suffix.lower(), 0)


def _document_variant_key(source_rel_path: str) -> str:
    return Path(source_rel_path).with_suffix("").as_posix().lower()


def to_posix_rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return path.as_posix()


def load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "doc"


def normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\t+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ ]{2,}", " ", text)
    return text.strip()


def chunk_text(text: str, chunk_chars: int, overlap_chars: int) -> List[str]:
    if not text:
        return []

    chunks: List[str] = []
    i = 0
    n = len(text)

    while i < n:
        end = min(i + chunk_chars, n)
        if end < n:
            split = text.rfind(" ", i, end)
            if split > i + int(chunk_chars * 0.6):
                end = split

        chunk = text[i:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= n:
            break
        i = max(end - overlap_chars, i + 1)

    return chunks


def _split_sentences(paragraph: str) -> List[str]:
    parts = re.split(r"(?<=[.!?;:])\s+(?=[A-ZÀ-Ỹ0-9\-\"'])", paragraph)
    return [part.strip() for part in parts if part.strip()]


def _split_semantic_units(text: str, max_unit_chars: int) -> List[str]:
    units: List[str] = []
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", text) if part.strip()]
    if not paragraphs:
        return []

    for paragraph in paragraphs:
        if len(paragraph) <= max_unit_chars:
            units.append(paragraph)
            continue

        sentences = _split_sentences(paragraph)
        if not sentences:
            sentences = [paragraph]

        current_parts: List[str] = []
        current_len = 0
        for sentence in sentences:
            sentence_len = len(sentence)
            if current_parts and current_len + 1 + sentence_len > max_unit_chars:
                units.append(" ".join(current_parts).strip())
                current_parts = [sentence]
                current_len = sentence_len
            else:
                current_parts.append(sentence)
                current_len += sentence_len if current_len == 0 else sentence_len + 1

        if current_parts:
            units.append(" ".join(current_parts).strip())

    return [unit for unit in units if unit]


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0

    n = min(len(a), len(b))
    if n == 0:
        return 0.0

    dot = sum(a[i] * b[i] for i in range(n))
    norm_a = math.sqrt(sum(a[i] * a[i] for i in range(n)))
    norm_b = math.sqrt(sum(b[i] * b[i] for i in range(n)))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def chunk_text_semantic(
    text: str,
    chunk_chars: int,
    min_chunk_chars: int,
    max_unit_chars: int,
    similarity_threshold: float,
    embedding_provider: ProviderClientFallback,
) -> Tuple[List[str], Dict[str, Any]]:
    units = _split_semantic_units(text, max_unit_chars=max_unit_chars)
    if not units:
        return [], {"units": 0, "semantic_boundaries": 0}
    if len(units) == 1:
        return units, {"units": 1, "semantic_boundaries": 0}

    vectors, provider = embedding_provider.embed_texts(units)
    if len(vectors) != len(units):
        raise RuntimeError("Semantic chunking embedding response length mismatch")

    chunks: List[str] = []
    current_units: List[str] = []
    current_len = 0
    semantic_boundaries = 0

    for idx, unit in enumerate(units):
        unit_len = len(unit)
        should_break = False

        if current_units:
            next_len = current_len + unit_len + 2
            if next_len > chunk_chars and current_len >= min_chunk_chars:
                should_break = True
            else:
                similarity = _cosine_similarity(vectors[idx - 1], vectors[idx])
                if similarity < similarity_threshold and current_len >= min_chunk_chars:
                    should_break = True
                    semantic_boundaries += 1

        if should_break:
            chunk = "\n\n".join(current_units).strip()
            if chunk:
                chunks.append(chunk)
            current_units = [unit]
            current_len = unit_len
        else:
            current_units.append(unit)
            current_len = unit_len if current_len == 0 else current_len + unit_len + 2

    if current_units:
        chunk = "\n\n".join(current_units).strip()
        if chunk:
            chunks.append(chunk)

    return chunks, {
        "units": len(units),
        "semantic_boundaries": semantic_boundaries,
        "embedding_provider": provider,
    }


def chunk_document(
    text: str,
    strategy: str,
    chunk_chars: int,
    overlap_chars: int,
    semantic_threshold: float,
    semantic_min_chunk_chars: int,
    semantic_max_unit_chars: int,
    embedding_provider: Optional[ProviderClientFallback],
) -> Tuple[List[str], Dict[str, Any]]:
    requested = (strategy or DEFAULT_CHUNKING_STRATEGY).strip().lower()
    if requested not in CHUNKING_STRATEGIES:
        requested = DEFAULT_CHUNKING_STRATEGY

    if requested == "static":
        chunks = chunk_text(text, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
        return chunks, {
            "requested_strategy": requested,
            "effective_strategy": "static",
            "chunk_chars": chunk_chars,
            "overlap_chars": overlap_chars,
        }

    if embedding_provider is None:
        chunks = chunk_text(text, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
        return chunks, {
            "requested_strategy": requested,
            "effective_strategy": "static",
            "fallback_reason": "embedding provider unavailable",
            "chunk_chars": chunk_chars,
            "overlap_chars": overlap_chars,
        }

    try:
        chunks, semantic_meta = chunk_text_semantic(
            text=text,
            chunk_chars=chunk_chars,
            min_chunk_chars=max(120, semantic_min_chunk_chars),
            max_unit_chars=max(80, semantic_max_unit_chars),
            similarity_threshold=max(0.0, min(1.0, semantic_threshold)),
            embedding_provider=embedding_provider,
        )
        if not chunks:
            raise RuntimeError("Semantic chunking produced no output")
        return chunks, {
            "requested_strategy": requested,
            "effective_strategy": "semantic",
            "chunk_chars": chunk_chars,
            "semantic_threshold": semantic_threshold,
            "semantic_min_chunk_chars": semantic_min_chunk_chars,
            "semantic_max_unit_chars": semantic_max_unit_chars,
            **semantic_meta,
        }
    except Exception as exc:
        logger.warning("Semantic chunking failed; fallback to static chunking: %s", sanitize_error_message(exc))
        chunks = chunk_text(text, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
        return chunks, {
            "requested_strategy": requested,
            "effective_strategy": "static",
            "fallback_reason": sanitize_error_message(exc),
            "chunk_chars": chunk_chars,
            "overlap_chars": overlap_chars,
        }


def load_state(path: Path) -> Dict[str, Any]:
    state = load_json(
        path,
        {
            "schema": STATE_SCHEMA,
            "updated_at": None,
            "documents": {},
        },
    )
    if "documents" not in state or not isinstance(state["documents"], dict):
        state["documents"] = {}
    if "schema" not in state:
        state["schema"] = STATE_SCHEMA
    return state


def save_state(path: Path, state: Dict[str, Any]) -> None:
    state["updated_at"] = utc_now()
    save_json(path, state)


def iter_source_docs(docs_dir: Path, processed_dir: Path) -> Iterable[Path]:
    supported_ext = {".pdf", ".txt", ".md", ".markdown"}

    selected_by_base: Dict[str, Path] = {}

    for path in sorted(docs_dir.rglob("*")):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in supported_ext:
            continue
        if processed_dir in path.parents:
            continue
        try:
            rel_parts = path.relative_to(docs_dir).parts
            if rel_parts and rel_parts[0].lower() == "processed":
                continue
        except Exception:
            pass

        base_key = path.relative_to(docs_dir).with_suffix("").as_posix().lower()
        existing = selected_by_base.get(base_key)
        if existing is None:
            selected_by_base[base_key] = path
            continue

        existing_priority = source_priority(existing.as_posix())
        current_priority = source_priority(path.as_posix())
        if current_priority > existing_priority:
            selected_by_base[base_key] = path

    for key in sorted(selected_by_base.keys()):
        yield selected_by_base[key]


def purge_non_preferred_state_variants(
    state: Dict[str, Any],
    workspace_root: Path,
    docs_dir: Path,
    processed_dir: Path,
) -> List[str]:
    preferred_by_variant: Dict[str, str] = {}
    for source in iter_source_docs(docs_dir, processed_dir):
        source_rel = to_posix_rel(source, workspace_root)
        preferred_by_variant[_document_variant_key(source_rel)] = source_rel

    removed: List[str] = []
    for source_key in list((state.get("documents") or {}).keys()):
        variant_key = _document_variant_key(source_key)
        preferred_source = preferred_by_variant.get(variant_key)
        if preferred_source and preferred_source != source_key:
            state["documents"].pop(source_key, None)
            removed.append(source_key)

    return removed


def extract_pdf_text(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("pypdf is required for PDF text extraction") from exc

    reader = PdfReader(str(path))
    pages: List[str] = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return "\n\n".join(pages)


def extract_pdf_ocr(path: Path, language: str, max_pages: int = 0) -> str:
    try:
        import pypdfium2 as pdfium
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("pypdfium2 is required for OCR") from exc

    tesseract_fn = None
    rapid_ocr = None

    try:
        import pytesseract

        tesseract_fn = lambda image: pytesseract.image_to_string(image, lang=language)
    except Exception:
        tesseract_fn = None

    try:
        import numpy as np
        from rapidocr_onnxruntime import RapidOCR

        ocr_engine = RapidOCR()

        def _rapidocr_fn(image):
            result, _ = ocr_engine(np.asarray(image))
            if not result:
                return ""
            return "\n".join(item[1] for item in result if len(item) > 1)

        rapid_ocr = _rapidocr_fn
    except Exception:
        rapid_ocr = None

    if tesseract_fn is None and rapid_ocr is None:
        raise RuntimeError(
            "No OCR backend available. Install Tesseract or rapidocr-onnxruntime."
        )

    doc = pdfium.PdfDocument(str(path))
    limit = len(doc) if max_pages <= 0 else min(len(doc), max_pages)

    pages: List[str] = []
    for idx in range(limit):
        page = doc[idx]
        bitmap = page.render(scale=2)
        pil_image = bitmap.to_pil()
        text = ""
        if tesseract_fn is not None:
            try:
                text = tesseract_fn(pil_image)
            except Exception:
                text = ""

        if not text and rapid_ocr is not None:
            text = rapid_ocr(pil_image)

        pages.append(text)

    return "\n\n".join(pages)


def extract_document_text(path: Path, ocr_language: str, min_pdf_chars: int, enable_ocr: bool) -> Tuple[str, str, List[str]]:
    warnings: List[str] = []
    suffix = path.suffix.lower()

    if suffix == ".pdf":
        text = extract_pdf_text(path)
        mode = "pdf_text"

        if enable_ocr and len(normalize_whitespace(text)) < min_pdf_chars:
            try:
                ocr_text = extract_pdf_ocr(path, ocr_language)
                if normalize_whitespace(ocr_text):
                    text = ocr_text
                    mode = "pdf_ocr"
                else:
                    warnings.append("OCR returned empty content; fallback to extracted text.")
            except Exception as exc:
                warnings.append(f"OCR failed: {exc}")

        return normalize_whitespace(text), mode, warnings

    text = path.read_text(encoding="utf-8", errors="ignore")
    return normalize_whitespace(text), "text", warnings


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def process_documents(
    workspace_root: Path,
    docs_dir: Path,
    processed_dir: Path,
    state_path: Path,
    chunking_strategy: str,
    chunk_chars: int,
    overlap_chars: int,
    semantic_threshold: float,
    semantic_min_chunk_chars: int,
    semantic_max_unit_chars: int,
    enable_ocr: bool,
    ocr_language: str,
    min_pdf_chars: int,
    force: bool,
) -> Dict[str, Any]:
    state = load_state(state_path)
    removed_variants = purge_non_preferred_state_variants(
        state=state,
        workspace_root=workspace_root,
        docs_dir=docs_dir,
        processed_dir=processed_dir,
    )

    text_dir = processed_dir / "text"
    chunks_dir = processed_dir / "chunks"
    text_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "total": 0,
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "chunking_strategy": (chunking_strategy or DEFAULT_CHUNKING_STRATEGY).strip().lower(),
        "semantic_threshold": semantic_threshold,
        "semantic_min_chunk_chars": semantic_min_chunk_chars,
        "semantic_max_unit_chars": semantic_max_unit_chars,
        "removed_state_variants": removed_variants,
        "documents": [],
    }

    semantic_provider: Optional[ProviderClientFallback] = None
    if summary["chunking_strategy"] == "semantic":
        embedding_model = os.getenv("MCP_EMBEDDING_MODEL", "text-embedding-3-small").strip()
        try:
            semantic_provider = _ensure_embedding_provider(embedding_model)
        except Exception as exc:
            logger.warning(
                "Unable to initialize embedding provider for semantic chunking; static fallback will be used: %s",
                sanitize_error_message(exc),
            )

    for source in iter_source_docs(docs_dir, processed_dir):
        summary["total"] += 1
        source_rel = to_posix_rel(source, workspace_root)
        source_hash = sha256_file(source)
        source_key = source_rel

        existing = state["documents"].get(source_key) or {}
        existing_hash = existing.get("content_sha256")

        if not force and existing_hash == source_hash and (processed_dir / existing.get("outputs", {}).get("chunks_file", "")).exists():
            summary["skipped"] += 1
            summary["documents"].append({"source": source_rel, "status": "skipped"})
            continue

        rel_no_suffix = source.relative_to(docs_dir).with_suffix("").as_posix()
        doc_id = slugify(rel_no_suffix)

        text_path = text_dir / f"{doc_id}.txt"
        chunks_path = chunks_dir / f"{doc_id}.jsonl"

        try:
            text, mode, warnings = extract_document_text(
                source,
                ocr_language=ocr_language,
                min_pdf_chars=min_pdf_chars,
                enable_ocr=enable_ocr,
            )
            if not text:
                raise RuntimeError("No extractable text found")

            chunks, chunking_meta = chunk_document(
                text=text,
                strategy=summary["chunking_strategy"],
                chunk_chars=chunk_chars,
                overlap_chars=overlap_chars,
                semantic_threshold=semantic_threshold,
                semantic_min_chunk_chars=semantic_min_chunk_chars,
                semantic_max_unit_chars=semantic_max_unit_chars,
                embedding_provider=semantic_provider,
            )
            if not chunks:
                raise RuntimeError("No chunks generated")

            text_path.write_text(text + "\n", encoding="utf-8")

            chunk_rows: List[Dict[str, Any]] = []
            title = source.stem
            for idx, chunk in enumerate(chunks, 1):
                chunk_rows.append(
                    {
                        "doc_id": doc_id,
                        "article_id": f"{doc_id}:{idx}",
                        "title": title,
                        "doc_type": "legal_document",
                        "chunk_index": idx,
                        "text": chunk,
                        "source_path": source_rel,
                    }
                )

            write_jsonl(chunks_path, chunk_rows)

            state["documents"][source_key] = {
                "source_path": source_rel,
                "doc_id": doc_id,
                "content_sha256": source_hash,
                "status": "processed",
                "processed_at": utc_now(),
                "processor": {
                    "mode": mode,
                    "warnings": warnings,
                    "strategy": chunking_meta.get("effective_strategy"),
                    "requested_strategy": chunking_meta.get("requested_strategy"),
                    "chunk_chars": chunk_chars,
                    "overlap_chars": overlap_chars,
                    "semantic_threshold": chunking_meta.get("semantic_threshold"),
                    "semantic_min_chunk_chars": chunking_meta.get("semantic_min_chunk_chars"),
                    "semantic_max_unit_chars": chunking_meta.get("semantic_max_unit_chars"),
                    "semantic_units": chunking_meta.get("units"),
                    "semantic_boundaries": chunking_meta.get("semantic_boundaries"),
                    "embedding_provider": chunking_meta.get("embedding_provider"),
                    "fallback_reason": chunking_meta.get("fallback_reason"),
                },
                "outputs": {
                    "text_file": to_posix_rel(text_path, processed_dir),
                    "chunks_file": to_posix_rel(chunks_path, processed_dir),
                    "chunks_count": len(chunk_rows),
                    "text_length": len(text),
                },
                "last_imported_sha256": existing.get("last_imported_sha256"),
                "import": existing.get("import") or {},
                "last_error": None,
            }

            summary["processed"] += 1
            summary["documents"].append(
                {
                    "source": source_rel,
                    "status": "processed",
                    "doc_id": doc_id,
                    "chunks": len(chunk_rows),
                    "mode": mode,
                    "chunking_strategy": chunking_meta.get("effective_strategy"),
                    "requested_chunking_strategy": chunking_meta.get("requested_strategy"),
                    "chunking_fallback_reason": chunking_meta.get("fallback_reason"),
                }
            )
        except Exception as exc:
            error_text = sanitize_error_message(exc)
            state["documents"][source_key] = {
                "source_path": source_rel,
                "doc_id": existing.get("doc_id") or doc_id,
                "content_sha256": source_hash,
                "status": "process_failed",
                "processed_at": utc_now(),
                "outputs": existing.get("outputs") or {},
                "last_imported_sha256": existing.get("last_imported_sha256"),
                "import": existing.get("import") or {},
                "last_error": error_text,
            }
            summary["failed"] += 1
            summary["documents"].append({"source": source_rel, "status": "failed", "error": error_text})

    combined_rows: List[Dict[str, Any]] = []
    seen_chunk_files: set[str] = set()
    for entry in state["documents"].values():
        outputs = entry.get("outputs") or {}
        chunks_file_rel = outputs.get("chunks_file")
        if not chunks_file_rel:
            continue
        if chunks_file_rel in seen_chunk_files:
            continue
        seen_chunk_files.add(chunks_file_rel)
        chunks_file = processed_dir / chunks_file_rel
        if chunks_file.exists():
            combined_rows.extend(read_jsonl(chunks_file))

    combined_path = chunks_dir / "all_chunks.jsonl"
    write_jsonl(combined_path, combined_rows)

    save_state(state_path, state)
    summary["combined_chunks"] = len(combined_rows)
    summary["state_file"] = to_posix_rel(state_path, workspace_root)
    return summary


def _is_placeholder(value: str) -> bool:
    low = value.strip().lower()
    if not low:
        return True
    markers = [
        "replace_with",
        "your-",
        "change_me",
        "<",
        "example",
    ]
    return any(marker in low for marker in markers)


def _milvus_client_from_env() -> Any:
    if MilvusClient is None:
        raise RuntimeError("pymilvus is not installed")

    endpoint = os.getenv("MCP_MILVUS_ENDPOINT", "").strip()
    uri = (endpoint or os.getenv("MCP_MILVUS_URI", "").strip())
    token = os.getenv("MCP_MILVUS_TOKEN", "").strip()
    database = os.getenv("MCP_MILVUS_DATABASE", "").strip()

    if not uri:
        raise RuntimeError("Missing MCP_MILVUS_ENDPOINT or MCP_MILVUS_URI")

    kwargs: Dict[str, Any] = {"uri": uri}
    if token:
        kwargs["token"] = token
    if database:
        kwargs["db_name"] = database

    return MilvusClient(**kwargs)


def _neo4j_driver_from_env() -> Any:
    if GraphDatabase is None:
        raise RuntimeError("neo4j package is not installed")

    uri = os.getenv("MCP_NEO4J_URI", "").strip()
    user = os.getenv("MCP_NEO4J_USER", "").strip()
    password = os.getenv("MCP_NEO4J_PASSWORD", "").strip()

    if not uri or not user or not password:
        raise RuntimeError("Missing Neo4j env configuration")

    return GraphDatabase.driver(uri, auth=(user, password))


def _neo4j_session_kwargs() -> Dict[str, Any]:
    db = os.getenv("MCP_NEO4J_DATABASE", "").strip()
    return {"database": db} if db else {}


def _milvus_collection_info(client: Any, collection_name: str) -> Optional[Dict[str, Any]]:
    try:
        return client.describe_collection(collection_name=collection_name)
    except Exception:
        return None


def _detect_vector_field(info: Dict[str, Any], preferred: str) -> Tuple[str, Optional[int]]:
    fields = list(info.get("fields") or [])

    # Milvus FLOAT_VECTOR type code is commonly 101.
    vector_fields = [f for f in fields if int(f.get("type", -1)) == 101]
    if not vector_fields:
        return preferred, None

    selected = None
    for field in vector_fields:
        if field.get("name") == preferred:
            selected = field
            break
    if selected is None:
        selected = vector_fields[0]

    dim_raw = (selected.get("params") or {}).get("dim")
    dim = int(dim_raw) if dim_raw is not None else None
    return str(selected.get("name") or preferred), dim


def _detect_primary_field(info: Dict[str, Any]) -> Tuple[Optional[str], bool, Optional[int]]:
    fields = list(info.get("fields") or [])
    for field in fields:
        if field.get("is_primary"):
            return str(field.get("name")), bool(field.get("auto_id")), int(field.get("type", -1))
    return None, False, None


def _stable_int_id(value: str) -> int:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:15]
    return int(digest, 16)


def _coerce_vector_dimension(vector: List[float], target_dim: Optional[int]) -> List[float]:
    if target_dim is None:
        return vector
    if len(vector) == target_dim:
        return vector
    if len(vector) > target_dim:
        return vector[:target_dim]
    return vector + [0.0] * (target_dim - len(vector))


def _create_standard_collection(client: Any, collection_name: str, vector_field: str, dim: int) -> None:
    try:
        from pymilvus import DataType

        schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
        schema.add_field(field_name="article_id", datatype=DataType.VARCHAR, is_primary=True, max_length=512)
        schema.add_field(field_name="doc_id", datatype=DataType.VARCHAR, max_length=256)
        schema.add_field(field_name="title", datatype=DataType.VARCHAR, max_length=1024)
        schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=65535)
        schema.add_field(field_name="doc_type", datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name=vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name=vector_field,
            metric_type="COSINE",
            index_type="AUTOINDEX",
            index_name=f"idx_{vector_field}",
        )
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)
        return
    except Exception as exc:
        logger.warning("Failed to create rich Milvus schema, fallback to simple collection: %s", exc)

    try:
        client.create_collection(
            collection_name=collection_name,
            dimension=dim,
            metric_type="COSINE",
            vector_field_name=vector_field,
        )
    except TypeError:
        client.create_collection(collection_name=collection_name, dimension=dim, metric_type="COSINE")


def _delete_doc_from_milvus(client: Any, collection_name: str, doc_id: str) -> None:
    expr = f'doc_id == "{doc_id.replace("\\", "\\\\").replace("\"", "\\\"")}"'
    try:
        client.delete(collection_name=collection_name, filter=expr)
        return
    except TypeError:
        pass
    except Exception:
        pass

    try:
        client.delete(collection_name=collection_name, expr=expr)
    except Exception:
        # Deletion is optional; import can still proceed.
        pass


def _milvus_collection_exists(client: Any, collection_name: str) -> bool:
    try:
        collections = list(client.list_collections() or [])
        return collection_name in collections
    except Exception:
        return False


def _milvus_query_rows(client: Any, collection_name: str, expr: str, output_fields: List[str]) -> List[Dict[str, Any]]:
    safe_limit = 16384
    try:
        return list(
            client.query(
                collection_name=collection_name,
                filter=expr,
                output_fields=output_fields,
                limit=safe_limit,
            )
            or []
        )
    except TypeError:
        return list(
            client.query(
                collection_name=collection_name,
                expr=expr,
                output_fields=output_fields,
                limit=safe_limit,
            )
            or []
        )


def _milvus_doc_count(client: Any, collection_name: str, doc_id: str) -> int:
    safe_doc_id = doc_id.replace("\\", "\\\\").replace('"', '\\"')
    expr = f'doc_id == "{safe_doc_id}"'
    try:
        rows = _milvus_query_rows(client, collection_name, expr, output_fields=["doc_id"])
        return len(rows)
    except Exception:
        return 0


def _ensure_neo4j_constraints(session: Any) -> None:
    session.run(
        "CREATE CONSTRAINT document_doc_id IF NOT EXISTS FOR (d:Document) REQUIRE d.doc_id IS UNIQUE"
    )
    session.run(
        "CREATE CONSTRAINT chunk_article_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.article_id IS UNIQUE"
    )


def _neo4j_counts(session: Any) -> Dict[str, int]:
    docs = int((session.run("MATCH (d:Document) RETURN count(d) AS count").single() or {}).get("count") or 0)
    chunks = int((session.run("MATCH (c:Chunk) RETURN count(c) AS count").single() or {}).get("count") or 0)
    rels = int((session.run("MATCH ()-[r]->() RETURN count(r) AS count").single() or {}).get("count") or 0)
    nodes = int((session.run("MATCH (n) RETURN count(n) AS count").single() or {}).get("count") or 0)
    return {
        "documents": docs,
        "chunks": chunks,
        "relationships": rels,
        "nodes": nodes,
    }


def _ensure_embedding_provider(embedding_model: str) -> ProviderClientFallback:
    fallback = ProviderClientFallback(
        llm_model=os.getenv("MCP_LLM_MODEL", "gpt-4o-mini").strip(),
        embedding_model=embedding_model,
    )
    fallback.validate(require_generation=False, require_embeddings=True)
    return fallback


def reset_neo4j_graph(
    workspace_root: Path,
    state_path: Path,
    confirm: str,
    keep_import_state: bool,
) -> Dict[str, Any]:
    if str(confirm).strip().lower() != "yes":
        raise ValueError("Refusing to reset Neo4j. Re-run with --confirm yes")

    state = load_state(state_path)
    summary: Dict[str, Any] = {
        "confirmed": True,
        "keep_import_state": bool(keep_import_state),
        "neo4j": {
            "reachable": False,
            "before": {},
            "after": {},
        },
        "state_file": to_posix_rel(state_path, workspace_root),
        "state_updates": {
            "documents_touched": 0,
            "cleared_import_metadata": False,
        },
    }

    neo4j_driver = _neo4j_driver_from_env()
    try:
        with neo4j_driver.session(**_neo4j_session_kwargs()) as session:
            summary["neo4j"]["reachable"] = True
            summary["neo4j"]["before"] = _neo4j_counts(session)
            session.run("MATCH (n) DETACH DELETE n")
            _ensure_neo4j_constraints(session)
            summary["neo4j"]["after"] = _neo4j_counts(session)
    finally:
        neo4j_driver.close()

    if not keep_import_state:
        touched = 0
        for entry in (state.get("documents") or {}).values():
            entry["last_imported_sha256"] = None
            entry["import"] = {}
            if str(entry.get("status") or "").startswith("import"):
                entry["status"] = "processed"
            touched += 1
        if touched:
            save_state(state_path, state)
        summary["state_updates"] = {
            "documents_touched": touched,
            "cleared_import_metadata": True,
        }

    return summary


def validate_ingestion(
    workspace_root: Path,
    processed_dir: Path,
    state_path: Path,
    skip_milvus: bool,
    skip_neo4j: bool,
) -> Dict[str, Any]:
    state = load_state(state_path)
    docs = state.get("documents") or {}

    expected_counts: Dict[str, int] = {}
    for entry in docs.values():
        doc_id = str(entry.get("doc_id") or "").strip()
        chunks_count = int((entry.get("outputs") or {}).get("chunks_count") or 0)
        if not doc_id or chunks_count <= 0:
            continue
        expected_counts[doc_id] = chunks_count

    summary: Dict[str, Any] = {
        "state_file": to_posix_rel(state_path, workspace_root),
        "processed_dir": to_posix_rel(processed_dir, workspace_root),
        "expected": {
            "documents": len(expected_counts),
            "doc_ids": sorted(expected_counts.keys()),
            "chunks": int(sum(expected_counts.values())),
        },
        "milvus": {
            "attempted": not skip_milvus,
            "collection": os.getenv("MCP_MILVUS_COLLECTION", "legal_articles").strip(),
            "reachable": False,
            "collection_exists": False,
            "doc_counts": {},
            "error": None,
        },
        "neo4j": {
            "attempted": not skip_neo4j,
            "reachable": False,
            "counts": {},
            "doc_counts": {},
            "error": None,
        },
        "checks": {
            "doc_level": [],
            "all_doc_counts_match": False,
        },
        "success": False,
    }

    if not skip_milvus:
        try:
            milvus_client = _milvus_client_from_env()
            collection_name = summary["milvus"]["collection"]
            summary["milvus"]["reachable"] = True
            summary["milvus"]["collection_exists"] = _milvus_collection_exists(milvus_client, collection_name)
            if summary["milvus"]["collection_exists"]:
                for doc_id in expected_counts:
                    summary["milvus"]["doc_counts"][doc_id] = _milvus_doc_count(milvus_client, collection_name, doc_id)
        except Exception as exc:
            summary["milvus"]["error"] = sanitize_error_message(exc)

    if not skip_neo4j:
        neo4j_driver = None
        try:
            neo4j_driver = _neo4j_driver_from_env()
            with neo4j_driver.session(**_neo4j_session_kwargs()) as session:
                summary["neo4j"]["reachable"] = True
                summary["neo4j"]["counts"] = _neo4j_counts(session)
                if expected_counts:
                    rows = session.run(
                        """
                        UNWIND $doc_ids AS doc_id
                        OPTIONAL MATCH (c:Chunk {doc_id: doc_id})
                        RETURN doc_id, count(c) AS chunks
                        """,
                        {"doc_ids": list(expected_counts.keys())},
                    )
                    for row in rows:
                        summary["neo4j"]["doc_counts"][str(row["doc_id"])] = int(row["chunks"])
        except Exception as exc:
            summary["neo4j"]["error"] = sanitize_error_message(exc)
        finally:
            if neo4j_driver is not None:
                neo4j_driver.close()

    doc_checks: List[Dict[str, Any]] = []
    for doc_id, expected in sorted(expected_counts.items()):
        milvus_count = summary["milvus"]["doc_counts"].get(doc_id) if summary["milvus"]["attempted"] else None
        neo4j_count = summary["neo4j"]["doc_counts"].get(doc_id) if summary["neo4j"]["attempted"] else None
        matches = True
        if milvus_count is not None and milvus_count != expected:
            matches = False
        if neo4j_count is not None and neo4j_count != expected:
            matches = False
        doc_checks.append(
            {
                "doc_id": doc_id,
                "expected_chunks": expected,
                "milvus_chunks": milvus_count,
                "neo4j_chunks": neo4j_count,
                "match": matches,
            }
        )

    summary["checks"]["doc_level"] = doc_checks
    summary["checks"]["all_doc_counts_match"] = all(item.get("match") for item in doc_checks) if doc_checks else False

    store_checks = []
    if summary["milvus"]["attempted"]:
        store_checks.append(bool(summary["milvus"]["reachable"] and summary["milvus"]["collection_exists"]))
    if summary["neo4j"]["attempted"]:
        store_checks.append(bool(summary["neo4j"]["reachable"]))

    if not store_checks:
        summary["success"] = bool(summary["checks"]["all_doc_counts_match"])
    else:
        summary["success"] = all(store_checks) and bool(summary["checks"]["all_doc_counts_match"])
    return summary


def import_processed_documents(
    workspace_root: Path,
    processed_dir: Path,
    state_path: Path,
    force_reimport: bool,
    batch_size: int,
    skip_milvus: bool,
    skip_neo4j: bool,
) -> Dict[str, Any]:
    state = load_state(state_path)

    embedding_model = os.getenv("MCP_EMBEDDING_MODEL", "text-embedding-3-small").strip()
    preferred_vector_field = os.getenv("MCP_MILVUS_VECTOR_FIELD", "dense_vector").strip() or "dense_vector"
    configured_collection = os.getenv("MCP_MILVUS_COLLECTION", "legal_articles").strip()
    env_embedding_dim = os.getenv("MCP_EMBEDDING_DIMENSIONS", "").strip()
    configured_embedding_dim = int(env_embedding_dim) if env_embedding_dim.isdigit() else None
    embedding_provider = _ensure_embedding_provider(embedding_model)
    provider_status = embedding_provider.status()

    milvus_client = None
    neo4j_driver = None

    if not skip_milvus:
        milvus_client = _milvus_client_from_env()
    if not skip_neo4j:
        neo4j_driver = _neo4j_driver_from_env()

    try:
        collection_info = _milvus_collection_info(milvus_client, configured_collection) if milvus_client else None

        vector_field_name = preferred_vector_field
        collection_dim: Optional[int] = None
        primary_name: Optional[str] = None
        primary_auto = False
        primary_type: Optional[int] = None

        if collection_info:
            vector_field_name, collection_dim = _detect_vector_field(collection_info, preferred_vector_field)
            primary_name, primary_auto, primary_type = _detect_primary_field(collection_info)

        target_embedding_dim = configured_embedding_dim or collection_dim

        summary = {
            "total_candidates": 0,
            "imported": 0,
            "skipped": 0,
            "failed": 0,
            "documents": [],
            "milvus_collection": configured_collection,
            "milvus_vector_field": vector_field_name,
            "embedding_dimensions": target_embedding_dim,
            "available_embedding_providers": provider_status.get("available_embedding_providers") or [],
            "embedding_provider": None,
            "preflight": {
                "milvus_enabled": milvus_client is not None,
                "milvus_collection_exists": bool(collection_info) if milvus_client is not None else None,
                "neo4j_enabled": neo4j_driver is not None,
                "neo4j_counts_before": None,
                "configured_embedding_dim": configured_embedding_dim,
                "collection_dim": collection_dim,
            },
            "postflight": {
                "milvus_collection_exists": None,
                "neo4j_counts_after": None,
            },
        }

        if neo4j_driver is not None:
            with neo4j_driver.session(**_neo4j_session_kwargs()) as session:
                _ensure_neo4j_constraints(session)
                summary["preflight"]["neo4j_counts_before"] = _neo4j_counts(session)

        documents = list((state.get("documents") or {}).items())
        documents.sort(
            key=lambda item: (
                -source_priority(str(item[1].get("source_path") or item[0])),
                str(item[0]).lower(),
            )
        )
        imported_chunk_files: set[str] = set()
        for key, entry in documents:
            outputs = entry.get("outputs") or {}
            chunks_rel = outputs.get("chunks_file")
            if not chunks_rel:
                continue

            summary["total_candidates"] += 1

            if chunks_rel in imported_chunk_files:
                summary["skipped"] += 1
                entry["status"] = "import_skipped_duplicate"
                entry["last_error"] = None
                summary["documents"].append(
                    {
                        "source": key,
                        "status": "skipped_duplicate",
                        "reason": "chunks_file already imported",
                    }
                )
                continue
            imported_chunk_files.add(chunks_rel)

            content_hash = str(entry.get("content_sha256") or "")
            imported_hash = str(entry.get("last_imported_sha256") or "")

            if not force_reimport and content_hash and imported_hash == content_hash:
                summary["skipped"] += 1
                summary["documents"].append({"source": key, "status": "skipped"})
                continue

            chunks_path = processed_dir / chunks_rel
            if not chunks_path.exists():
                summary["failed"] += 1
                entry["status"] = "import_failed"
                entry["last_error"] = f"Missing chunks file: {chunks_rel}"
                summary["documents"].append({"source": key, "status": "failed", "error": entry["last_error"]})
                continue

            try:
                chunks = read_jsonl(chunks_path)
                if not chunks:
                    raise RuntimeError("No chunks found in processed file")

                texts = [str(chunk.get("text") or "") for chunk in chunks]
                vectors: List[List[float]] = []
                active_embedding_provider: Optional[str] = None
                for start in range(0, len(texts), max(1, batch_size)):
                    batch = texts[start : start + max(1, batch_size)]
                    batch_vectors, batch_provider = embedding_provider.embed_texts(
                        batch,
                        dimensions=target_embedding_dim,
                    )
                    active_embedding_provider = batch_provider
                    batch_vectors = [_coerce_vector_dimension(vector, target_embedding_dim) for vector in batch_vectors]
                    vectors.extend(batch_vectors)

                if not vectors:
                    raise RuntimeError("Embedding returned no vectors")

                # Create collection when missing, using embedding dimension resolved from first vector.
                if milvus_client is not None and collection_info is None:
                    vector_dim = len(vectors[0])
                    _create_standard_collection(
                        milvus_client,
                        collection_name=configured_collection,
                        vector_field=preferred_vector_field,
                        dim=vector_dim,
                    )
                    collection_info = _milvus_collection_info(milvus_client, configured_collection)
                    if collection_info:
                        vector_field_name, collection_dim = _detect_vector_field(collection_info, preferred_vector_field)
                        primary_name, primary_auto, primary_type = _detect_primary_field(collection_info)

                if collection_dim is not None and len(vectors[0]) != collection_dim:
                    vectors = [_coerce_vector_dimension(vector, collection_dim) for vector in vectors]

                if collection_dim is not None and len(vectors[0]) != collection_dim:
                    raise RuntimeError(
                        f"Embedding dimension {len(vectors[0])} does not match Milvus collection dimension {collection_dim}."
                    )

                doc_id = str(entry.get("doc_id") or chunks[0].get("doc_id") or "")
                if not doc_id:
                    raise RuntimeError("doc_id is missing")

                if milvus_client is not None:
                    _delete_doc_from_milvus(milvus_client, configured_collection, doc_id)

                    rows: List[Dict[str, Any]] = []
                    for chunk, vector in zip(chunks, vectors):
                        article_id = str(chunk.get("article_id") or "")
                        row: Dict[str, Any] = {
                            "article_id": article_id,
                            "doc_id": doc_id,
                            "title": str(chunk.get("title") or ""),
                            "text": str(chunk.get("text") or ""),
                            "doc_type": str(chunk.get("doc_type") or "legal_document"),
                            "chunk_index": int(chunk.get("chunk_index") or 0),
                            "source_path": str(chunk.get("source_path") or key),
                            vector_field_name: vector,
                        }

                        if primary_name and not primary_auto and primary_name not in row:
                            if primary_type == 5:
                                row[primary_name] = _stable_int_id(article_id)
                            else:
                                row[primary_name] = article_id

                        rows.append(row)

                    milvus_client.insert(collection_name=configured_collection, data=rows)

                if neo4j_driver is not None:
                    with neo4j_driver.session(**_neo4j_session_kwargs()) as session:
                        session.run(
                            """
                            MERGE (d:Document {doc_id: $doc_id})
                            SET d.title = $title,
                                d.source_path = $source_path,
                                d.updated_at = datetime()
                            """,
                            {
                                "doc_id": doc_id,
                                "title": str(chunks[0].get("title") or doc_id),
                                "source_path": str(chunks[0].get("source_path") or key),
                            },
                        )

                        session.run(
                            """
                            UNWIND $items AS item
                            MERGE (c:Chunk {article_id: item.article_id})
                            SET c.doc_id = item.doc_id,
                                c.title = item.title,
                                c.text = item.text,
                                c.doc_type = item.doc_type,
                                c.chunk_index = item.chunk_index,
                                c.updated_at = datetime()
                            WITH c, item
                            MATCH (d:Document {doc_id: item.doc_id})
                            MERGE (d)-[:HAS_CHUNK]->(c)
                            """,
                            {
                                "items": [
                                    {
                                        "article_id": str(chunk.get("article_id") or ""),
                                        "doc_id": str(chunk.get("doc_id") or doc_id),
                                        "title": str(chunk.get("title") or ""),
                                        "text": str(chunk.get("text") or ""),
                                        "doc_type": str(chunk.get("doc_type") or "legal_document"),
                                        "chunk_index": int(chunk.get("chunk_index") or 0),
                                    }
                                    for chunk in chunks
                                ]
                            },
                        )

                        if len(chunks) > 1:
                            links = []
                            for idx in range(len(chunks) - 1):
                                links.append(
                                    {
                                        "from": str(chunks[idx].get("article_id") or ""),
                                        "to": str(chunks[idx + 1].get("article_id") or ""),
                                    }
                                )
                            session.run(
                                """
                                UNWIND $links AS link
                                MATCH (a:Chunk {article_id: link.from})
                                MATCH (b:Chunk {article_id: link.to})
                                MERGE (a)-[:NEXT]->(b)
                                """,
                                {"links": links},
                            )

                entry["status"] = "imported"
                entry["last_imported_sha256"] = content_hash
                entry["import"] = {
                    "imported_at": utc_now(),
                    "embedding_provider": active_embedding_provider,
                    "milvus": {
                        "collection": configured_collection,
                        "vector_field": vector_field_name,
                        "chunks": len(chunks),
                    }
                    if milvus_client is not None
                    else {"skipped": True},
                    "neo4j": {
                        "database": os.getenv("MCP_NEO4J_DATABASE", "").strip() or None,
                        "chunks": len(chunks),
                    }
                    if neo4j_driver is not None
                    else {"skipped": True},
                }
                entry["last_error"] = None

                summary["imported"] += 1
                summary["embedding_provider"] = active_embedding_provider
                summary["documents"].append(
                    {
                        "source": key,
                        "status": "imported",
                        "doc_id": doc_id,
                        "chunks": len(chunks),
                        "embedding_provider": active_embedding_provider,
                    }
                )
            except Exception as exc:
                error_text = sanitize_error_message(exc)
                entry["status"] = "import_failed"
                entry["last_error"] = error_text
                summary["failed"] += 1
                summary["documents"].append({"source": key, "status": "failed", "error": error_text})

        if milvus_client is not None:
            summary["postflight"]["milvus_collection_exists"] = _milvus_collection_exists(
                milvus_client,
                configured_collection,
            )

        if neo4j_driver is not None:
            with neo4j_driver.session(**_neo4j_session_kwargs()) as session:
                summary["postflight"]["neo4j_counts_after"] = _neo4j_counts(session)

        save_state(state_path, state)
        summary["state_file"] = to_posix_rel(state_path, workspace_root)
        return summary
    finally:
        if neo4j_driver is not None:
            neo4j_driver.close()


def cleanup_sources(
    workspace_root: Path,
    state_path: Path,
    source_suffixes: List[str],
    skip_milvus: bool,
    skip_neo4j: bool,
) -> Dict[str, Any]:
    state = load_state(state_path)
    normalized_suffixes = {
        suffix.lower() if suffix.startswith(".") else f".{suffix.lower()}"
        for suffix in source_suffixes
    }

    targets: List[Tuple[str, Dict[str, Any]]] = []
    for source_key, entry in list((state.get("documents") or {}).items()):
        source_path = str(entry.get("source_path") or source_key).lower()
        if any(source_path.endswith(suffix) for suffix in normalized_suffixes):
            targets.append((source_key, entry))

    summary: Dict[str, Any] = {
        "source_suffixes": sorted(normalized_suffixes),
        "matched": len(targets),
        "removed_state_entries": [],
        "doc_ids": [],
        "milvus": {"attempted": False, "removed_doc_ids": [], "error": None},
        "neo4j": {"attempted": False, "removed_doc_ids": [], "error": None},
        "state_file": to_posix_rel(state_path, workspace_root),
    }

    if not targets:
        return summary

    doc_ids = sorted({str(entry.get("doc_id") or "").strip() for _, entry in targets if str(entry.get("doc_id") or "").strip()})
    summary["doc_ids"] = doc_ids

    if not skip_milvus and doc_ids:
        summary["milvus"]["attempted"] = True
        try:
            milvus_client = _milvus_client_from_env()
            collection_name = os.getenv("MCP_MILVUS_COLLECTION", "legal_articles").strip()
            for doc_id in doc_ids:
                _delete_doc_from_milvus(milvus_client, collection_name, doc_id)
                summary["milvus"]["removed_doc_ids"].append(doc_id)
        except Exception as exc:
            summary["milvus"]["error"] = sanitize_error_message(exc)

    if not skip_neo4j and doc_ids:
        summary["neo4j"]["attempted"] = True
        try:
            neo4j_driver = _neo4j_driver_from_env()
            try:
                with neo4j_driver.session(**_neo4j_session_kwargs()) as session:
                    session.run(
                        """
                        UNWIND $doc_ids AS doc_id
                        OPTIONAL MATCH (d:Document {doc_id: doc_id})
                        OPTIONAL MATCH (c:Chunk {doc_id: doc_id})
                        DETACH DELETE d, c
                        """,
                        {"doc_ids": doc_ids},
                    )
                    summary["neo4j"]["removed_doc_ids"] = list(doc_ids)
            finally:
                neo4j_driver.close()
        except Exception as exc:
            summary["neo4j"]["error"] = sanitize_error_message(exc)

    for source_key, _ in targets:
        state["documents"].pop(source_key, None)
        summary["removed_state_entries"].append(source_key)

    save_state(state_path, state)
    return summary


def run_server_then_pipeline(
    workspace_root: Path,
    server_script: str,
    startup_seconds: float,
    run_callable,
) -> Dict[str, Any]:
    cmd = [sys.executable, server_script]
    log_path = workspace_root / "docs" / "processed" / "pipeline_server.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("a", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(workspace_root),
            stdout=log_file,
            stderr=log_file,
            text=True,
        )

        try:
            time.sleep(max(0.5, startup_seconds))
            if proc.poll() is not None:
                raise RuntimeError(
                    f"Server exited before pipeline started. Check log: {log_path}"
                )

            result = run_callable()
            result["server"] = {
                "script": server_script,
                "pid": proc.pid,
                "log_file": to_posix_rel(log_path, workspace_root),
            }
            return result
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process legal docs and import them into Milvus + Neo4j with state tracking."
    )

    parser.add_argument("--workspace-root", default=".", help="Workspace root path.")
    parser.add_argument("--docs-dir", default="docs", help="Raw docs directory.")
    parser.add_argument("--processed-dir", default="docs/processed", help="Processed docs directory.")
    parser.add_argument(
        "--state-file",
        default="docs/processed/ingestion_state.json",
        help="State file path tracking processed/imported documents.",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    def _add_chunking_args(cmd: argparse.ArgumentParser) -> None:
        cmd.add_argument("--chunking-strategy", choices=list(CHUNKING_STRATEGIES), default=DEFAULT_CHUNKING_STRATEGY)
        cmd.add_argument("--chunk-chars", type=int, default=900)
        cmd.add_argument("--overlap-chars", type=int, default=120)
        cmd.add_argument("--semantic-threshold", type=float, default=0.72)
        cmd.add_argument("--semantic-min-chunk-chars", type=int, default=420)
        cmd.add_argument("--semantic-max-unit-chars", type=int, default=420)

    p_process = sub.add_parser("process", help="Process source docs into chunked data files.")
    _add_chunking_args(p_process)
    p_process.add_argument("--enable-ocr", action="store_true")
    p_process.add_argument("--ocr-language", default="vie+eng")
    p_process.add_argument("--min-pdf-chars", type=int, default=600)
    p_process.add_argument("--force", action="store_true", help="Re-process even if source hash unchanged.")

    p_import = sub.add_parser("import", help="Import processed chunks into Milvus and Neo4j.")
    p_import.add_argument("--force-reimport", action="store_true")
    p_import.add_argument("--batch-size", type=int, default=24)
    p_import.add_argument("--skip-milvus", action="store_true")
    p_import.add_argument("--skip-neo4j", action="store_true")

    p_cleanup = sub.add_parser(
        "cleanup",
        help="Remove state and store data for specific source suffixes (for example: .pdf).",
    )
    p_cleanup.add_argument(
        "--source-suffix",
        action="append",
        default=None,
        help="Source suffix to clean up. Can be repeated. Defaults to .pdf",
    )
    p_cleanup.add_argument("--skip-milvus", action="store_true")
    p_cleanup.add_argument("--skip-neo4j", action="store_true")

    p_reset = sub.add_parser(
        "reset",
        help="Reset Neo4j graph and recreate constraints. Requires explicit confirmation.",
    )
    p_reset.add_argument("--confirm", required=True, help="Type 'yes' to execute destructive reset.")
    p_reset.add_argument(
        "--keep-import-state",
        action="store_true",
        help="Keep import metadata in ingestion_state.json (default clears to force re-import).",
    )

    p_validate = sub.add_parser(
        "validate",
        help="Validate imported chunk parity across state, Milvus, and Neo4j.",
    )
    p_validate.add_argument("--skip-milvus", action="store_true")
    p_validate.add_argument("--skip-neo4j", action="store_true")

    p_run = sub.add_parser("run", help="Run process then import.")
    _add_chunking_args(p_run)
    p_run.add_argument("--enable-ocr", action="store_true")
    p_run.add_argument("--ocr-language", default="vie+eng")
    p_run.add_argument("--min-pdf-chars", type=int, default=600)
    p_run.add_argument("--force", action="store_true")
    p_run.add_argument("--force-reimport", action="store_true")
    p_run.add_argument("--batch-size", type=int, default=24)
    p_run.add_argument("--skip-milvus", action="store_true")
    p_run.add_argument("--skip-neo4j", action="store_true")

    p_server_run = sub.add_parser(
        "server-run",
        help="Start legal answer server, then run process+import pipeline, then stop server.",
    )
    p_server_run.add_argument("--server-script", default="legal_answer_server.py")
    p_server_run.add_argument("--startup-seconds", type=float, default=2.0)
    _add_chunking_args(p_server_run)
    p_server_run.add_argument("--enable-ocr", action="store_true")
    p_server_run.add_argument("--ocr-language", default="vie+eng")
    p_server_run.add_argument("--min-pdf-chars", type=int, default=600)
    p_server_run.add_argument("--force", action="store_true")
    p_server_run.add_argument("--force-reimport", action="store_true")
    p_server_run.add_argument("--batch-size", type=int, default=24)
    p_server_run.add_argument("--skip-milvus", action="store_true")
    p_server_run.add_argument("--skip-neo4j", action="store_true")

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    workspace_root = Path(args.workspace_root).resolve()
    docs_dir = (workspace_root / args.docs_dir).resolve()
    processed_dir = (workspace_root / args.processed_dir).resolve()
    state_path = (workspace_root / args.state_file).resolve()

    if not docs_dir.exists():
        raise FileNotFoundError(f"Docs directory not found: {docs_dir}")

    if args.command == "process":
        result = process_documents(
            workspace_root=workspace_root,
            docs_dir=docs_dir,
            processed_dir=processed_dir,
            state_path=state_path,
            chunking_strategy=str(args.chunking_strategy),
            chunk_chars=max(200, int(args.chunk_chars)),
            overlap_chars=max(0, int(args.overlap_chars)),
            semantic_threshold=max(0.0, min(float(args.semantic_threshold), 1.0)),
            semantic_min_chunk_chars=max(120, int(args.semantic_min_chunk_chars)),
            semantic_max_unit_chars=max(80, int(args.semantic_max_unit_chars)),
            enable_ocr=bool(args.enable_ocr),
            ocr_language=str(args.ocr_language),
            min_pdf_chars=max(1, int(args.min_pdf_chars)),
            force=bool(args.force),
        )
    elif args.command == "cleanup":
        suffixes = args.source_suffix or [".pdf"]
        result = cleanup_sources(
            workspace_root=workspace_root,
            state_path=state_path,
            source_suffixes=list(suffixes),
            skip_milvus=bool(args.skip_milvus),
            skip_neo4j=bool(args.skip_neo4j),
        )
    elif args.command == "reset":
        result = reset_neo4j_graph(
            workspace_root=workspace_root,
            state_path=state_path,
            confirm=str(args.confirm),
            keep_import_state=bool(args.keep_import_state),
        )
    elif args.command == "validate":
        result = validate_ingestion(
            workspace_root=workspace_root,
            processed_dir=processed_dir,
            state_path=state_path,
            skip_milvus=bool(args.skip_milvus),
            skip_neo4j=bool(args.skip_neo4j),
        )
    elif args.command == "import":
        result = import_processed_documents(
            workspace_root=workspace_root,
            processed_dir=processed_dir,
            state_path=state_path,
            force_reimport=bool(args.force_reimport),
            batch_size=max(1, int(args.batch_size)),
            skip_milvus=bool(args.skip_milvus),
            skip_neo4j=bool(args.skip_neo4j),
        )
    elif args.command == "run":
        process_result = process_documents(
            workspace_root=workspace_root,
            docs_dir=docs_dir,
            processed_dir=processed_dir,
            state_path=state_path,
            chunking_strategy=str(args.chunking_strategy),
            chunk_chars=max(200, int(args.chunk_chars)),
            overlap_chars=max(0, int(args.overlap_chars)),
            semantic_threshold=max(0.0, min(float(args.semantic_threshold), 1.0)),
            semantic_min_chunk_chars=max(120, int(args.semantic_min_chunk_chars)),
            semantic_max_unit_chars=max(80, int(args.semantic_max_unit_chars)),
            enable_ocr=bool(args.enable_ocr),
            ocr_language=str(args.ocr_language),
            min_pdf_chars=max(1, int(args.min_pdf_chars)),
            force=bool(args.force),
        )
        import_result = import_processed_documents(
            workspace_root=workspace_root,
            processed_dir=processed_dir,
            state_path=state_path,
            force_reimport=bool(args.force_reimport),
            batch_size=max(1, int(args.batch_size)),
            skip_milvus=bool(args.skip_milvus),
            skip_neo4j=bool(args.skip_neo4j),
        )
        result = {"process": process_result, "import": import_result}
    elif args.command == "server-run":
        def _run_combined() -> Dict[str, Any]:
            process_result = process_documents(
                workspace_root=workspace_root,
                docs_dir=docs_dir,
                processed_dir=processed_dir,
                state_path=state_path,
                chunking_strategy=str(args.chunking_strategy),
                chunk_chars=max(200, int(args.chunk_chars)),
                overlap_chars=max(0, int(args.overlap_chars)),
                semantic_threshold=max(0.0, min(float(args.semantic_threshold), 1.0)),
                semantic_min_chunk_chars=max(120, int(args.semantic_min_chunk_chars)),
                semantic_max_unit_chars=max(80, int(args.semantic_max_unit_chars)),
                enable_ocr=bool(args.enable_ocr),
                ocr_language=str(args.ocr_language),
                min_pdf_chars=max(1, int(args.min_pdf_chars)),
                force=bool(args.force),
            )
            import_result = import_processed_documents(
                workspace_root=workspace_root,
                processed_dir=processed_dir,
                state_path=state_path,
                force_reimport=bool(args.force_reimport),
                batch_size=max(1, int(args.batch_size)),
                skip_milvus=bool(args.skip_milvus),
                skip_neo4j=bool(args.skip_neo4j),
            )
            return {"process": process_result, "import": import_result}

        result = run_server_then_pipeline(
            workspace_root=workspace_root,
            server_script=str(args.server_script),
            startup_seconds=float(args.startup_seconds),
            run_callable=_run_combined,
        )
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
