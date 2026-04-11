from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from fastmcp import FastMCP

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore

try:
    from pymilvus import MilvusClient as PyMilvusClient
except Exception:  # pragma: no cover
    PyMilvusClient = None  # type: ignore

try:
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover
    GraphDatabase = None  # type: ignore


if sys.platform == "win32":
    sys.stderr.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("LegalAnswerServer")

mcp = FastMCP("LegalAnswerServer")

DEFAULT_TOP_K = max(1, int(os.getenv("MCP_TOP_K", "4")))
MAX_TOP_K = max(DEFAULT_TOP_K, int(os.getenv("MCP_MAX_TOP_K", "8")))
CACHE_TTL_SECONDS = max(0, int(os.getenv("MCP_CACHE_TTL_SECONDS", "300")))
CONTEXT_CHAR_BUDGET = max(1000, int(os.getenv("MCP_CONTEXT_CHAR_BUDGET", "2800")))
MAX_CONTEXT_ITEMS = max(1, int(os.getenv("MCP_MAX_CONTEXT_ITEMS", "8")))
MAX_ITEM_CHARS = max(120, int(os.getenv("MCP_MAX_ITEM_CHARS", "700")))


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def _normalize_query(query: str) -> str:
    compact = " ".join(query.strip().split())
    compact = re.sub(r"\s+([,.;:!?])", r"\1", compact)
    return compact


class HybridAnswerRuntime:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready = False

        self._openai_client = None
        self._milvus_client = None
        self._neo4j_driver = None

        self._llm_model = os.getenv("MCP_LLM_MODEL", "gpt-4o-mini")
        self._embedding_model = os.getenv("MCP_EMBEDDING_MODEL", "text-embedding-3-small")
        self._milvus_uri = os.getenv("MCP_MILVUS_URI", "http://localhost:19530")
        self._milvus_collection = os.getenv("MCP_MILVUS_COLLECTION", "legal_articles")

        self._answer_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    def _cache_key(self, query: str, top_k: int, include_graph: bool) -> str:
        return f"{top_k}:{int(include_graph)}:{query.lower()}"

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        if CACHE_TTL_SECONDS <= 0:
            return None
        item = self._answer_cache.get(key)
        if item is None:
            return None
        ts, value = item
        if time.time() - ts > CACHE_TTL_SECONDS:
            self._answer_cache.pop(key, None)
            return None
        return value

    def _cache_set(self, key: str, value: Dict[str, Any]) -> None:
        if CACHE_TTL_SECONDS <= 0:
            return
        self._answer_cache[key] = (time.time(), value)

    def ensure_ready(self) -> None:
        if self._ready:
            return

        with self._lock:
            if self._ready:
                return

            api_key = os.getenv("MCP_OPENAI_API_KEY")
            if not api_key:
                raise RuntimeError("Missing MCP_OPENAI_API_KEY. Use a dedicated key for MCP repo.")
            if OpenAI is None:
                raise RuntimeError("openai package is not installed.")

            self._openai_client = OpenAI(api_key=api_key)

            if PyMilvusClient is not None:
                self._milvus_client = PyMilvusClient(uri=self._milvus_uri)
            else:
                logger.warning("pymilvus not installed, vector retrieval disabled")

            neo4j_uri = os.getenv("MCP_NEO4J_URI")
            neo4j_user = os.getenv("MCP_NEO4J_USER")
            neo4j_password = os.getenv("MCP_NEO4J_PASSWORD")
            if neo4j_uri and neo4j_user and neo4j_password:
                if GraphDatabase is None:
                    logger.warning("neo4j package not installed, graph retrieval disabled")
                else:
                    self._neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

            self._ready = True
            logger.info("Legal answer runtime initialized")

    def health(self) -> Dict[str, Any]:
        missing = []
        if not os.getenv("MCP_OPENAI_API_KEY"):
            missing.append("MCP_OPENAI_API_KEY")

        return {
            "success": len(missing) == 0,
            "missing_env": missing,
            "dependencies": {
                "openai": OpenAI is not None,
                "pymilvus": PyMilvusClient is not None,
                "neo4j": GraphDatabase is not None,
            },
            "models": {
                "llm": self._llm_model,
                "embedding": self._embedding_model,
            },
            "milvus": {
                "uri": self._milvus_uri,
                "collection": self._milvus_collection,
            },
            "graph_enabled": bool(os.getenv("MCP_NEO4J_URI")),
        }

    def _embed_query(self, query: str) -> List[float]:
        response = self._openai_client.embeddings.create(  # type: ignore[union-attr]
            model=self._embedding_model,
            input=query,
        )
        return list(response.data[0].embedding)

    def _search_kb(self, query: str, top_k: int) -> List[Dict[str, Any]]:
        if self._milvus_client is None:
            return []

        vector = self._embed_query(query)
        try:
            search_results = self._milvus_client.search(
                collection_name=self._milvus_collection,
                data=[vector],
                anns_field="dense_vector",
                search_params={"metric_type": "COSINE", "params": {"ef": 128}},
                limit=max(1, top_k),
                output_fields=["article_id", "doc_id", "text", "title", "doc_type"],
            )
        except Exception as exc:
            logger.warning("Milvus search failed: %s", exc)
            return []

        hits = search_results[0] if search_results else []
        output: List[Dict[str, Any]] = []
        for h in hits:
            try:
                entity = h.get("entity", {}) if isinstance(h, dict) else getattr(h, "entity", {})
                if not isinstance(entity, dict):
                    entity = {}

                distance = h.get("distance") if isinstance(h, dict) else getattr(h, "distance", None)
                if isinstance(h, dict):
                    article_id = entity.get("article_id") or h.get("id")
                else:
                    article_id = entity.get("article_id") or getattr(h, "id", None)

                doc_id = str(entity.get("doc_id", ""))
                text = str(entity.get("text", ""))
                title = str(entity.get("title", ""))
                if not doc_id and isinstance(article_id, str) and ":" in article_id:
                    doc_id = article_id.split(":", 1)[0]

                score = 0.0
                if distance is not None:
                    score = 1.0 - float(distance) if float(distance) <= 1.0 else 1.0 / (1.0 + float(distance))
                if article_id and text:
                    output.append(
                        {
                            "source": "KB",
                            "article_id": str(article_id),
                            "doc_id": str(doc_id),
                            "title": str(title),
                            "text": str(text),
                            "score": float(score),
                        }
                    )
            except Exception:
                continue

        output.sort(key=lambda item: item.get("score", 0.0), reverse=True)
        return output

    def _expand_kg(self, kb_hits: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
        if self._neo4j_driver is None:
            return []

        seed_ids: List[str] = []
        seen = set()
        for hit in kb_hits:
            for seed in [hit.get("article_id"), hit.get("doc_id")]:
                if seed and seed not in seen:
                    seen.add(seed)
                    seed_ids.append(str(seed))

        if not seed_ids:
            return []

        query = """
        UNWIND $seed_ids AS sid
        CALL {
            WITH sid
            MATCH (n)
            WHERE n.id = sid OR n.doc_id = sid OR n.article_id = sid OR n.clause_id = sid
            MATCH (n)-[r]-(m)
            RETURN sid,
                   type(r) AS relation_type,
                   labels(m) AS labels,
                   m.doc_id AS doc_id,
                   m.article_id AS article_id,
                   m.clause_id AS clause_id,
                   coalesce(m.title, m.name, '') AS title,
                   coalesce(m.text, m.raw_text, '') AS text
            LIMIT $limit_per_seed
        }
        RETURN sid, relation_type, labels, doc_id, article_id, clause_id, title, text
        """

        output: List[Dict[str, Any]] = []
        dedup = set()
        try:
            with self._neo4j_driver.session() as session:
                records = session.run(
                    query,
                    {
                        "seed_ids": seed_ids,
                        "limit_per_seed": max(1, top_k),
                    },
                )
                for rec in records:
                    node_key = rec.get("doc_id") or rec.get("article_id") or rec.get("clause_id")
                    if not node_key:
                        continue
                    if node_key in dedup:
                        continue
                    dedup.add(node_key)
                    output.append(
                        {
                            "source": "KG",
                            "label": (rec.get("labels") or ["Node"])[0],
                            "doc_id": rec.get("doc_id"),
                            "article_id": rec.get("article_id"),
                            "clause_id": rec.get("clause_id"),
                            "title": rec.get("title") or "",
                            "text": rec.get("text") or "",
                            "relation_type": rec.get("relation_type") or "",
                            "score": 0.8,
                        }
                    )
        except Exception as exc:
            logger.warning("Neo4j expansion failed: %s", exc)

        return output

    def _build_context(self, kb_hits: List[Dict[str, Any]], kg_hits: List[Dict[str, Any]]) -> str:
        lines: List[str] = []
        used_chars = 0

        def push(block: str) -> bool:
            nonlocal used_chars
            if used_chars >= CONTEXT_CHAR_BUDGET:
                return False
            block = _truncate(block, max(1, CONTEXT_CHAR_BUDGET - used_chars))
            lines.append(block)
            used_chars += len(block)
            return used_chars < CONTEXT_CHAR_BUDGET

        if kb_hits:
            push("=== Knowledge Base (Milvus) ===\n")
        for idx, item in enumerate(kb_hits[:MAX_CONTEXT_ITEMS], 1):
            text = _truncate(item.get("text", ""), MAX_ITEM_CHARS)
            row = (
                f"[KB-{idx}] doc={item.get('doc_id', '')} article={item.get('article_id', '')} "
                f"score={item.get('score', 0.0):.4f}\n{text}\n"
            )
            if not push(row):
                break

        if kg_hits and used_chars < CONTEXT_CHAR_BUDGET:
            push("\n=== Knowledge Graph (Neo4j) ===\n")
        for idx, item in enumerate(kg_hits[:MAX_CONTEXT_ITEMS], 1):
            text = _truncate(item.get("text", ""), MAX_ITEM_CHARS)
            row = (
                f"[KG-{idx}] type={item.get('label', '')} relation={item.get('relation_type', '')} "
                f"doc={item.get('doc_id', '')} article={item.get('article_id', '')}\n{text}\n"
            )
            if not push(row):
                break

        return "\n".join(lines)

    def _generate_answer(self, query: str, context: str) -> str:
        prompt = (
            "You are a Vietnamese legal assistant. "
            "Answer only from provided context and cite sources as [KB-x] or [KG-x].\n\n"
            f"Context:\n{context}\n\n"
            f"Question: {query}\n\n"
            "Requirements:\n"
            "1) Be concise and legally grounded.\n"
            "2) If context is insufficient, explicitly say so.\n"
            "3) Keep citations next to each key statement."
        )

        response = self._openai_client.chat.completions.create(  # type: ignore[union-attr]
            model=self._llm_model,
            messages=[
                {"role": "system", "content": "You provide grounded Vietnamese legal answers."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        return response.choices[0].message.content or ""

    def answer(self, question: str, top_k: int = DEFAULT_TOP_K, include_graph: bool = True, use_cache: bool = True) -> Dict[str, Any]:
        if not question or not question.strip():
            return {"success": False, "error": "question must not be empty"}

        self.ensure_ready()
        safe_top_k = max(1, min(int(top_k), MAX_TOP_K))
        normalized_query = _normalize_query(question)
        cache_key = self._cache_key(normalized_query, safe_top_k, include_graph)

        if use_cache:
            cached = self._cache_get(cache_key)
            if cached is not None:
                out = dict(cached)
                out["cache_hit"] = True
                return out

        t0 = time.perf_counter()
        t_retrieve_start = time.perf_counter()
        kb_hits = self._search_kb(normalized_query, safe_top_k)
        kg_hits = self._expand_kg(kb_hits, safe_top_k) if include_graph else []
        retrieve_ms = (time.perf_counter() - t_retrieve_start) * 1000.0

        if not kb_hits and not kg_hits:
            response = {
                "success": True,
                "query": normalized_query,
                "answer": "No relevant evidence found in the configured knowledge stores.",
                "retrieved": {"kb": 0, "kg": 0},
                "sources": [],
                "latency_ms": {
                    "retrieve": round(retrieve_ms, 2),
                    "generate": 0.0,
                    "total": round((time.perf_counter() - t0) * 1000.0, 2),
                },
                "cache_hit": False,
            }
            if use_cache:
                self._cache_set(cache_key, response)
            return response

        context = self._build_context(kb_hits, kg_hits)

        t_generate_start = time.perf_counter()
        answer_text = self._generate_answer(normalized_query, context)
        generate_ms = (time.perf_counter() - t_generate_start) * 1000.0

        sources: List[Dict[str, Any]] = []
        for item in kb_hits:
            sources.append(
                {
                    "source": "KB",
                    "doc_id": item.get("doc_id"),
                    "article_id": item.get("article_id"),
                    "score": item.get("score"),
                }
            )
        for item in kg_hits:
            sources.append(
                {
                    "source": "KG",
                    "doc_id": item.get("doc_id"),
                    "article_id": item.get("article_id"),
                    "clause_id": item.get("clause_id"),
                    "relation_type": item.get("relation_type"),
                }
            )

        response = {
            "success": True,
            "query": normalized_query,
            "answer": answer_text,
            "retrieved": {"kb": len(kb_hits), "kg": len(kg_hits)},
            "sources": sources,
            "latency_ms": {
                "retrieve": round(retrieve_ms, 2),
                "generate": round(generate_ms, 2),
                "total": round((time.perf_counter() - t0) * 1000.0, 2),
            },
            "cache_hit": False,
        }

        if use_cache:
            self._cache_set(cache_key, response)

        return response

    def close(self) -> None:
        if self._neo4j_driver is not None:
            try:
                self._neo4j_driver.close()
            except Exception:
                logger.exception("Failed to close Neo4j driver")


_RUNTIME = HybridAnswerRuntime()


@mcp.tool()
def answer_service_healthcheck() -> Dict[str, Any]:
    """Return health and dependency status for the standalone legal answer pipeline."""
    return _RUNTIME.health()


@mcp.tool()
def answer_legal_question(
    question: str,
    top_k: int = DEFAULT_TOP_K,
    include_graph: bool = True,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """Answer legal question with standalone hybrid workflow: normalize -> KB -> KG -> grounded LLM."""
    try:
        return _RUNTIME.answer(
            question=question,
            top_k=top_k,
            include_graph=include_graph,
            use_cache=use_cache,
        )
    except Exception as exc:
        logger.exception("answer_legal_question failed")
        return {
            "success": False,
            "error": str(exc),
        }


if __name__ == "__main__":
    try:
        mcp.run(transport="stdio")
    finally:
        _RUNTIME.close()
