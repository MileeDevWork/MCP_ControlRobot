from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import httpx

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


PROVIDER_PRIORITY = ["openai", "claude", "gemini", "togetherai"]
EMBEDDING_CAPABLE_PROVIDERS = {"openai", "gemini", "togetherai"}


class ProviderSetupError(RuntimeError):
    pass


def _first_non_empty(*values: Optional[str]) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


class ProviderClientFallback:
    def __init__(
        self,
        llm_model: Optional[str] = None,
        embedding_model: Optional[str] = None,
        timeout_seconds: float = 60.0,
    ) -> None:
        self.timeout_seconds = timeout_seconds

        self.keys: Dict[str, str] = {
            "openai": _first_non_empty(
                os.getenv("MCP_OPENAI_API_KEY"),
                os.getenv("OPENAI_API_KEY"),
            ),
            "claude": _first_non_empty(
                os.getenv("MCP_CLAUDE_API_KEY"),
                os.getenv("MCP_ANTHROPIC_API_KEY"),
                os.getenv("ANTHROPIC_API_KEY"),
            ),
            "gemini": _first_non_empty(
                os.getenv("MCP_GEMINI_API_KEY"),
                os.getenv("GOOGLE_API_KEY"),
                os.getenv("GEMINI_API_KEY"),
            ),
            "togetherai": _first_non_empty(
                os.getenv("MCP_TOGETHER_API_KEY"),
                os.getenv("TOGETHER_API_KEY"),
            ),
        }

        self.llm_models: Dict[str, str] = {
            "openai": _first_non_empty(
                os.getenv("MCP_OPENAI_LLM_MODEL"),
                llm_model,
                os.getenv("MCP_LLM_MODEL"),
                "gpt-4o-mini",
            ),
            "claude": _first_non_empty(
                os.getenv("MCP_CLAUDE_LLM_MODEL"),
                "claude-3-5-sonnet-latest",
            ),
            "gemini": _first_non_empty(
                os.getenv("MCP_GEMINI_LLM_MODEL"),
                "gemini-2.0-flash",
            ),
            "togetherai": _first_non_empty(
                os.getenv("MCP_TOGETHER_LLM_MODEL"),
                "meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo",
            ),
        }

        self.embedding_models: Dict[str, str] = {
            "openai": _first_non_empty(
                os.getenv("MCP_OPENAI_EMBEDDING_MODEL"),
                embedding_model,
                os.getenv("MCP_EMBEDDING_MODEL"),
                "text-embedding-3-small",
            ),
            "gemini": _first_non_empty(
                os.getenv("MCP_GEMINI_EMBEDDING_MODEL"),
                "gemini-embedding-001",
            ),
            "togetherai": _first_non_empty(
                os.getenv("MCP_TOGETHER_EMBEDDING_MODEL"),
                "togethercomputer/m2-bert-80M-32k-retrieval",
            ),
        }

        self.base_urls: Dict[str, str] = {
            "togetherai": _first_non_empty(
                os.getenv("MCP_TOGETHER_BASE_URL"),
                "https://api.together.xyz/v1",
            ),
        }
        self.gemini_base_url = _first_non_empty(
            os.getenv("MCP_GEMINI_BASE_URL"),
            "https://generativelanguage.googleapis.com/v1beta",
        )

        self._openai_clients: Dict[str, Any] = {}
        self.last_generation_provider: Optional[str] = None
        self.last_embedding_provider: Optional[str] = None

    def available_generation_providers(self) -> List[str]:
        return [provider for provider in PROVIDER_PRIORITY if self.keys.get(provider)]

    def available_embedding_providers(self) -> List[str]:
        return [
            provider
            for provider in PROVIDER_PRIORITY
            if provider in EMBEDDING_CAPABLE_PROVIDERS and self.keys.get(provider)
        ]

    def status(self) -> Dict[str, Any]:
        return {
            "priority": list(PROVIDER_PRIORITY),
            "available_generation_providers": self.available_generation_providers(),
            "available_embedding_providers": self.available_embedding_providers(),
            "active_generation_provider": self.last_generation_provider,
            "active_embedding_provider": self.last_embedding_provider,
            "models": {
                "llm": dict(self.llm_models),
                "embedding": dict(self.embedding_models),
            },
        }

    def validate(self, require_generation: bool, require_embeddings: bool) -> None:
        if require_generation and not self.available_generation_providers():
            raise ProviderSetupError(
                "No generation provider key found. Configure one of: "
                "MCP_OPENAI_API_KEY, MCP_CLAUDE_API_KEY, MCP_GEMINI_API_KEY, MCP_TOGETHER_API_KEY"
            )

        if require_embeddings and not self.available_embedding_providers():
            raise ProviderSetupError(
                "No embedding-capable provider key found. Configure one of: "
                "MCP_OPENAI_API_KEY, MCP_GEMINI_API_KEY, MCP_TOGETHER_API_KEY"
            )

    def _ordered_candidates(self, providers: List[str], preferred: Optional[str]) -> List[str]:
        if not preferred:
            return list(providers)
        if preferred not in providers:
            return list(providers)
        ordered = [preferred]
        for provider in providers:
            if provider != preferred:
                ordered.append(provider)
        return ordered

    def _openai_client_for(self, provider: str) -> Any:
        if OpenAI is None:
            raise RuntimeError("openai package is not installed")

        cached = self._openai_clients.get(provider)
        if cached is not None:
            return cached

        key = self.keys.get(provider, "")
        if not key:
            raise RuntimeError(f"Missing key for provider: {provider}")

        kwargs: Dict[str, Any] = {"api_key": key}
        if provider in self.base_urls:
            kwargs["base_url"] = self.base_urls[provider]

        client = OpenAI(**kwargs)
        self._openai_clients[provider] = client
        return client

    def _anthropic_generate(self, prompt: str, system: str, temperature: float) -> str:
        api_key = self.keys.get("claude", "")
        if not api_key:
            raise RuntimeError("MCP_CLAUDE_API_KEY is not configured")

        model = self.llm_models["claude"]
        max_tokens = max(128, int(os.getenv("MCP_CLAUDE_MAX_TOKENS", "1200")))

        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system,
            "messages": [{"role": "user", "content": prompt}],
        }

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        parts = data.get("content") or []
        text_parts = [str(item.get("text") or "") for item in parts if item.get("type") == "text"]
        output = "".join(text_parts).strip()
        if not output:
            raise RuntimeError("Claude returned empty content")
        return output

    def _gemini_generate(self, prompt: str, system: str, temperature: float) -> str:
        api_key = self.keys.get("gemini", "")
        if not api_key:
            raise RuntimeError("MCP_GEMINI_API_KEY is not configured")

        model = self.llm_models["gemini"]
        url = f"{self.gemini_base_url}/models/{model}:generateContent?key={api_key}"
        payload = {
            "system_instruction": {
                "parts": [{"text": system}],
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": {
                "temperature": temperature,
            },
        }

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()

        candidates = data.get("candidates") or []
        for candidate in candidates:
            content = candidate.get("content") or {}
            parts = content.get("parts") or []
            text = "".join(str(part.get("text") or "") for part in parts).strip()
            if text:
                return text

        raise RuntimeError("Gemini returned empty content")

    def _gemini_embed_texts(self, texts: List[str]) -> List[List[float]]:
        api_key = self.keys.get("gemini", "")
        if not api_key:
            raise RuntimeError("MCP_GEMINI_API_KEY is not configured")

        model = self.embedding_models["gemini"]
        url = f"{self.gemini_base_url}/models/{model}:embedContent?key={api_key}"

        vectors: List[List[float]] = []
        with httpx.Client(timeout=self.timeout_seconds) as client:
            for text in texts:
                payload = {
                    "content": {
                        "parts": [{"text": text}],
                    }
                }
                response = client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
                values = ((data.get("embedding") or {}).get("values") or [])
                if not values:
                    raise RuntimeError("Gemini embedding response was empty")
                vectors.append([float(value) for value in values])

        return vectors

    def generate_text(self, prompt: str, system: str, temperature: float = 0.2) -> Tuple[str, str]:
        providers = self.available_generation_providers()
        if not providers:
            raise ProviderSetupError("No available generation providers")

        errors: List[str] = []
        for provider in self._ordered_candidates(providers, self.last_generation_provider):
            try:
                if provider == "claude":
                    output = self._anthropic_generate(prompt=prompt, system=system, temperature=temperature)
                elif provider == "gemini":
                    output = self._gemini_generate(prompt=prompt, system=system, temperature=temperature)
                else:
                    client = self._openai_client_for(provider)
                    response = client.chat.completions.create(
                        model=self.llm_models[provider],
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": prompt},
                        ],
                        temperature=temperature,
                    )
                    output = str(response.choices[0].message.content or "").strip()
                    if not output:
                        raise RuntimeError("Model returned empty content")

                self.last_generation_provider = provider
                return output, provider
            except Exception as exc:
                errors.append(f"{provider}: {exc}")

        raise RuntimeError("All generation providers failed: " + " | ".join(errors))

    def embed_texts(self, texts: List[str], dimensions: Optional[int] = None) -> Tuple[List[List[float]], str]:
        providers = self.available_embedding_providers()
        if not providers:
            raise ProviderSetupError("No available embedding providers")

        errors: List[str] = []
        for provider in self._ordered_candidates(providers, self.last_embedding_provider):
            try:
                if provider == "gemini":
                    vectors = self._gemini_embed_texts(texts)
                else:
                    client = self._openai_client_for(provider)
                    kwargs: Dict[str, Any] = {
                        "model": self.embedding_models[provider],
                        "input": texts,
                    }
                    if dimensions is not None and provider == "openai":
                        kwargs["dimensions"] = dimensions

                    response = client.embeddings.create(**kwargs)
                    vectors = [list(item.embedding) for item in response.data]
                if not vectors:
                    raise RuntimeError("Embedding response was empty")

                self.last_embedding_provider = provider
                return vectors, provider
            except Exception as exc:
                errors.append(f"{provider}: {exc}")

        raise RuntimeError("All embedding providers failed: " + " | ".join(errors))

    def embed_query(self, text: str, dimensions: Optional[int] = None) -> Tuple[List[float], str]:
        vectors, provider = self.embed_texts([text], dimensions=dimensions)
        return vectors[0], provider
