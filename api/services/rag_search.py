from typing import Optional

import anthropic
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from sentence_transformers import SentenceTransformer

from shared.config.settings import NLQueryConfig, QdrantConfig, RAGConfig
from shared.utils.logger import get_logger

logger = get_logger("api.rag_search")

# System prompt is static across all queries — use prompt caching
_SYSTEM_PROMPT = """You are a network operations analyst with deep expertise \
in multi-vendor device management.

You will be given excerpts from actual CLI output collected by an automated \
fleet management system. Each excerpt includes the device vendor, region, \
network segment, and operation that produced the output.

Your job is to analyse the device logs and directly answer the question.
- Reference specific values, interface names, error counts, or patterns \
you observe.
- If you see anomalies or potential issues, call them out explicitly.
- If the logs are insufficient to answer fully, say so and describe what \
additional data would help.
- Be concise and actionable — an operations engineer will act on your answer."""


class RAGSearchService:
    """
    Phase 3 — RAG + LLM search over raw device CLI output.

    Retrieval path:
        1. Embed the user question with all-MiniLM-L6-v2 (same model
           as the RAG Indexer — ensures vector-space consistency).
        2. Qdrant nearest-neighbour search returns the most relevant
           raw CLI log chunks, ranked by cosine similarity.

    Analysis path:
        3. Build a structured context block from retrieved chunks,
           each annotated with device metadata.
        4. Call Claude with the static system prompt (prompt-cached)
           and the combined context + question.
        5. Return Claude's analysis alongside the supporting evidence.

    Why raw logs instead of TextFSM-normalised records for RAG:
        TextFSM-normalised records are structured JSON in MongoDB — they
        are used by Phase 2 NL Query for precise field-level filters.
        Raw CLI output contains the full conversational richness of device
        responses and is better suited for semantic retrieval and LLM
        analysis (error messages, log lines, descriptive text).
    """

    def __init__(self):
        self._qdrant = QdrantClient(
            host=QdrantConfig.HOST,
            port=QdrantConfig.PORT,
            timeout=10,
        )
        self._collection = QdrantConfig.COLLECTION
        self._anthropic = anthropic.Anthropic(
            api_key=NLQueryConfig.ANTHROPIC_API_KEY
        )
        self._model: Optional[SentenceTransformer] = None

    def search(
        self,
        question: str,
        top_k: int = 10,
        filters: Optional[dict] = None,
    ) -> dict:
        question_vec = self._embed(question)
        qdrant_filter = self._build_filter(filters) if filters else None

        hits = self._qdrant.search(
            collection_name=self._collection,
            query_vector=question_vec,
            limit=top_k,
            query_filter=qdrant_filter,
            with_payload=True,
        )

        if not hits:
            return {
                "question": question,
                "analysis": (
                    "No relevant device logs found for your query. "
                    "The fleet may not have been polled yet, or the "
                    "filters are too restrictive."
                ),
                "evidence": [],
            }

        evidence, context_blocks = self._build_evidence(hits)
        analysis = self._analyse(question, "\n\n---\n\n".join(context_blocks))

        return {
            "question": question,
            "analysis": analysis,
            "evidence": evidence,
        }

    # ------------------------------------------------------------------ #
    # Internals                                                            #
    # ------------------------------------------------------------------ #

    def _embed(self, text: str) -> list[float]:
        if self._model is None:
            logger.info(
                f"Loading embedding model: {RAGConfig.EMBEDDING_MODEL}"
            )
            self._model = SentenceTransformer(RAGConfig.EMBEDDING_MODEL)
        vec = self._model.encode([text])[0]
        return vec.tolist() if hasattr(vec, "tolist") else list(vec)

    def _build_evidence(
        self, hits: list
    ) -> tuple[list[dict], list[str]]:
        evidence = []
        context_blocks = []
        for hit in hits:
            p = hit.payload
            evidence.append({
                "score": round(hit.score, 4),
                "device_id": p.get("device_id"),
                "vendor": p.get("vendor"),
                "region": p.get("region"),
                "segment": p.get("segment"),
                "operation": p.get("operation"),
                "collected_at": p.get("collected_at"),
                "raw_chunk": p.get("raw_chunk"),
            })
            context_blocks.append(
                f"[device={p.get('device_id')} "
                f"vendor={p.get('vendor')} "
                f"region={p.get('region')} "
                f"segment={p.get('segment')} "
                f"op={p.get('operation')} "
                f"relevance={hit.score:.3f}]\n"
                f"{p.get('raw_chunk', '')}"
            )
        return evidence, context_blocks

    def _analyse(self, question: str, context: str) -> str:
        """Call Claude with retrieved log context. System prompt is cached."""
        user_content = (
            f"Device log excerpts (most relevant first):\n\n"
            f"{context}\n\n"
            f"Question: {question}"
        )
        try:
            response = self._anthropic.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=[
                    {
                        "type": "text",
                        "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_content}],
            )
            return response.content[0].text
        except anthropic.APIStatusError as exc:
            logger.error(f"Claude API error {exc.status_code}: {exc.message}")
            raise
        except anthropic.APIConnectionError as exc:
            logger.error(f"Claude connection error: {exc}")
            raise

    def _build_filter(self, filters: dict) -> Optional[Filter]:
        conditions = [
            FieldCondition(key=k, match=MatchValue(value=v))
            for k, v in filters.items()
            if isinstance(v, str)
        ]
        return Filter(must=conditions) if conditions else None
