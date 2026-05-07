from typing import Optional

import anthropic
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shared.utils.logger import get_logger

logger = get_logger("api.routers.search")
router = APIRouter()

_service = None


def _get_service():
    global _service
    if _service is None:
        from api.services.rag_search import RAGSearchService
        _service = RAGSearchService()
    return _service


class SearchRequest(BaseModel):
    question: str
    top_k: int = Field(default=10, ge=1, le=50)
    filters: Optional[dict] = Field(
        default=None,
        description=(
            "Optional Qdrant payload filters. "
            "Keys: device_id, vendor, region, segment, operation. "
            "Example: {\"region\": \"mumbai\", \"vendor\": \"cisco_ios\"}"
        ),
    )


@router.post("/search")
async def search_device_logs(request: SearchRequest):
    """
    RAG + LLM search over raw device CLI output.

    Finds the most relevant device log excerpts in Qdrant using
    semantic (embedding) search, then passes the retrieved context
    to Claude for analysis and explanation.

    Use this endpoint when you need to:
    - Understand what a device log actually means
    - Identify patterns or anomalies across device output
    - Get a natural-language explanation of CLI output

    For structured queries ("find all Cisco devices with CRC > 100")
    use POST /api/v1/query instead.
    """
    try:
        result = _get_service().search(
            question=request.question,
            top_k=request.top_k,
            filters=request.filters,
        )
        return result
    except anthropic.APIStatusError as exc:
        if exc.status_code >= 500:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "llm_unavailable",
                    "detail": str(exc.message),
                },
            )
        raise HTTPException(status_code=exc.status_code, detail=str(exc.message))
    except anthropic.APIConnectionError as exc:
        raise HTTPException(
            status_code=503,
            detail={"error": "llm_unavailable", "detail": str(exc)},
        )
    except Exception as exc:
        msg = str(exc).lower()
        if "qdrant" in msg or "connection refused" in msg:
            raise HTTPException(
                status_code=503,
                detail={"error": "qdrant_unavailable", "detail": str(exc)},
            )
        logger.error(f"search_device_logs failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
