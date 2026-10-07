from __future__ import annotations

from elasticsearch import ConnectionError as ESConnectionError
from elasticsearch import NotFoundError
from fastapi import APIRouter, HTTPException

from app.es_client import INDEX_NAME, get_client
from app.schemas import DocumentDetail

router = APIRouter()


@router.get("/api/documents/{doc_id:path}", response_model=DocumentDetail)
def get_document(doc_id: str):
    try:
        resp = get_client().get(index=INDEX_NAME, id=doc_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Document not found")
    except ESConnectionError:
        raise HTTPException(status_code=503, detail="Elasticsearch is not reachable")

    return DocumentDetail(id=resp["_id"], **resp["_source"])
