from __future__ import annotations

from elasticsearch import ConnectionError as ESConnectionError
from elasticsearch import NotFoundError
from fastapi import APIRouter, HTTPException

from app.es_client import INDEX_NAME, get_client
from app.schemas import DocumentDetail

router = APIRouter()


@router.get("/api/documents/{doc_id:path}", response_model=DocumentDetail)
def get_document(doc_id: str):
    # doc_id is a full URL (GDELT SOURCEURL / RSS guid-or-link), so it
    # contains "/" -- FastAPI's default path converter stops at the first
    # "/" it sees once the incoming %2F is decoded, causing a 404 on every
    # real id. The :path converter matches the rest of the path including
    # slashes.
    try:
        resp = get_client().get(index=INDEX_NAME, id=doc_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Document not found")
    except ESConnectionError:
        raise HTTPException(status_code=503, detail="Elasticsearch is not reachable")

    return DocumentDetail(id=resp["_id"], **resp["_source"])
