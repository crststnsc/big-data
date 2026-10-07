from __future__ import annotations

from typing import Literal

from elasticsearch import ConnectionError as ESConnectionError
from fastapi import APIRouter, HTTPException, Query

from app.es_client import INDEX_NAME, get_client
from app.schemas import FacetBucket, FacetsResponse, SearchResponse
from app.search import HIGHLIGHT, LIST_SOURCE_FIELDS, build_query, build_sort, hit_to_summary

router = APIRouter()


@router.get("/api/search", response_model=SearchResponse)
def search(
    q: str = Query(""),
    type: list[str] | None = Query(None),
    language: list[str] | None = Query(None),
    feed_category: list[str] | None = Query(None),
    entity: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    sort: Literal["relevance", "date_desc", "date_asc"] | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    filters = {
        "type": type,
        "language": language,
        "feed_category": feed_category,
        "entity": entity,
        "date_from": date_from,
        "date_to": date_to,
    }
    body = {
        "query": build_query(q, filters),
        "sort": build_sort(sort, q),
        "from": (page - 1) * page_size,
        "size": page_size,
        "_source": LIST_SOURCE_FIELDS,
        "track_total_hits": True,
    }
    if q and q.strip():
        body["highlight"] = HIGHLIGHT

    try:
        resp = get_client().search(index=INDEX_NAME, **body)
    except ESConnectionError:
        raise HTTPException(status_code=503, detail="Elasticsearch is not reachable")

    results = [hit_to_summary(h) for h in resp["hits"]["hits"]]
    return SearchResponse(total=resp["hits"]["total"]["value"], page=page, page_size=page_size, results=results)


@router.get("/api/facets", response_model=FacetsResponse)
def facets():
    body = {
        "size": 0,
        "aggs": {
            "types": {"terms": {"field": "type", "size": 10}},
            "languages": {"terms": {"field": "language", "size": 50}},
            "feed_categories": {"terms": {"field": "feed_category", "size": 50}},
        },
    }
    try:
        resp = get_client().search(index=INDEX_NAME, **body)
    except ESConnectionError:
        raise HTTPException(status_code=503, detail="Elasticsearch is not reachable")

    def buckets(name: str) -> list[FacetBucket]:
        return [FacetBucket(key=b["key"], count=b["doc_count"]) for b in resp["aggregations"][name]["buckets"]]

    return FacetsResponse(
        types=buckets("types"), languages=buckets("languages"), feed_categories=buckets("feed_categories")
    )
