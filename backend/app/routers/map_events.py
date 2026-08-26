"""File named map_events.py rather than map.py to avoid shadowing Python's
builtin `map` in any module that imports this package's routers.
"""
from __future__ import annotations

from elasticsearch import ConnectionError as ESConnectionError
from fastapi import APIRouter, HTTPException, Query

from app.es_client import INDEX_NAME, get_client
from app.map_stats import build_map_query
from app.schemas import MapEventPoint, MapEventsResponse

router = APIRouter()


@router.get("/api/map/events", response_model=MapEventsResponse)
def map_events(
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    quad_class: list[int] | None = Query(None),
    event_root_code: list[str] | None = Query(None),
):
    body = build_map_query(date_from, date_to, quad_class, event_root_code)
    try:
        resp = get_client().search(index=INDEX_NAME, **body)
    except ESConnectionError:
        raise HTTPException(status_code=503, detail="Elasticsearch is not reachable")

    points = []
    for b in resp["aggregations"]["grid"]["buckets"]:
        centroid = b["centroid"]["location"]
        rep = b["rep"]["hits"]["hits"][0]
        points.append(
            MapEventPoint(
                geohash=b["key"],
                lat=centroid["lat"],
                lon=centroid["lon"],
                count=b["doc_count"],
                doc_id=rep["_id"],
                **rep["_source"],
            )
        )

    return MapEventsResponse(
        total_matched=resp["hits"]["total"]["value"],
        bucket_count=len(points),
        points=points,
    )
