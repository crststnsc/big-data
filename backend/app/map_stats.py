from __future__ import annotations

MAX_BUCKETS = 300
GEOHASH_PRECISION = 3  # ~156km x 156km cells at the equator

REP_SOURCE_FIELDS = ["place", "country", "actor1", "actor2", "event_code", "quad_class", "content_date"]


def build_map_query(
    date_from: str | None,
    date_to: str | None,
    quad_class: list[int] | None,
    event_root_code: list[str] | None,
) -> dict:
    filter_clauses: list[dict] = [
        {"term": {"type": "gdelt_event"}},
        {"exists": {"field": "location"}},
    ]
    if date_from or date_to:
        rng = {}
        if date_from:
            rng["gte"] = date_from
        if date_to:
            rng["lte"] = date_to
        filter_clauses.append({"range": {"content_date": rng}})
    if quad_class:
        filter_clauses.append({"terms": {"quad_class": quad_class}})
    if event_root_code:
        filter_clauses.append({"terms": {"event_root_code": event_root_code}})

    return {
        "size": 0,
        "query": {"bool": {"filter": filter_clauses}},
        "track_total_hits": True,
        "aggs": {
            "grid": {
                "geohash_grid": {"field": "location", "precision": GEOHASH_PRECISION, "size": MAX_BUCKETS},
                "aggs": {
                    "centroid": {"geo_centroid": {"field": "location"}},
                    "rep": {"top_hits": {"size": 1, "_source": REP_SOURCE_FIELDS}},
                },
            }
        },
    }
