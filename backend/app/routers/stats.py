from __future__ import annotations

from elasticsearch import ConnectionError as ESConnectionError
from fastapi import APIRouter, HTTPException

from app.es_client import INDEX_NAME, get_client
from app.schemas import DateBucket, FacetBucket, StatsResponse, ToneBucket
from app.stats import build_stats_query

router = APIRouter()


@router.get("/api/stats", response_model=StatsResponse)
def stats():
    try:
        resp = get_client().search(index=INDEX_NAME, **build_stats_query())
    except ESConnectionError:
        raise HTTPException(status_code=503, detail="Elasticsearch is not reachable")

    aggs = resp["aggregations"]
    by_type = {b["key"]: b["doc_count"] for b in aggs["by_type"]["buckets"]}

    volume_over_time = []
    for b in aggs["volume_over_time"]["buckets"]:
        counts = {sub["key"]: sub["doc_count"] for sub in b["by_type"]["buckets"]}
        volume_over_time.append(
            DateBucket(
                date=b["key_as_string"][:10],
                gdelt_event=counts.get("gdelt_event", 0),
                rss_article=counts.get("rss_article", 0),
            )
        )

    avg_tone_over_time = [
        ToneBucket(date=b["key_as_string"][:10], avg_tone=b["avg_tone"]["value"])
        for b in aggs["avg_tone_over_time"]["over_time"]["buckets"]
    ]

    def buckets(agg_name: str) -> list[FacetBucket]:
        return [FacetBucket(key=str(b["key"]), count=b["doc_count"]) for b in aggs[agg_name]["vals"]["buckets"]]

    return StatsResponse(
        total_docs=sum(by_type.values()),
        total_gdelt_events=by_type.get("gdelt_event", 0),
        total_rss_articles=by_type.get("rss_article", 0),
        volume_over_time=volume_over_time,
        quad_class=buckets("quad_class"),
        event_root_code=buckets("event_root_code"),
        languages=buckets("languages"),
        avg_tone_over_time=avg_tone_over_time,
    )
