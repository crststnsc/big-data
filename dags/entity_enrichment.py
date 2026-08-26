from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

MAX_DOCS_PER_RUN = 2000


@dag(
    dag_id="entity_enrichment",
    description="Fill in entities/entities_en on news_events docs that don't have them yet",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["enrichment", "nlp", "ingestion"],
)
def entity_enrichment():
    @task
    def enrich_gdelt() -> dict:
        from elasticsearch.helpers import scan
        from news_es import INDEX_NAME, bulk_index_actions, ensure_index, get_client

        es = get_client()
        ensure_index(es)

        query = {
            "bool": {
                "filter": [{"term": {"type": "gdelt_event"}}],
                "must_not": [{"exists": {"field": "enriched_at"}}],
            }
        }
        hits = scan(
            es, index=INDEX_NAME, query={"query": query},
            _source=["actor1", "actor2", "place"], size=1000,
        )

        counts = {"processed": 0}

        def actions():
            for hit in hits:
                if counts["processed"] >= MAX_DOCS_PER_RUN:
                    break
                src = hit["_source"]
                organizations = sorted({v for v in (src.get("actor1"), src.get("actor2")) if v})
                locations = [src["place"]] if src.get("place") else []
                entities = {"persons": [], "organizations": organizations, "locations": locations}
                counts["processed"] += 1
                yield {
                    "_op_type": "update",
                    "_index": INDEX_NAME,
                    "_id": hit["_id"],
                    "doc": {
                        "entities": entities,
                        "entities_en": entities,
                        "enriched_at": datetime.now(timezone.utc).isoformat(),
                        "fetch_status": "ok",
                    },
                }

        stats = bulk_index_actions(actions(), es)
        stats.update(counts)
        logger.info("GDELT entity enrichment: %s", stats)
        return stats

    @task
    def enrich_rss() -> dict:
        from elasticsearch.helpers import scan
        from news_es import INDEX_NAME, bulk_index_actions, ensure_index, get_client
        from nlp_enrich import extract_entities, translate_entities

        es = get_client()
        ensure_index(es)

        query = {
            "bool": {
                "filter": [{"term": {"type": "rss_article"}}],
                "must_not": [{"exists": {"field": "enriched_at"}}],
            }
        }
        hits = scan(
            es, index=INDEX_NAME, query={"query": query},
            _source=["content", "summary", "title", "language", "fetch_status"], size=500,
        )

        counts = {"processed": 0, "skipped_unsupported": 0}

        def actions():
            for hit in hits:
                if counts["processed"] + counts["skipped_unsupported"] >= MAX_DOCS_PER_RUN:
                    break
                src = hit["_source"]
                lang = src.get("language")
                text = src.get("content") or src.get("summary") or src.get("title") or ""
                now = datetime.now(timezone.utc).isoformat()

                entities = extract_entities(text, lang) if lang and text else None
                if entities is None:
                    counts["skipped_unsupported"] += 1
                    doc_update = {"enriched_at": now}
                    if src.get("fetch_status") != "ok":
                        doc_update["fetch_status"] = "failed"
                    yield {"_op_type": "update", "_index": INDEX_NAME, "_id": hit["_id"], "doc": doc_update}
                    continue

                entities_en = translate_entities(entities, lang)
                counts["processed"] += 1
                yield {
                    "_op_type": "update",
                    "_index": INDEX_NAME,
                    "_id": hit["_id"],
                    "doc": {
                        "entities": entities,
                        "entities_en": entities_en,
                        "enriched_at": now,
                        "fetch_status": "ok",  # entities successfully extracted
                    },
                }

        stats = bulk_index_actions(actions(), es)
        stats.update(counts)
        logger.info("RSS entity enrichment: %s", stats)
        return stats

    enrich_gdelt()
    enrich_rss()


entity_enrichment()
