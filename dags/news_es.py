"""Elasticsearch index definition + bulk loader for GDELT v2 events and RSS
feed articles, sharing one index so both can be queried/searched together.

Run directly to (re)create the index:
    python news_es.py --recreate
    python news_es.py --load /path/to/20260822123000.csv

Import from the Airflow DAGs for the indexing tasks:
    from news_es import bulk_index_file, bulk_index_rss_docs, rss_entry_to_doc
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import urlparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
INDEX_NAME = os.environ.get("NEWS_INDEX", "news_events")

GDELT_EVENT_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat",
    "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat",
    "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]


INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "content_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                }
            }
        },
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            # --- identity / provenance
            "type": {"type": "keyword"},
            "ingested_at": {"type": "date"},
            "interval_ts": {"type": "keyword"},

            # --- time
            "seen_at": {"type": "date"},        # GDELT: DATEADDED
            "content_date": {"type": "date"},   # GDELT: Day | RSS: entry published/updated date

            # --- source article
            "url": {"type": "keyword"},
            "domain": {"type": "keyword"},

            # --- CAMEO codes (keyword: exact-match filters + terms aggregations)
            "event_code": {"type": "keyword"},
            "event_base_code": {"type": "keyword"},
            "event_root_code": {"type": "keyword"},
            "quad_class": {"type": "integer"},
            "is_root_event": {"type": "boolean"},

            # --- numeric metrics (dashboards)
            "goldstein": {"type": "float"},
            "avg_tone": {"type": "float"},
            "num_mentions": {"type": "integer"},
            "num_sources": {"type": "integer"},
            "num_articles": {"type": "integer"},

            # --- actors (the graph edges)
            "actor1": {
                "type": "text",
                "analyzer": "content_analyzer",
                "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
            },
            "actor2": {
                "type": "text",
                "analyzer": "content_analyzer",
                "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
            },
            "actor1_country": {"type": "keyword"},
            "actor2_country": {"type": "keyword"},

            # --- geography (the map)
            "place": {
                "type": "text",
                "analyzer": "content_analyzer",
                "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
            },
            "country": {"type": "keyword"},
            "adm1": {"type": "keyword"},
            "geo_type": {"type": "integer"},
            "location": {"type": "geo_point"},

            # --- article content. For GDELT this is populated by a later
            # fetch/enrich stage (fields declared now so that doesn't require
            # a reindex); for RSS it's populated directly from the feed entry
            # at ingest time, when the feed provides it.
            "title": {"type": "text", "analyzer": "content_analyzer"},
            "content": {"type": "text", "analyzer": "content_analyzer"},
            "summary": {"type": "text", "analyzer": "content_analyzer"},
            "language": {"type": "keyword"},
            "fetch_status": {"type": "keyword"},  # ok | failed | pending
            "entities": {
                "properties": {
                    "persons": {"type": "keyword"},
                    "organizations": {"type": "keyword"},
                    "locations": {"type": "keyword"},
                }
            },

            # --- RSS-specific (source feed metadata; sparse on GDELT docs)
            "feed_name": {"type": "keyword"},
            "feed_category": {"type": "keyword"},
            "source_country": {"type": "keyword"},
            "author": {"type": "keyword"},
            "guid": {"type": "keyword"},
        },
    },
}


def get_client() -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=30, retry_on_timeout=True, max_retries=3)


def ensure_index(es: Elasticsearch, recreate: bool = False) -> None:
    if recreate and es.indices.exists(index=INDEX_NAME):
        logger.warning("Deleting existing index %s", INDEX_NAME)
        es.indices.delete(index=INDEX_NAME)
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, **INDEX_SETTINGS)
        logger.info("Created index %s", INDEX_NAME)
    else:
        logger.info("Index %s already exists", INDEX_NAME)


# ---------------------------------------------------------------------------
# row -> document
# ---------------------------------------------------------------------------

def _f(value: str | None) -> float | None:
    """GDELT writes empty strings for missing numerics."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _i(value: str | None) -> int | None:
    f = _f(value)
    return int(f) if f is not None else None


def _s(value: str | None) -> str | None:
    v = (value or "").strip()
    return v or None


def _date(value: str | None, fmt: str) -> str | None:
    v = _s(value)
    if not v:
        return None
    try:
        return datetime.strptime(v, fmt).replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


def row_to_doc(row: dict, interval_ts: str) -> dict | None:
    """Normalize one raw GDELT row. Returns None for rows we cannot use."""
    event_id = _s(row.get("GlobalEventID"))
    url = _s(row.get("SOURCEURL"))
    if not event_id or not url:
        return None

    lat = _f(row.get("ActionGeo_Lat"))
    lon = _f(row.get("ActionGeo_Long"))
    
    has_geo = lat is not None and lon is not None and not (lat == 0.0 and lon == 0.0)

    doc = {
        "type": "gdelt_event",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "interval_ts": interval_ts,
        "seen_at": _date(row.get("DATEADDED"), "%Y%m%d%H%M%S"),
        "content_date": _date(row.get("Day"), "%Y%m%d"),
        "url": url,
        "domain": urlparse(url).netloc or None,
        "event_code": _s(row.get("EventCode")),
        "event_base_code": _s(row.get("EventBaseCode")),
        "event_root_code": _s(row.get("EventRootCode")),
        "quad_class": _i(row.get("QuadClass")),
        "is_root_event": _s(row.get("IsRootEvent")) == "1",
        "goldstein": _f(row.get("GoldsteinScale")),
        "avg_tone": _f(row.get("AvgTone")),
        "num_mentions": _i(row.get("NumMentions")),
        "num_sources": _i(row.get("NumSources")),
        "num_articles": _i(row.get("NumArticles")),
        "actor1": _s(row.get("Actor1Name")),
        "actor2": _s(row.get("Actor2Name")),
        "actor1_country": _s(row.get("Actor1CountryCode")),
        "actor2_country": _s(row.get("Actor2CountryCode")),
        "place": _s(row.get("ActionGeo_Fullname")),
        "country": _s(row.get("ActionGeo_CountryCode")),
        "adm1": _s(row.get("ActionGeo_ADM1Code")),
        "geo_type": _i(row.get("ActionGeo_Type")),
        "location": {"lat": lat, "lon": lon} if has_geo else None,
        "fetch_status": "pending",
    }
    
    doc = {k: v for k, v in doc.items() if v is not None}
    doc["_id"] = event_id
    return doc


def iter_docs(path: Path, interval_ts: str) -> Iterator[dict]:
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        sample = fh.readline()
        fh.seek(0)

        has_header = sample.startswith("GlobalEventID")
        reader = csv.DictReader(
            fh,
            fieldnames=None if has_header else GDELT_EVENT_COLUMNS,
            delimiter="\t",
        )
        for row in reader:
            doc = row_to_doc(row, interval_ts)
            if doc is None:
                continue
            doc_id = doc.pop("_id")
            yield {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": doc_id,
                "_source": doc,
            }


def bulk_index_actions(actions: Iterable[dict], es: Elasticsearch | None = None) -> dict:
    """Shared bulk-load + error-reporting path for any source's pre-built
    bulk actions (GDELT rows, RSS entries, ...)."""
    es = es or get_client()
    ensure_index(es)

    success, errors = bulk(es, actions, chunk_size=500, raise_on_error=False, stats_only=False)
    es.indices.refresh(index=INDEX_NAME)

    if errors:
        for err in errors[:5]:
            logger.error("Bulk index error: %s", err)
    return {"indexed": success, "failed": len(errors)}


def bulk_index_file(path: str | Path, interval_ts: str, es: Elasticsearch | None = None) -> dict:
    stats = bulk_index_actions(iter_docs(Path(path), interval_ts), es)
    stats["interval_ts"] = interval_ts
    logger.info("Bulk load %s -> %s", path, stats)
    return stats


# ---------------------------------------------------------------------------
# RSS entry -> document
# ---------------------------------------------------------------------------

def rss_entry_to_doc(entry: dict, feed: dict, batch_ts: str) -> dict | None:
    """Normalize one feedparser entry. Returns None for entries we cannot use."""
    link = entry.get("link")
    guid = _s(entry.get("id")) or link
    if not guid or not link:
        return None

    # feedparser exposes full article body (e.g. RSS content:encoded, Atom
    # content) as a list; take the first value when a feed provides it.
    content_entries = entry.get("content")
    content = _s(content_entries[0].get("value")) if content_entries else None

    published_struct = entry.get("published_parsed") or entry.get("updated_parsed")
    content_date = (
        datetime(*published_struct[:6], tzinfo=timezone.utc).isoformat()
        if published_struct else None
    )

    doc = {
        "type": "rss_article",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "interval_ts": batch_ts,
        "content_date": content_date,
        "url": link,
        "domain": urlparse(link).netloc or None,
        "title": _s(entry.get("title")),
        "summary": _s(entry.get("summary")),
        "content": content,
        "language": _s(feed.get("language")),
        # a feed that ships full content:encoded needs no later enrich pass;
        # one that only gives a summary is left "pending" for that stage.
        "fetch_status": "ok" if content else "pending",
        "author": _s(entry.get("author")),
        "guid": guid,
        "feed_name": _s(feed.get("name")),
        "feed_category": _s(feed.get("category")),
        "source_country": _s(feed.get("country")),
    }
    doc = {k: v for k, v in doc.items() if v is not None}
    doc["_id"] = guid
    return doc


def bulk_index_rss_docs(docs: list[dict], es: Elasticsearch | None = None) -> dict:
    actions = (
        {"_op_type": "index", "_index": INDEX_NAME, "_id": d.pop("_id"), "_source": d}
        for d in docs
    )
    stats = bulk_index_actions(actions, es)
    logger.info("Bulk load %d RSS docs -> %s", len(docs), stats)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--recreate", action="store_true", help="Drop and recreate the index")
    p.add_argument("--load", help="Path to a filtered/raw GDELT TSV to bulk load")
    p.add_argument("--interval-ts", default="manual", help="Interval tag stored on each doc")
    p.add_argument("--count", action="store_true", help="Print current doc count")
    args = p.parse_args()

    es = get_client() 
    info = es.info()
    print(f"Connected to ES {info['version']['number']} at {ES_URL}")

    if args.recreate:
        ensure_index(es, recreate=True)
    else:
        ensure_index(es)

    if args.load:
        print(bulk_index_file(args.load, args.interval_ts, es))

    if args.count or args.load:
        print(f"{INDEX_NAME} doc count: {es.count(index=INDEX_NAME)['count']}")


if __name__ == "__main__":
    main()