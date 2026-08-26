from __future__ import annotations

import argparse
import csv
import logging
import os
from datetime import datetime, timezone
from html.parser import HTMLParser
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
            "type": {"type": "keyword"},
            "ingested_at": {"type": "date"},
            "interval_ts": {"type": "keyword"},

            "seen_at": {"type": "date"},        # GDELT: DATEADDED
            "content_date": {"type": "date"},   # GDELT: Day | RSS: entry published/updated date

            "url": {"type": "keyword"},
            "domain": {"type": "keyword"},

            "event_code": {"type": "keyword"},
            "event_base_code": {"type": "keyword"},
            "event_root_code": {"type": "keyword"},
            "quad_class": {"type": "integer"},
            "is_root_event": {"type": "boolean"},

            "goldstein": {"type": "float"},
            "avg_tone": {"type": "float"},
            "num_mentions": {"type": "integer"},
            "num_sources": {"type": "integer"},
            "num_articles": {"type": "integer"},

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

            "place": {
                "type": "text",
                "analyzer": "content_analyzer",
                "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
            },
            "country": {"type": "keyword"},
            "adm1": {"type": "keyword"},
            "geo_type": {"type": "integer"},
            "location": {"type": "geo_point"},

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
            
            "entities_en": {
                "properties": {
                    "persons": {"type": "keyword"},
                    "organizations": {"type": "keyword"},
                    "locations": {"type": "keyword"},
                }
            },
            
            "enriched_at": {"type": "date"},

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
        es.indices.put_mapping(index=INDEX_NAME, properties=INDEX_SETTINGS["mappings"]["properties"])
        logger.info("Index %s already exists; mapping is up to date", INDEX_NAME)


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


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def text(self) -> str:
        return " ".join(self._parts)


def _strip_html(value: str | None) -> str | None:
    v = _s(value)
    if not v:
        return None
    parser = _TextExtractor()
    parser.feed(v)
    text = " ".join(parser.text().split())
    return text or None


def row_to_doc(row: dict, interval_ts: str) -> dict | None:
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
    link = entry.get("link")
    guid = _s(entry.get("id")) or link
    if not guid or not link:
        return None

    content_entries = entry.get("content")
    content = _strip_html(content_entries[0].get("value")) if content_entries else None

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
        "summary": _strip_html(entry.get("summary")),
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


def backfill_strip_html(es: Elasticsearch | None = None) -> dict:
    from elasticsearch.helpers import scan

    es = es or get_client()
    hits = scan(
        es, index=INDEX_NAME, query={"query": {"term": {"type": "rss_article"}}},
        _source=["content", "summary", "title", "language", "entities", "fetch_status"],
    )

    counts = {"scanned": 0, "text_changed": 0, "entities_reextracted": 0}

    def actions():
        for hit in hits:
            counts["scanned"] += 1
            src = hit["_source"]
            clean_content = _strip_html(src.get("content"))
            clean_summary = _strip_html(src.get("summary"))

            doc_update = {}
            if clean_content != src.get("content"):
                doc_update["content"] = clean_content
            if clean_summary != src.get("summary"):
                doc_update["summary"] = clean_summary

            if not clean_content and src.get("fetch_status") == "ok":
                doc_update["fetch_status"] = "pending"

            if not doc_update:
                continue
            counts["text_changed"] += 1

            if src.get("entities") is not None:
                from nlp_enrich import extract_entities, translate_entities

                lang = src.get("language")
                text = clean_content or clean_summary or src.get("title") or ""
                entities = extract_entities(text, lang) if lang and text else None
                if entities is not None:
                    doc_update["entities"] = entities
                    doc_update["entities_en"] = translate_entities(entities, lang)
                    counts["entities_reextracted"] += 1

            yield {"_op_type": "update", "_index": INDEX_NAME, "_id": hit["_id"], "doc": doc_update}

    stats = bulk_index_actions(actions(), es)
    stats.update(counts)
    logger.info("HTML backfill: %s", stats)
    return stats


def backfill_fetch_status(es: Elasticsearch | None = None) -> dict:
    es = es or get_client()

    to_ok = es.update_by_query(
        index=INDEX_NAME,
        query={
            "bool": {
                "filter": [{"exists": {"field": "enriched_at"}}, {"exists": {"field": "entities"}}],
                "must_not": [{"term": {"fetch_status": "ok"}}],
            }
        },
        script={"source": "ctx._source.fetch_status = 'ok'"},
    )
    to_failed = es.update_by_query(
        index=INDEX_NAME,
        query={
            "bool": {
                "filter": [{"term": {"type": "rss_article"}}, {"exists": {"field": "enriched_at"}}],
                "must_not": [
                    {"exists": {"field": "entities"}},
                    {"terms": {"fetch_status": ["ok", "failed"]}},
                ],
            }
        },
        script={"source": "ctx._source.fetch_status = 'failed'"},
    )
    stats = {"marked_ok": to_ok["updated"], "marked_failed": to_failed["updated"]}
    logger.info("fetch_status backfill: %s", stats)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--recreate", action="store_true", help="Drop and recreate the index")
    p.add_argument("--load", help="Path to a filtered/raw GDELT TSV to bulk load")
    p.add_argument("--interval-ts", default="manual", help="Interval tag stored on each doc")
    p.add_argument("--count", action="store_true", help="Print current doc count")
    p.add_argument(
        "--backfill-html", action="store_true",
        help="Strip HTML from already-stored RSS content/summary and re-extract entities where affected",
    )
    p.add_argument(
        "--backfill-fetch-status", action="store_true",
        help="Bring fetch_status in line with already-stored entities for previously-enriched docs",
    )
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

    if args.backfill_html:
        print(backfill_strip_html(es))

    if args.backfill_fetch_status:
        print(backfill_fetch_status(es))

    if args.count or args.load:
        print(f"{INDEX_NAME} doc count: {es.count(index=INDEX_NAME)['count']}")


if __name__ == "__main__":
    main()