from __future__ import annotations

from pydantic import BaseModel


class EntitySet(BaseModel):
    persons: list[str] = []
    organizations: list[str] = []
    locations: list[str] = []


class DocumentSummary(BaseModel):
    id: str
    type: str
    score: float | None = None
    content_date: str | None = None
    url: str | None = None
    domain: str | None = None
    title: str | None = None
    summary: str | None = None
    language: str | None = None
    fetch_status: str | None = None

    # RSS-only
    feed_name: str | None = None
    feed_category: str | None = None
    source_country: str | None = None
    author: str | None = None

    # GDELT-only
    event_code: str | None = None
    event_base_code: str | None = None
    event_root_code: str | None = None
    quad_class: int | None = None
    is_root_event: bool | None = None
    goldstein: float | None = None
    avg_tone: float | None = None
    num_mentions: int | None = None
    num_sources: int | None = None
    num_articles: int | None = None
    actor1: str | None = None
    actor2: str | None = None
    actor1_country: str | None = None
    actor2_country: str | None = None
    place: str | None = None
    country: str | None = None
    adm1: str | None = None

    entities: EntitySet | None = None
    entities_en: EntitySet | None = None
    highlight: dict[str, list[str]] | None = None


class DocumentDetail(DocumentSummary):
    content: str | None = None
    ingested_at: str | None = None
    seen_at: str | None = None
    enriched_at: str | None = None
    interval_ts: str | None = None
    guid: str | None = None
    geo_type: int | None = None
    location: dict | None = None


class SearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[DocumentSummary]


class FacetBucket(BaseModel):
    key: str
    count: int


class FacetsResponse(BaseModel):
    types: list[FacetBucket]
    languages: list[FacetBucket]
    feed_categories: list[FacetBucket]


class DateBucket(BaseModel):
    date: str
    gdelt_event: int
    rss_article: int


class ToneBucket(BaseModel):
    date: str
    avg_tone: float | None = None  # None when the bucket has zero gdelt_event docs


class StatsResponse(BaseModel):
    total_docs: int
    total_gdelt_events: int
    total_rss_articles: int
    volume_over_time: list[DateBucket]
    quad_class: list[FacetBucket]
    event_root_code: list[FacetBucket]
    languages: list[FacetBucket]
    avg_tone_over_time: list[ToneBucket]


class MapEventPoint(BaseModel):
    geohash: str
    lat: float
    lon: float
    count: int
    place: str | None = None
    country: str | None = None
    actor1: str | None = None
    actor2: str | None = None
    event_code: str | None = None
    quad_class: int | None = None
    content_date: str | None = None
    doc_id: str | None = None


class MapEventsResponse(BaseModel):
    total_matched: int
    bucket_count: int
    points: list[MapEventPoint]
