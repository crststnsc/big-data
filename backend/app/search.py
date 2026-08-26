from __future__ import annotations

ENTITY_FIELDS_BOOSTED = [
    ("entities_en.persons", 4),
    ("entities_en.organizations", 3),
    ("entities_en.locations", 2),
    ("entities.persons", 2),
    ("entities.organizations", 1.5),
    ("entities.locations", 1),
]

LIST_SOURCE_FIELDS = [
    "type", "content_date", "url", "domain", "title", "summary", "language",
    "fetch_status", "feed_name", "feed_category", "source_country", "author",
    "event_code", "event_base_code", "event_root_code", "quad_class", "is_root_event",
    "goldstein", "avg_tone", "num_mentions", "num_sources", "num_articles",
    "actor1", "actor2", "actor1_country", "actor2_country", "place", "country", "adm1",
    "entities", "entities_en",
]

HIGHLIGHT = {
    "pre_tags": ["<mark>"],
    "post_tags": ["</mark>"],
    "fields": {
        "title": {"number_of_fragments": 0},
        "summary": {"fragment_size": 150, "number_of_fragments": 2},
        "content": {"fragment_size": 150, "number_of_fragments": 2},
    },
}


def _entity_should(term: str, fields: list[tuple[str, float]]) -> list[dict]:
    value = f"*{term.strip().lower()}*"
    return [
        {"wildcard": {field: {"value": value, "case_insensitive": True, "boost": boost}}}
        for field, boost in fields
    ]


def build_query(q: str | None, filters: dict) -> dict:
    must: list[dict] = []
    if q and q.strip():
        should = [
            {
                "multi_match": {
                    "query": q,
                    "type": "most_fields",
                    "fields": ["title^3", "summary^2", "content", "actor1^1.5", "actor2^1.5", "place"],
                    "fuzziness": "AUTO",
                }
            },
            *_entity_should(q, ENTITY_FIELDS_BOOSTED),
        ]
        must.append({"bool": {"should": should, "minimum_should_match": 1}})

    filter_clauses: list[dict] = []
    if filters.get("type"):
        filter_clauses.append({"terms": {"type": filters["type"]}})
    if filters.get("language"):
        filter_clauses.append({"terms": {"language": filters["language"]}})
    if filters.get("feed_category"):
        filter_clauses.append({"terms": {"feed_category": filters["feed_category"]}})
    if filters.get("date_from") or filters.get("date_to"):
        rng = {}
        if filters.get("date_from"):
            rng["gte"] = filters["date_from"]
        if filters.get("date_to"):
            rng["lte"] = filters["date_to"]
        filter_clauses.append({"range": {"content_date": rng}})
    if filters.get("entity"):
        filter_clauses.append(
            {"bool": {"should": _entity_should(filters["entity"], ENTITY_FIELDS_BOOSTED), "minimum_should_match": 1}}
        )

    if not must and not filter_clauses:
        return {"match_all": {}}
    return {"bool": {"must": must, "filter": filter_clauses}}


def build_sort(sort_param: str | None, q: str | None) -> list:
    effective = sort_param or ("relevance" if q and q.strip() else "date_desc")
    if effective == "date_asc":
        return [{"content_date": {"order": "asc", "missing": "_last"}}]
    if effective == "date_desc":
        return [{"content_date": {"order": "desc", "missing": "_last"}}]
    return ["_score", {"content_date": {"order": "desc", "missing": "_last"}}]


def hit_to_summary(hit: dict) -> dict:
    return {
        "id": hit["_id"],
        "score": hit.get("_score"),
        "highlight": hit.get("highlight"),
        **hit["_source"],
    }
