# Architecture & Design Rationale

This document explains **every piece of code in this project, and why it exists** — not just what it does. It's written for you as the developer maintaining this codebase (and, likely, defending it as a thesis project), so it goes deep on the *why* behind each decision, including the Elasticsearch query parameters, which are easy to copy-paste without understanding.

## The big picture

Three layers, built in this order:

1. **Ingestion pipeline** (`dags/`, run by Apache Airflow) — pulls data from two public sources (GDELT's global event feed, ~190 RSS news feeds), normalizes both into one shared schema, and indexes them into Elasticsearch. A third DAG enriches already-indexed documents with extracted entities (people/orgs/locations), translated to English for cross-lingual search.
2. **Backend API** (`backend/`, FastAPI) — a read-only query layer over that Elasticsearch index. Nothing here writes to the index; it only searches, aggregates, and looks up by ID.
3. **Frontend** (`frontend/`, React + Vite) — the UI: full-text search with filters, a document detail view, a stats dashboard, and a geographic map.

All three layers talk to the **same Elasticsearch index**, `news_events`, which is the single source of truth. Its schema is defined once, in `dags/news_es.py`, and referenced (not imported — see below) by the backend.

```
GDELT (public HTTP)  ─┐
                       ├─→ [Airflow DAGs] ─→ Elasticsearch "news_events" ←─ [FastAPI backend] ←─ [React frontend]
~190 RSS feeds       ─┘         ↑
                                 └─ entity_enrichment DAG reads+updates docs already in the index
```

Two document "types" share one index (`gdelt_event`, `rss_article`), distinguished by a `type` keyword field. This was a deliberate choice (see `dags/news_es.py` below) so a single search/dashboard/map can span both sources instead of maintaining two indices and merging results in the application layer.

---

# Part 1 — The ingestion pipeline (`dags/`)

## `dags/news_es.py` — the shared schema and indexing library

This is the most important file in the project: it's the **only place the Elasticsearch mapping is defined**, and both ingestion DAGs and the enrichment DAG import from it. It is deliberately *not* an Airflow DAG itself — it's a plain Python module with a `main()` for command-line use (`python news_es.py --recreate`, `--load`, `--backfill-html`, etc.).

### Connection config

```python
ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
INDEX_NAME = os.environ.get("NEWS_INDEX", "news_events")

def get_client() -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=30, retry_on_timeout=True, max_retries=3)
```

- Both are env-var overridable with sane local defaults, so the same code runs against a local dev ES or a different one without editing source.
- `request_timeout=30`: ES bulk operations on a few hundred docs can occasionally take a few seconds under load; 30s avoids spurious timeouts without hanging forever on a truly dead cluster.
- `retry_on_timeout=True, max_retries=3`: the ES client will itself retry a request that times out, before the caller (an Airflow task) has to fail and go through Airflow's own (much slower, minutes-scale) retry/backoff.

### `INDEX_SETTINGS` — the mapping, field by field

```python
"settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": {"analyzer": {"content_analyzer": {
        "type": "custom", "tokenizer": "standard", "filter": ["lowercase", "asciifolding"],
    }}},
},
```

- **1 shard, 0 replicas**: this is a single-node local dev cluster with a modest document count (tens of thousands, not millions). More shards would only add coordination overhead with nothing to parallelize across; replicas exist for failover across multiple nodes, which doesn't apply here.
- **`content_analyzer`**: a custom analyzer applied to every free-text field (`title`, `content`, `summary`, `actor1`, `actor2`, `place`). It's `standard` tokenizer (splits on word boundaries, handles punctuation reasonably across languages) plus two filters:
  - `lowercase` — so a search for "Putin" matches indexed "putin" and vice versa.
  - `asciifolding` — strips diacritics (e.g. "café" → "cafe", "İstanbul" → "istanbul"), so an English keyboard search still matches accented text. Given the RSS side ingests 15 languages, several with diacritics (French, Romanian, Polish, Portuguese...), this materially widens what a plain-ASCII query can match.

```python
"mappings": {
    "dynamic": "strict",
    "properties": { ... }
}
```

**`"dynamic": "strict"`** is the single most consequential setting in this file: it means **any field not explicitly declared here is rejected on write**, and querying an undeclared field name is also an error. This was a deliberate choice, not a default left alone:

- It forces every new field (this happened repeatedly — `entities_en`, `enriched_at`, `feed_name`, etc. were all added *after* the index already existed) to be a conscious mapping decision, not something Elasticsearch guesses the type for on first sight (which is how you accidentally get a `text` field that should have been a `keyword`, or a numeric field that gets typed as a string because the first doc happened to have one).
- The tradeoff: every time a new field is introduced in code, `ensure_index()` (below) has to be told about it, or every subsequent write fails. This happened in practice with `entities_en`/`enriched_at` — see the "Bugs found and fixed" section for the migration mechanism this required.

Now the fields themselves, grouped as they are in the file:

| Field | ES type | Why |
|---|---|---|
| `type` | `keyword` | The discriminator (`"gdelt_event"` / `"rss_article"`). `keyword` (not `text`) because it's never full-text searched, only filtered/aggregated on exact value — `keyword` fields are stored as a single unanalyzed token, which is both correct (no tokenization would ever make sense here) and faster for `term`/`terms` queries and aggregations. |
| `ingested_at` | `date` | When *our* pipeline wrote the doc — provenance, not the content's own date. |
| `interval_ts` | `keyword` | The batch/run identifier (GDELT's 15-min file timestamp, or the RSS DAG's hourly run timestamp). Debugging aid: "which pipeline run produced this doc." |
| `seen_at` | `date` | GDELT's own `DATEADDED` field (when GDELT's crawler recorded the event) — GDELT-only, absent on RSS docs. |
| `content_date` | `date` | **Shared** across both types on purpose: GDELT's `Day` (when the event happened) *or* an RSS entry's published/updated date. Originally this field was called `event_date` (GDELT-only naming) and got renamed to `content_date` specifically so a single field could serve as "the" display/sort date for both content types — see the Part 4 renaming note. |
| `url`, `domain` | `keyword` | Exact-match/filterable, never searched as text. `domain` is `urlparse(url).netloc`, precomputed at ingest time rather than derived at query time, since it's cheap to compute once and expensive to compute per-query across every result. |
| `event_code`, `event_base_code`, `event_root_code` | `keyword` | GDELT's CAMEO taxonomy codes (3-digit / 3-digit-root / 2-digit-root). Kept as strings deliberately — CAMEO codes can be zero-padded (`"04"`, `"026"`) and storing them numerically would either lose the padding or require re-padding on display; GDELT's own codebook explicitly recommends string storage for this reason. |
| `quad_class` | `integer` | 1–4, GDELT's top-level classification (Verbal/Material × Cooperation/Conflict). A real integer (not `keyword`) because the dashboard needs a `terms` agg on it either way, and integer comparisons/sorting are marginally cheaper — though the real reason is simpler: it's numerically meaningful (there's no reason to treat "2" as an opaque string when it's fundamentally an ordinal code). |
| `is_root_event` | `boolean` | GDELT flag for "found in the lead paragraph" (a rough importance signal). |
| `goldstein`, `avg_tone` | `float` | Genuinely continuous numeric scores (-10..10 and -100..100 respectively) — need range queries and averaging (used directly in the dashboard's tone-over-time chart), so `float` is correct, not `keyword`. |
| `num_mentions`, `num_sources`, `num_articles` | `integer` | Simple counts. |
| `actor1`, `actor2` | `text` **+** `actor1.raw`/`actor2.raw` `keyword` (`ignore_above: 256`) | This is a **multi-field**: the same value indexed two ways. The `text` version (with `content_analyzer`) is what free-text search matches against (so searching "putin" finds an actor named "Vladimir Putin"). The `.raw` `keyword` sub-field is for exact-match/aggregation use cases (e.g., "show me all events with actor1 exactly = 'UNITED STATES'") that the analyzed version can't do reliably (tokenized "united states" would also match "states united" in some query types, and can't be used in a `terms` agg meaningfully). `ignore_above: 256` means values longer than 256 chars aren't indexed as the keyword (they'd almost certainly be garbage/non-actor text at that length) — a defensive cap, not a functional requirement. **Note**: the `.raw` sub-fields exist in the mapping but the backend doesn't currently query them — they were added for completeness/future use, not because a current feature needs them. |
| `actor1_country`, `actor2_country` | `keyword` | 3-letter CAMEO country codes — exact-match only, never free text. |
| `place` | `text` + `place.raw` `keyword` | Same multi-field reasoning as actors — GDELT's `ActionGeo_Fullname` (e.g. "Reutlingen, Baden-Württemberg, Germany"), searchable as text, filterable as an exact value via `.raw`. |
| `country`, `adm1` | `keyword` | GDELT's `ActionGeo_CountryCode`/`ActionGeo_ADM1Code` — administrative codes, always exact-match. |
| `geo_type` | `integer` | GDELT's geo-resolution code (1=country, 2=US state, 3=US city, 4=world city, 5=world state) — kept for potential future filtering ("only landmark-level events"), not currently used by any query. |
| `location` | `geo_point` | The field the map is built on. `geo_point` is Elasticsearch's dedicated lat/lon type — it's what makes `geohash_grid`, `geo_centroid`, and geo-distance queries possible at all; storing lat/lon as two plain floats would not support any of that. |
| `title`, `content`, `summary` | `text` (`content_analyzer`) | The actual searchable article text. All three exist on both types in principle, but are sparse in practice — see the RSS ingestion section for exactly when each is populated. |
| `language` | `keyword` | RSS-only (from the feed's own metadata in `feeds.clean.json`) — the language the *spaCy NER pass* should use, not a general-purpose i18n field. |
| `fetch_status` | `keyword` | `"ok"` \| `"pending"` \| `"failed"` — a state-machine field explained fully in the enrichment section below; its meaning evolved twice during development (see Part 4). |
| `entities` / `entities_en` | nested `properties` object: `persons`/`organizations`/`locations`, each `keyword` array | **Not** `nested` type (the ES feature) — a plain object with array-of-keyword sub-fields. This matters: a real `nested` field would let you query "a doc where the *same* person appears in a specific role," but here `persons`/`organizations`/`locations` are just independent tag lists per doc, so the extra complexity (and query overhead) of `nested` buys nothing. `entities` holds the extraction in whatever language the source document is in; `entities_en` is the same shape, machine-translated (or a straight copy, if already English/CAMEO) — see `entity_enrichment.py` below for why two fields exist at all. |
| `enriched_at` | `date` | Not descriptive metadata — this is a **worker queue marker**. Its *absence* is what the enrichment DAG's query filters on (`must_not: {"exists": {"field": "enriched_at"}}`) to find unprocessed docs. Once set, a doc is never rescanned by that DAG again (barring an explicit backfill). |
| `feed_name`, `feed_category`, `source_country`, `author`, `guid` | `keyword` | RSS-only metadata straight from `feeds.clean.json` / the feed entry. All exact-match/filter fields, never searched as text (a feed's *name* isn't something a user free-text-searches for the way they'd search article content). |

### `ensure_index()` — additive migration

```python
def ensure_index(es, recreate=False):
    if recreate and es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, **INDEX_SETTINGS)
    else:
        es.indices.put_mapping(index=INDEX_NAME, properties=INDEX_SETTINGS["mappings"]["properties"])
```

Because the mapping is `dynamic: strict`, adding a new field to `INDEX_SETTINGS` in code does nothing to an *already-existing* index until something calls `put_mapping` with the new field definitions. `PUT _mapping` in Elasticsearch is **additive-only** by design — it can add new field definitions to an existing mapping, but can't change or remove an existing field's type (that requires a reindex, which this project has never needed since no field's type was ever changed after creation, only new fields added). So `ensure_index()`, called at the start of every ingestion/enrichment task, re-applies the *entire current* `properties` dict every time: for fields that already exist with the same definition, this is a documented no-op; for new fields, it merges them in. This is what let `entities_en`, `enriched_at`, `feed_name` etc. get added to a live index with real data already in it, with zero downtime and no reindex — every task run since has just quietly kept the mapping current.

### Doc-shaping functions

**`row_to_doc(row, interval_ts)`** — turns one raw GDELT CSV row into a document:

- Returns `None` if there's no `GlobalEventID` or `SOURCEURL` — both are treated as required; a row without them can't be meaningfully indexed (no stable ID, no linkable source).
- `_f`/`_i`/`_s`/`_date` are small parsing helpers: GDELT's raw CSV represents missing numerics as empty strings, not nulls, so `_f` explicitly checks for `""` before attempting `float()`. `_s` strips whitespace and turns empty-after-strip into `None`. `_date` parses GDELT's compact date formats (`YYYYMMDD`, `YYYYMMDDHHMMSS`) into ISO 8601, which is what Elasticsearch's `date` type expects by default.
- **The null-island check**: `has_geo = lat is not None and lon is not None and not (lat == 0.0 and lon == 0.0)`. GDELT uses `(0, 0)` as a placeholder for "no location resolved," not an actual location — `(0, 0)` is a real point in the Gulf of Guinea off the coast of West Africa, and without this check every unresolved event would incorrectly pin a marker there on the map, visually dominating the actual data.
- **The person/org bucketing decision**: `actor1`/`actor2` are put into `organizations` in `entity_enrichment.py` (not here directly, but the design decision belongs with this data), never `persons`. GDELT's CAMEO actor codes conflate individuals, countries, and institutional roles (a head of state, "UNITED STATES," and "POLICE" are all valid `Actor1Name` values) with no reliable structured signal in the row to distinguish "this is a named individual" from "this is a country or role" without deeper CAMEO type-code parsing this pipeline doesn't do. Bucketing everything as `organizations` was chosen as the honest simplification — leaving `persons` empty for GDELT rather than guessing wrong.
- The dict is filtered to drop `None` values before indexing (`{k: v for k, v in doc.items() if v is not None}`) — this keeps documents small and means an `exists` query on a field is a meaningful "was this ever populated" check, not always-true because every doc has every key with a null value.

**`rss_entry_to_doc(entry, feed, batch_ts)`** — turns one `feedparser` entry into a document:

- `guid = _s(entry.get("id")) or link` — prefers the feed's own GUID (a stable identifier some feeds provide independent of the URL, useful if a URL ever changes) but falls back to the article link, which is always present if the entry is usable at all.
- **Full content vs. summary**: `feedparser` exposes a full article body (RSS `content:encoded` or Atom `content`) as a list under `entry.content` when a feed provides it; most don't, and only give a short `summary`/`description`. This directly drives `fetch_status`: `"ok"` if the feed gave full content, `"pending"` if only a summary (meaning a future, not-yet-built full-page-scrape stage could still improve this doc).
- **`_strip_html`**: both `content` and `summary` are passed through a stdlib `html.parser.HTMLParser`-based stripper before storage. This was added *after* a real bug was found (see Part 4) — RSS `content:encoded` commonly embeds raw HTML (`<p>`, `<img>`, `<a>`) that, left unstripped, both pollutes full-text search relevance and corrupts spaCy's entity extraction (a stray `<img.../>` tag glued onto the front of a real name). No external HTML-parsing library was added for this — Python's built-in `HTMLParser`, subclassed to just collect `handle_data` text, was enough and avoided one more dependency in an already dependency-heavy project (spaCy + argos-translate already pulled in a full PyTorch stack).
- `doc["_id"] = guid` — the RSS article's natural key is its own GUID/link, exactly mirroring GDELT using `GlobalEventID`. Using a natural key (not an auto-generated ES `_id`) means re-ingesting the same article twice **upserts** rather than duplicates — important since the same recent articles reappear on every poll of a feed until they age out of it.

### Bulk indexing helpers

```python
def bulk_index_actions(actions, es=None):
    es = es or get_client()
    ensure_index(es)
    success, errors = bulk(es, actions, chunk_size=500, raise_on_error=False, stats_only=False)
    es.indices.refresh(index=INDEX_NAME)
    ...
    return {"indexed": success, "failed": len(errors)}
```

This is the one function every other bulk operation in the whole project funnels through (GDELT loads, RSS loads, and both enrichment tasks' partial updates) — it doesn't care whether the actions are `"index"` (full document write) or `"update"` (partial merge) operations, since that's decided by whoever builds the `actions` iterable; it only owns the generic "run it, log errors, report counts" concern. `chunk_size=500` batches the bulk request into groups of 500 actions rather than one giant request (avoids one oversized HTTP payload) or 500 tiny requests (avoids per-request overhead dominating). `raise_on_error=False, stats_only=False` means a handful of bad documents in a batch of thousands doesn't abort the whole batch — the helper's caller gets a success count and a list of the actual errors to log, and everything else still gets indexed. `es.indices.refresh()` forces newly-written documents to become immediately searchable (by default Elasticsearch refreshes on a ~1-second interval, which is usually fine, but an explicit refresh here means "how many rows did this DAG run index" logging and any immediate verification queries see accurate numbers right away rather than a stale count).

`bulk_index_file`/`bulk_index_rss_docs` are thin, source-specific wrappers around this: they only differ in *what* they iterate over (a CSV file's rows vs. an in-memory list of already-built RSS docs) and both end up calling `bulk_index_actions`.

### The two backfill functions

These exist because bugs were found in already-indexed data *after* the ingestion code was fixed going forward — fixing the code doesn't retroactively fix documents already written. Both are exposed as CLI flags (`--backfill-html`, `--backfill-fetch-status`) rather than being automatic, since they're one-off maintenance operations, not something that should silently re-run on every deploy.

- **`backfill_strip_html`**: re-applies `_strip_html` to every already-indexed RSS doc's stored `content`/`summary` (idempotent — a doc ingested after the fix is already clean, so re-stripping it is a no-op and the function correctly detects "nothing changed" and skips it). For any doc that had *already* been entity-enriched before the fix (so its `entities` were extracted from dirty, HTML-contaminated text), it re-runs `extract_entities`/`translate_entities` on the now-clean text — because the corruption wasn't just in the stored text field, it had already propagated into extracted entity values too (a `/><p>Sabine Gross` artifact, not just a raw HTML tag sitting in `content`). It also catches a second-order effect: if a `content:encoded` value turns out to be *pure* markup (e.g. a cartoon/vignette post that's just an `<img>` tag, no real text), stripping it leaves nothing — and a `fetch_status` of `"ok"` (set at ingest time based on *raw* content being present) becomes wrong; the function downgrades it to `"pending"` in that specific case.
- **`backfill_fetch_status`**: a narrower, later fix using `update_by_query` (an ES-native "run this script against every matching doc server-side" operation — much more efficient than scanning and reissuing bulk updates in Python when the update logic is this simple) rather than the scan+bulk pattern the other functions use, because this one needs no per-document computation, just a conditional field flip based on other already-stored fields. It exists because `entity_enrichment.py`'s two tasks didn't originally set `fetch_status` at all — only later did the semantics get extended so `fetch_status` also reflects "did entity enrichment succeed," and every doc enriched *before* that change needed the same correction applied retroactively.

---

## `dags/gdelt_export.py` — GDELT ingestion DAG

Three tasks: `compute_ts` → `download` → `extract_csv` → `index_to_es`.

**Scheduling** — `schedule="*/15 * * * *"` relies on a specific, non-obvious property of Airflow's cron scheduling: a DAG run for the interval `[12:00, 12:15)` doesn't execute *at* 12:00, it executes *after* the interval closes, i.e. at 12:15, with `logical_date=12:00`. GDELT publishes each 15-minute file a few minutes after its own boundary. So by the time this DAG's run for `logical_date=12:00` actually executes (at 12:15 wall-clock), the file it needs has had a full 15 minutes to appear — no sensor, no artificial delay, no polling-with-retry-hoping-the-file-shows-up. This is why `gdelt_ts_for()` simply floors `logical_date` to its own 15-minute boundary rather than doing anything more clever with the *execution* time.

**`download`**: a plain `requests.get` with a 404 specifically translated into an `AirflowException` — Airflow's task-level `retries=3` + `retry_delay=timedelta(minutes=2)` + `retry_exponential_backoff=True` (in `default_args`) then absorb the case where GDELT's publish lag is slightly longer than usual on a given run, without any bespoke retry logic in the task itself.

**`extract_csv`**: does a raw `shutil.copyfileobj` byte copy of the CSV out of the zip, not a parse-and-rewrite. GDELT's own export files ship with **no header row** — this task's only value-add is prepending one (`GDELT_EVENT_COLUMNS`, transcribed from the official codebook) so the file downstream is self-describing. Since nothing is being filtered or transformed at this stage, parsing every row into a `csv.DictReader`/`DictWriter` round-trip just to write the same bytes back out would be pure overhead — a straight byte copy is both simpler and faster.

**`index_to_es`**: imports `bulk_index_file` from `news_es` *inside* the task function, not at module level. This is deliberate: Airflow parses every DAG file's top-level code repeatedly (to build the UI's DAG list), so anything with real cost — like importing `elasticsearch`, or worse, `spacy`/`argostranslate` in other DAGs — is pushed into the function body so it only happens when the task actually runs, not every time the scheduler re-parses the file.

---

## `dags/rss_export.py` — RSS ingestion DAG

One task: `fetch_and_index`. This is deliberately **not** built with Airflow's dynamic task mapping (`.expand()`), which would create one task instance per feed (~190 of them, every run). That was an explicit tradeoff, not an oversight — dynamic mapping gives per-feed retry/failure isolation and per-feed visibility in the Airflow UI "for free," but 190 task instances per run, every hour, is a lot of scheduler/UI overhead for what's fundamentally 190 short HTTP fetches. A `ThreadPoolExecutor` inside one task gets the same real concurrency (`MAX_WORKERS = 20` fetches in flight at once) with a fraction of the bookkeeping. The cost, accepted deliberately: a single feed's failure is only visible in this one task's logs (not its own task instance), and a task-level retry re-fetches *every* feed, not just the ones that failed.

`fetch_feed(feed)` returns `(feed, entries, error)` rather than raising — this lets the `as_completed()` loop in `fetch_and_index` tally `feeds_ok`/`feeds_failed` counts and keep going, instead of one bad feed (a dead domain, a timeout, a malformed response) aborting the whole batch.

- `REQUEST_TIMEOUT = 15`: short enough that one slow/hanging feed doesn't dominate the run's wall-clock time (with 20 workers processing ~190 feeds, a worst case of many simultaneous 15s timeouts is still bounded).
- `USER_AGENT`: a browser-like string. Many news sites' RSS endpoints reject or rate-limit the default `python-requests/x.y` user agent; a plausible UA reduces spurious 403s.
- `batch_ts = logical_date.strftime(...)`: this is the value stored in every doc's `interval_ts` for this run — used purely for debugging/traceability ("which run produced this doc"), unrelated to GDELT's identically-shaped but functionally different `file_ts`.

---

## `dags/nlp_enrich.py` — language-aware NER + translation

Not a DAG — a plain library module imported by `entity_enrichment.py`.

**`SPACY_MODEL_BY_LANG`**: spaCy ships officially-trained pipelines for only some languages. Of the 15 languages present in `feeds.clean.json`, 12 have one (mapped here); Bengali, Farsi, and Indonesian don't. `UNSUPPORTED_LANGUAGES` names those three explicitly, and `get_spacy_model`/`extract_entities` return `None` for them rather than raising — a deliberate scope decision to skip NER for 3 languages rather than integrate a second NER framework just for them.

**Model loading**: `spacy.load(model_name, exclude=["parser", "tagger", "lemmatizer", "attribute_ruler"])` — spaCy's default pipeline does dependency parsing, POS tagging, and lemmatization in addition to NER; none of that output is used here, so excluding those components measurably speeds up processing (NER is the only stage actually run per document) with no loss of functionality. Models are cached in a module-level dict (`_spacy_models`) keyed by language, loaded once per process and reused — loading a spaCy model is a genuinely expensive operation (deserializing weights), so doing it once per language per Airflow worker process (not once per document) is what makes processing thousands of documents per run tractable.

**Label mapping**: `PERSON_LABELS = {"PERSON", "PER"}`, `ORG_LABELS = {"ORG"}`, `LOCATION_LABELS = {"GPE", "LOC"}`. This works across all 12 languages because of a specific, non-obvious spaCy property: **entity label *names* are standardized in English across every language pipeline** — a French or Japanese model still emits the label string `"PERSON"`, not a French/Japanese word for it. The two variants per bucket (`PERSON`/`PER`, `GPE`/`LOC`) exist because different training corpora used slightly different label schemes (English's `en_core_web_sm` uses the richer OntoNotes scheme with `PERSON`/`GPE`/`LOC` as separate categories; several of the other "news" models use a simpler CoNLL-style `PER`/`LOC` scheme) — covering both variants means one mapping works everywhere instead of needing a per-language lookup table.

**`translate_entities`**: English sources pass through unchanged (no translation call at all — a name is already in English). For everything else, `argostranslate.translate.translate(text, lang, "en")` runs per extracted entity string, with an in-process cache (`_translation_cache`, keyed by `(lang, text)`) so a recurring entity (a country name, a frequently-mentioned public figure) is translated once per run, not once per occurrence. A failed translation falls back to the original string rather than dropping the entity entirely — a name search is still useful even untranslated, and this is strictly better than silently losing data. **Why argos-translate specifically**: it's a fully offline, local translation library with no API key and no per-character cost — consistent with every other piece of infrastructure in this project (self-hosted Elasticsearch, local spaCy models) being free and self-contained, with no external paid dependency anywhere. The tradeoff, discovered during setup: `argostranslate` pulls in a full PyTorch/CUDA stack via its `stanza` dependency (~5GB installed, most of it unused GPU tooling on a CPU-only box) — heavier than expected for "translate a few short strings," but accepted since disk space wasn't a constraint and no lighter fully-offline alternative with the same language coverage was readily available.

---

## `dags/entity_enrichment.py` — the enrichment DAG

Two independent tasks in one DAG, run every 30 minutes: `enrich_gdelt` and `enrich_rss`. They're independent (no data dependency between them) so Airflow runs them in parallel; they're two tasks rather than one because they do fundamentally different work — one is a cheap, deterministic field remap, the other is CPU-bound NER across a dozen languages plus network-free translation calls, and keeping them separate means a slow RSS run doesn't block GDELT enrichment from completing, and either can retry independently.

**Why GDELT bypasses spaCy entirely**: GDELT's actor/place fields are already structured, CAMEO-normalized data (see `row_to_doc` above) — running NER on the *article text* to re-derive what's already sitting in `actor1`/`actor2`/`place` would be redundant work producing a strictly worse result (NER is probabilistic; the CAMEO fields are exact). `enrich_gdelt` just reads three already-indexed fields and reshapes them into the `entities` structure.

**The scan query, for both tasks**:
```python
query = {"bool": {
    "filter": [{"term": {"type": "gdelt_event"}}],
    "must_not": [{"exists": {"field": "enriched_at"}}],
}}
hits = scan(es, index=INDEX_NAME, query={"query": query}, _source=[...], size=1000)
```
`elasticsearch.helpers.scan` is the standard tool for "iterate every matching document," using Elasticsearch's scroll API under the hood rather than paginating with `from`/`size` (which becomes slow and eventually errors past 10,000 results — irrelevant for a one-off search, very relevant for "walk the entire backlog"). The `must_not: exists(enriched_at)` clause is the queue: once a doc's `enriched_at` gets set, it drops out of every future scan, so re-running this DAG is always safe and never redoes work. `_source` is restricted to only the fields each task actually reads (`actor1`/`actor2`/`place` for GDELT; `content`/`summary`/`title`/`language`/`fetch_status` for RSS) — ES has to transfer and deserialize less data per hit when the full document body isn't needed.

**`MAX_DOCS_PER_RUN = 2000`**: a self-imposed cap, checked inside the generator that yields bulk actions (`if counts["processed"] >= MAX_DOCS_PER_RUN: break`). Without it, a large backlog (this happened in practice — over 10,000 unenriched docs accumulated before this DAG existed) would mean one task run tries to NER-process everything in a single, potentially very long-running task. Capping at 2000 means a big backlog gets worked off incrementally, 2000 docs every 30 minutes, rather than risking one run that never finishes or times out midway with nothing committed (bulk actions are yielded lazily and sent in chunks of 500, so even a capped run's partial progress is saved to ES incrementally, not held in memory until the end).

**The `fetch_status` state machine** (see Part 4 for how this evolved): both tasks set `fetch_status: "ok"` whenever they actually produce entities — this is a second, independent signal alongside `enriched_at` ("has this been processed") meaning "did processing succeed at getting something useful." For RSS, when extraction can't happen (unsupported language, or no text at all), the doc still gets `enriched_at` set (so it's never rescanned) but `fetch_status` only moves to `"failed"` **if it wasn't already `"ok"`** — a document that already had full article content (from `content:encoded` at ingest time) but happens to be in an unsupported language keeps its `"ok"` status, since the content-fetch itself genuinely succeeded; the fact that NER can't run on it is a separate, unrelated limitation, not a fetch failure.

---

# Part 2 — The backend (`backend/app/`)

## `es_client.py`

Six lines, deliberately **duplicated** from `dags/news_es.py`'s `ES_URL`/`INDEX_NAME`/`get_client()` rather than imported. Reasoning: `dags/` has no `__init__.py` and was never designed to be a general-purpose importable library; other modules that live alongside `news_es.py` in that folder (`entity_enrichment.py`) import `airflow.sdk`, so importing *anything* from `dags/` risks pulling Airflow's runtime environment into a plain FastAPI process for no benefit. `news_es.py` also owns ingestion/migration concerns (`ensure_index`, `bulk_index_*`, the backfills) that a read-only query API has no business touching. The actual shared contract between the two systems is just "the index name and field names agree" — six lines of duplication is far cheaper than a cross-service import dependency.

## `schemas.py`

`DocumentSummary`/`DocumentDetail` are **flat**, with almost every field `Optional` — not a tagged union of `GdeltEventSummary | RssArticleSummary`. This mirrors how the data actually is: a `gdelt_event` doc has no `feed_name`, an unenriched `rss_article` doc has no `entities` — sparseness is the normal case, not an edge case, for both types. A tagged union would force every consumer (the frontend) to narrow the type before accessing any field; the flat-optional shape instead lets the frontend branch once on `type` and then just check individual fields with `??`/`&&`, which is both simpler code and matches how the API actually responds (Pydantic here just omits `None` fields' *values* are still `None` in the JSON, not the keys removed — `DocumentDetail` inherits from `DocumentSummary` because a detail view needs strictly more fields than a summary, never fewer).

## `search.py` — the query builder

This is the file with the most non-obvious design decision in the whole backend: **cross-lingual entity search**.

### The problem it solves

`entities`/`entities_en` are plain `keyword` arrays (not `text`) — see the mapping table above. A `keyword` field is matched as one exact, whole, case-sensitive token. So a `match` or `multi_match` query for `"zelensky"` would **not** match a stored value of `"Volodymyr Zelensky"` — there's no tokenization to split that into matchable words. Adding an analyzed sub-field to `entities`/`entities_en` (the "normal" fix) would require another mapping migration on the ingestion side, adding scope this backend build deliberately didn't take on.

### The fix: `wildcard` with `case_insensitive`

```python
def _entity_should(term, fields):
    value = f"*{term.strip().lower()}*"
    return [{"wildcard": {field: {"value": value, "case_insensitive": True, "boost": boost}}} for field, boost in fields]
```

A `wildcard` query does substring/pattern matching directly against the raw stored keyword value. `*zelensky*` (lowercased) against `case_insensitive: True` matches `"Volodymyr Zelensky"` regardless of its stored casing. `case_insensitive` matters concretely here: entities are stored with their original capitalization (proper nouns), and a user typing a lowercase search term needs that to still match. This is the mechanism that makes the stated cross-lingual goal real: an English query against `entities_en.persons` surfaces a French or German article whose *original-language* `entities` field would never have matched.

`ENTITY_FIELDS_BOOSTED` runs the same wildcard search across six fields at once (`entities_en.persons/organizations/locations` and `entities.persons/organizations/locations`) with a boost ladder (`entities_en.persons` weighted 4×, down to `entities.locations` at 1×) — `entities_en` fields outrank `entities` fields so an English search prefers the translated match, but a search in the original language still works and still contributes to relevance scoring, just lower.

**Documented, accepted limitation**: `wildcard` here matches one *contiguous* lowercase substring — `"angela merkel"` matches a stored `"Angela Merkel"` fine, but a query with the words in a different order wouldn't match at all (there's no per-token matching happening). Living with this was a conscious tradeoff against the larger scope of adding an analyzed entity sub-field.

### `build_query(q, filters)`

```python
if q and q.strip():
    should = [
        {"multi_match": {"query": q, "type": "most_fields",
                          "fields": ["title^3", "summary^2", "content", "actor1^1.5", "actor2^1.5", "place"],
                          "fuzziness": "AUTO"}},
        *_entity_should(q, ENTITY_FIELDS_BOOSTED),
    ]
    must.append({"bool": {"should": should, "minimum_should_match": 1}})
```

- **`multi_match` / `type: "most_fields"`**: searches the same query text across several analyzed fields at once and combines their scores, rewarding a document that matches in *multiple* fields (title *and* summary) over one that only matches in one — appropriate when the fields represent overlapping views of the same content (a title is often a compressed version of the summary), as opposed to `"best_fields"` (take the single best-matching field's score) or `"cross_fields"` (treat all fields as one combined field — better for structured name-like data, not free text).
- **Field boosts** (`title^3`, `summary^2`, `content` unweighted, `actor1^1.5`/`actor2^1.5`): a match in the title is a much stronger relevance signal than the same word appearing somewhere in a long article body, hence the highest boost; summary sits in between; actor names get a modest boost since a GDELT event literally being *about* a named actor is meaningfully relevant.
- **`fuzziness: "AUTO"`**: tolerates small typos/spelling variants (edit-distance-based; `AUTO` scales the allowed distance with the query term's length so short words aren't fuzzy-matched into unrelated short words). This is what let a search for "Norris" incidentally also catch a closely-spelled name like "Boris" at low relevance — an accepted tradeoff of typo tolerance, with correct results still ranking far above the fuzzy noise.
- **The outer `should` + `minimum_should_match: 1`**: means a result can come from *either* the text match *or* the entity wildcard match (or both) — a query doesn't need to hit both to count, since a non-English article might only match via its translated entities, never via its (non-English) `title`/`content`/`summary`.

Filters (`type`, `language`, `feed_category`, date range, the dedicated `entity` param) all go in the query's `filter` clause, not `must` — in Elasticsearch, `filter` context doesn't contribute to relevance scoring and its results are cacheable, which is exactly right for "narrow the result set" criteria that shouldn't affect ranking (a `gdelt_event`-only filter shouldn't make GDELT results score higher, just exclude RSS results).

`if not must and not filter_clauses: return {"match_all": {}}` — an explicit browse-mode case (empty search box, no filters) rather than relying on an empty `bool` query's edge-case behavior; `match_all` is the unambiguous, idiomatic way to say "everything."

### `build_sort`

Default sort is `content_date desc` (newest first) in browse mode, but `_score` (relevance) first, then `content_date desc` as a tiebreaker, once a query string is present — search results should be ranked by relevance; a plain browse should be ranked by recency, since there's no relevance signal to rank by otherwise. `missing: "_last"` on the date sort pushes documents with no `content_date` to the end rather than (Elasticsearch's default) the *beginning* of a descending sort, which would otherwise show undated documents first — the opposite of useful.

### `HIGHLIGHT` and `LIST_SOURCE_FIELDS`

Highlighting (`<mark>`/`</mark>` tags around matched terms) is only requested when `q` is non-empty — there's nothing meaningful to highlight in browse mode, and asking Elasticsearch to compute highlights it'll never use is wasted work. `LIST_SOURCE_FIELDS` explicitly excludes `content` from search *result list* responses (`_source` filtering) — full article bodies can be large, and the list view never displays them (only a short highlighted snippet); `content` is fetched in full only by the single-document detail endpoint, where it's actually needed.

## `routers/search.py`, `routers/documents.py`

`GET /api/search` assembles the query/sort/highlight pieces from `search.py`, adds `track_total_hits: True`, and returns paginated results. **`track_total_hits`** was a real bug found during testing: Elasticsearch's default behavior stops counting matches past 10,000 for performance (returning `"total": {"value": 10000, "relation": "gte"}` — "at least 10,000, we stopped counting"), which silently broke pagination math on this ~47K-document index (browse mode showed exactly `10000` total regardless of the real count). Since this index is modest in size, `track_total_hits: True` forces an exact count unconditionally — the right tradeoff here, though it would need reconsidering on a much larger index where exact counting becomes expensive.

`GET /api/documents/{doc_id:path}` — the `:path` converter (not the default `{doc_id}`) is load-bearing, not stylistic. FastAPI's default path parameter converter matches a single path segment and explicitly excludes `/`. Every document ID in this system **is a full URL** (GDELT's `SOURCEURL` / RSS's `guid`-or-link), which contains `/` and often `?`. The frontend correctly percent-encodes the ID (`encodeURIComponent`), but ASGI servers decode `%2F` back into a literal `/` *before* route matching happens — so the decoded path genuinely has multiple segments, and the default converter's pattern fails to match, 404ing on **every single document**, not just ones with unusual characters. `:path` matches everything remaining in the path, slashes included, which is exactly what's needed for a URL-shaped identifier. (This bug shipped once, got caught when the user clicked an actual result card and got a 404, and slipped past initial testing because the test script checked for the *absence* of certain response fields as a proxy for "found a sparse document" — which is indistinguishable from a 404 error body also lacking those fields. Lesson: check status codes, not just body shape.)

## `stats.py` + `routers/stats.py` — the dashboard aggregation

One query, five aggregations, all wrapped under one filter:

```python
{"query": {"range": {"content_date": {"gte": "2000-01-01"}}}, "aggs": {...}}
```

**The date floor**: exactly one document in the index has a corrupted `content_date` (year `0206` — almost certainly a date-parsing edge case somewhere upstream in feed data). Any date-based chart (a histogram, a min/max range) would otherwise stretch its axis across an 1800-year span to accommodate one outlier, making every real data point invisible by comparison. `2000-01-01` is a permanent floor baked into the query (not a user-adjustable filter) since there is no legitimate document from before 2008 in this index.

The five aggregations:

- **`by_type`**: a plain `terms` aggregation on `type` — bucket counts for `gdelt_event`/`rss_article`, used for the dashboard's headline totals.
- **`volume_over_time`**: a `date_histogram` (one bucket per calendar month) with a nested `terms` sub-aggregation on `type` inside each month bucket. This is what lets the volume chart show *both* the total documents-per-month *and* the type breakdown within each month, from a single query — the sub-aggregation runs once per outer bucket, computed server-side, rather than requiring N separate queries (one per month) or one query per type.
- **`quad_class`** / **`event_root_code`** / **`languages`**: each is a `filter` aggregation (scoping to `gdelt_event` or `rss_article` specifically, since `quad_class`/`event_root_code` only exist on GDELT docs and `language` only on RSS) wrapping a `terms` sub-aggregation on the field itself. The outer `filter` agg is necessary because without it, a plain `terms` agg on `quad_class` would include a phantom "missing" concept for every RSS doc that doesn't have the field at all — scoping first makes the bucket counts exactly the distribution *within the relevant document type*, not diluted by the other type's absence of that field.
- **`avg_tone_over_time`**: a `filter` (GDELT-only) wrapping a `date_histogram` wrapping an `avg` metric aggregation on `avg_tone` — the average sentiment per calendar month. Some months have zero GDELT documents (real, confirmed against live data) and correctly return `avg_tone: null` for that bucket rather than `0` — `0` would be a meaningfully different (and wrong) value on a tone scale that's centered at zero, so the frontend's tone chart explicitly handles `null` as "no data" (a gap in the line, via recharts' `connectNulls={false}`), not "zero sentiment."

`routers/stats.py` just runs this query and reshapes the raw aggregation response (`aggs["quad_class"]["vals"]["buckets"]`, etc. — Elasticsearch's aggregation responses nest under the aggregation names you chose, which is why `stats.py`'s aggregation names and `routers/stats.py`'s reshaping code are tightly coupled) into the typed `StatsResponse` Pydantic model.

## `map_stats.py` + `routers/map_events.py` — the map aggregation

The map's core problem: 31,617 of the 32,534 GDELT documents have a populated `location` (97.2% coverage — confirmed against live data before building this). Returning that many individual points to a browser would mean a huge JSON payload and tens of thousands of overlapping Leaflet markers with no real way to interact with any of them individually at a global zoom level. The fix is server-side clustering via a **`geohash_grid`** aggregation:

```python
"aggs": {"grid": {
    "geohash_grid": {"field": "location", "precision": 3, "size": 300},
    "aggs": {
        "centroid": {"geo_centroid": {"field": "location"}},
        "rep": {"top_hits": {"size": 1, "_source": [...]}},
    },
}}
```

- **`geohash_grid`**: divides the world into a grid of cells (geohash cells) and buckets every matching document by which cell its `location` falls into. **`precision: 3`** controls cell size — geohash precision is a length-of-string-prefix concept, and precision 3 yields cells roughly 156km × 156km at the equator (finer near the poles) — coarse enough that nearby events in the same country/region cluster together into one marker, fine enough that distinct countries or major cities don't get merged into one blob.
- **`size: 300`**: a hard cap on how many grid-cell buckets come back, regardless of how many documents match or how open the filters are. Without a `size` cap, the unfiltered query actually produces **1,345** grid cells globally (confirmed against live data) — capping at 300 (sorted by document count, so the cap drops the *smallest*, least-interesting clusters first) bounds the response size and the number of markers the browser has to render, no matter what filter combination a user applies.
- **`geo_centroid`** (a sub-aggregation *inside* each grid bucket): computes the actual average position of the real points within that cell, rather than using the geohash cell's nominal corner or midpoint — a more accurate marker position with negligible extra query cost.
- **`top_hits: {"size": 1, ...}`** (also inside each grid bucket): pulls one full representative document (place, actors, event code, date) per cluster, specifically for the map popup — without this, clicking a cluster marker would only reveal a count, not any actual content about what happened there.
- **`track_total_hits: True`**: same reasoning as the search endpoint — this index is small enough that an exact "how many total events matched your filters" count (shown above the map) is cheap to compute and more informative than a capped approximate count.

The optional filters (`date_from`/`date_to` as a `range`, `quad_class`/`event_root_code` as `terms`) are appended to the *same* base filter (`type: gdelt_event` + `exists: location`) that's always present — so no filter combination can ever accidentally include an RSS document or an ungeotagged GDELT event, regardless of what the frontend sends.

## `main.py`

`CORSMiddleware` allowing only `localhost:5173`/`127.0.0.1:5173` (the Vite dev server's origin), `GET` only. This is explicitly "defense in depth," not the primary mechanism: the frontend's actual dev-time requests go through Vite's own `/api` proxy (configured in `vite.config.ts`), which means the browser never makes a genuine cross-origin request in normal use — the proxy makes the backend look same-origin. CORS is configured anyway for the case of hitting the backend directly from a browser tab (e.g. `fetch()` from the browser console while debugging), which *would* be a real cross-origin request.

---

# Part 3 — The frontend (`frontend/src/`)

## Conventions used throughout

- **URL-driven state** (`SearchPage.tsx`, `MapPage.tsx`'s filter state): search/filter parameters live in the URL's query string (`useSearchParams` from `react-router-dom`), not just component state. This makes a search shareable (paste the URL, get the same results) and makes the browser back button work correctly (navigating back restores the previous query/filters, since they were never only in memory).
- **Sparse-field rendering**: every component that renders a `DocumentSummary`/`DocumentDetail` checks field presence with `&&`/`??` rather than assuming a field exists — this directly mirrors the backend's flat-optional schema design, which in turn mirrors the actual sparse data.
- **No CSS framework**: plain CSS custom properties (`--bg`, `--fg`, `--muted`, `--border`, `--accent`, `--card-bg`, `--tag-bg`, plus the chart-specific ones added later) defined once in `index.css`, redefined under `@media (prefers-color-scheme: dark)`. Automatic OS-level dark mode, no manual light/dark toggle exists (or was added) anywhere in the app.

## `types.ts` / `api/client.ts`

`types.ts` is a hand-maintained TypeScript mirror of the backend's Pydantic models — there's no code generation step (e.g. from the OpenAPI schema FastAPI already exposes at `/openapi.json`); the two are kept in sync by hand, which is a real maintenance burden to be aware of if the backend schema changes. `api/client.ts` has one `getJson<T>()` helper that every specific fetch function (`searchDocuments`, `getDocument`, `getFacets`, `getStats`, `getMapEvents`) is a thin wrapper around — it throws on any non-2xx response, so every caller gets consistent error handling (a rejected promise with a message) rather than each one re-checking `res.ok` individually.

## Search page and its components

- **`SearchPage.tsx`**: reads `q`/`page`/filters from the URL, calls `searchDocuments()` in a `useEffect` keyed on the serialized search params (`searchParams.toString()` — string comparison, so the effect only reruns when the *actual* params change, not on every render), and writes back to the URL (via `setSearchParams`) whenever the search box, a filter, or the page number changes. Any change to the query or filters resets to page 1 (`effectivePage = next.page ?? 1`) — landing on page 7 of a brand new search would almost always show an empty or confusing result set otherwise.
- **`FilterPanel.tsx`**: fetches `/api/facets` once on mount to populate its language/category checkboxes with *real* values and counts from the live data, rather than a hardcoded list that could drift from what's actually indexed (a category no longer present in the data would show a dead, always-empty checkbox; a new one wouldn't show up without a code change).
- **`ResultCard.tsx`**: builds a different headline/metadata line depending on `doc.type` — GDELT cards show `actor1 → actor2 (event_code)` plus place/goldstein/tone; RSS cards show the title, feed name, and language. `dangerouslySetInnerHTML` is used only for the `<mark>`-wrapped highlight fragments the *backend itself* generates from the original indexed text — not raw user input — so this is a deliberate, scoped use, not an XSS risk (the content being injected is derived from the same document's own already-trusted text, wrapped by our own backend's highlighter, not arbitrary external HTML).
- **`Pagination.tsx`**: computed entirely from `total`/`pageSize` (`Math.ceil(total / pageSize)`), no server round-trip needed to know how many pages exist.

## `DetailPage.tsx`

Same type-conditional pattern as `ResultCard`, at a larger scale: a GDELT doc gets a facts list (`event_code`, `goldstein`, mention counts, etc.); an RSS doc gets its full `content` (falling back to `summary` if no full content was ever fetched, falling back further to an explicit "No article text available" message rather than rendering nothing with no explanation). Both types render `entities` and, if present, a separate `entities_en` block — showing both lets you see, for a non-English article, both the original-language extraction and its translation side by side, rather than only exposing the translated version.

## Dashboard (`DashboardPage.tsx` + `components/charts/`)

Fetches `/api/stats` once and renders three plain stat tiles plus five charts. Chart-type choices, and the color decisions behind them, followed the project's `dataviz` skill procedure rather than being picked by eye — this is worth explaining in full since it's the least obvious part of the frontend.

**Why each chart is the type it is:**
- `VolumeChart` — a stacked `AreaChart` (recharts), two series (`gdelt_event`, `rss_article`). Stacked area shows both total volume *and* the type composition over time in one chart, which two separate line charts wouldn't as directly.
- `QuadClassChart` — a `PieChart`, 4 slices. Few categories with a strong skew (quad_class 1 is ~63% of all GDELT events) — a pie makes that dominance immediately visually obvious in a way a bar chart's more uniform bar heights wouldn't emphasize as strongly.
- `EventRootCodeChart` / `LanguageChart` — horizontal `BarChart`s (via the shared `HorizontalBarChart` component, since these two are structurally identical apart from color and label formatting — built once, parameterized, rather than duplicating the same recharts boilerplate twice). Both plot **one measure** (`count`) across many categories (12 CAMEO codes / 15 languages) — each bar gets **one consistent color**, not a different hue per bar. A per-bar rainbow would use color to redundantly re-encode the category identity the Y-axis labels already show, which the dataviz skill explicitly flags as an anti-pattern ("color follows the entity, never its rank"; a single-series bar chart should read as one series, not one color per data point).
- `ToneChart` — a `LineChart`, single series, with a `ReferenceLine` at `y=0` (a dashed muted-gray baseline) since `avg_tone` genuinely crosses zero (positive = favorable coverage, negative = unfavorable) and a zero reference makes that crossing legible at a glance. `connectNulls={false}` is load-bearing, not decorative: months with zero GDELT documents return `avg_tone: null` from the backend (see `stats.py` above), and connecting across those gaps would visually claim a smooth trend through months where there's no actual data to support one.

**The color palette (`components/charts/palette.ts`)** — this is where the most deliberate reasoning went in:

The dataviz skill's reference palette defines a fixed-order set of 8 categorical hues, pre-validated for colorblind-safety (measured in OKLab color space) *under two different adjacency assumptions*: an "adjacent" context (a stacked bar, a pie chart, a line chart — where only neighboring segments/slices are ever visually compared side by side) safely supports the first 4 slots in fixed order; a "scatter/bubble" context (where *any two* colors might end up next to each other, since there's no fixed spatial order) only reliably supports the first **3** slots simultaneously.

`quad_class` is used as a color-coding dimension in **two different chart types**, which turned out to need **two different color treatments**:

- In the **pie chart** (an adjacent context), all 4 fixed slots (blue/orange/aqua/yellow) are safe to use — so the pie shows the true 4-way breakdown.
- On the **map** (a scatter context — markers for any quad_class can end up adjacent anywhere on the globe), using a 4th hue would risk a real colorblind-accessibility failure between two of the slots. Rather than accept that risk (or arbitrarily drop one of the four real categories), the map instead colors by quad_class's own actual semantic structure: CAMEO's quad classes are literally *cooperation* (1, 2) vs. *conflict* (3, 4) — a genuine 2-category valence axis. The map uses the palette's pre-validated 2-hue **diverging pair** (blue ↔ red) for that binary instead of 4 categorical hues. This isn't just a safety compromise — a map colored by "where is conflict concentrated" is arguably a *more* useful geographic read than a 4-way legend would be at a glance, so the constraint led to a better design, not just a compliant one.

`quadClassMapColor()` implements exactly that binary (`quadClass <= 2 ? cooperation : conflict`). `usePrefersDark()`/`useChartPalette()` reactively track the OS dark-mode media query (not just read it once at mount) so charts recolor immediately if the OS theme changes while the dashboard is open, matching the rest of the app's automatic (not manually toggled) dark mode. The hex values here are a **deliberate duplication** of the `--chart-*`/`--quad-*`/`--map-*` custom properties also defined in `index.css` — SVG `fill`/`stroke` attributes have inconsistent cross-browser support for resolving `var(--x)` CSS custom properties when set as plain string props (as recharts/Leaflet do), so plain hex constants in TypeScript were the more reliable choice, at the cost of needing to keep two representations of the same colors in sync by hand.

## `MapPage.tsx` + `MapFilterPanel.tsx`

- **`CircleMarker`, not `Marker`**: Leaflet's default `Marker` icon is an image asset whose URL resolution breaks under Vite's bundling (a well-known, long-standing Leaflet/bundler friction point — the library's default icon paths assume a classic non-bundled script-tag setup). Using `CircleMarker` instead (a pure vector/SVG circle, not an image) sidesteps the problem entirely rather than working around it with icon-path patches.
- **`radiusForCount(count) = min(4 + sqrt(count), 30)`**: square-root scaling (not linear) so that one very dense cluster (hundreds of events) doesn't visually dwarf the rest of the map — perceived circle *area* scales roughly linearly with the actual count this way (since a circle's area is proportional to the square of its radius), which is the more honest visual encoding than linear radius scaling would give. The `min(..., 30)` cap prevents an extreme outlier from producing an absurdly large circle regardless.
- **Tiles**: the public OpenStreetMap tile server, no API key — consistent with the rest of the project's avoidance of any paid/keyed external service. The one accepted gap: OSM's tiles are light-only (no free dark-tile alternative was used), so `index.css` applies a CSS `filter: invert(1) hue-rotate(...)` trick to the tile layer under the dark-mode media query — a cosmetic approximation, not a true dark map style, but avoids the map glaring against an otherwise dark page.
- **`MapFilterPanel.tsx`**: quad_class checkboxes use the same 4 real labels (`QUAD_CLASS_LABELS`) as the pie chart — filtering is by the true 4-way classification even though the map's *color* only shows the 2-way cooperation/conflict split; these are independent concerns (what you can filter by vs. what a color can safely encode at a glance).

## `cameo.ts`

A hand-transcribed lookup of GDELT's 20 CAMEO root event codes (`"04"` → `"Consult"`, etc.), sourced from the official GDELT Event Codebook. Used by both the dashboard's event-category chart and the map's popup, so a user sees "Consult" rather than the bare code "04" — the raw codes are meaningful only to someone who already has the codebook memorized.

---

# Part 4 — Bugs found and fixed along the way

Kept here as a reference, since several of these are non-obvious footguns worth remembering if this pattern shows up again elsewhere:

1. **`ts` as a task parameter name** (Airflow) — `ts` collided with Airflow's own reserved context variable of the same name (the ISO8601 logical date, auto-injected into every task's context). Renamed to `file_ts` throughout `gdelt_export.py`.
2. **`Variable.get(..., default_var=...)`** — Airflow-2-era kwarg name; Airflow 3's `airflow.sdk.Variable.get()` renamed it to `default`. Caught by actually running the task, not just importing the module.
3. **RSS `content`/`summary` stored with raw HTML** — `content:encoded` commonly embeds markup; left unstripped, it polluted both search relevance and (worse) spaCy's entity spans (a literal `<img.../>` fragment glued onto an extracted name). Fixed going forward in `rss_entry_to_doc` (`_strip_html`); backfilled retroactively via `backfill_strip_html`.
4. **`fetch_status` not reflecting enrichment outcome** — originally only set at ingest time based on raw content presence, never updated by the enrichment DAG. Fixed in two passes: first the enrichment tasks themselves, then a `backfill_fetch_status` correction for everything enriched before that fix existed. A sub-bug within this fix: the first attempt at the backfill's ES `scan()` call didn't include `fetch_status` in its `_source` filter, so the correction's own conditional check silently always saw `None` and never fired — caught by checking the actual document state after running it, not trusting the reported "success" stats alone.
5. **`GET /api/documents/{doc_id}` 404ing on every real ID** — FastAPI's default path converter doesn't match `/`; document IDs are full URLs. Fixed with the `:path` converter. This one is particularly worth remembering: the original manual test of this exact endpoint passed, because the test checked for the *absence* of certain fields as a proxy for "found a real (sparse) document," which is indistinguishable from a 404 error body also lacking those fields. The bug shipped, then was caught for real when a result card was actually clicked.
6. **Search `total` capped at 10,000** — Elasticsearch's default `track_total_hits` behavior. Fixed by setting it to `True` explicitly on the search endpoint (accepted as fine at this index's current size; would need revisiting at much larger scale).
7. **One corrupted `content_date`** (year `0206`, a single RSS document) — not really "fixed" so much as permanently excluded via a `2000-01-01` floor filter in every date-based aggregation, since it's a single bad data point rather than a systemic issue worth chasing to its root cause.
8. **`recharts` build-mode type errors** — `tsc --noEmit` passed, but `tsc -b` (the actual `npm run build` path, using project references) caught stricter type mismatches in the pie chart's `label`/tooltip callback signatures (a `bigint | null | undefined` key type recharts' generics allow, used as an object index). Fixed by normalizing with `String(...)` before lookup — a reminder that `--noEmit` and a real build can disagree.

---

# Part 5 — Known limitations / deliberate simplifications (not bugs)

- **GDELT `entities.persons` is always empty.** CAMEO actor codes don't reliably distinguish a named individual from a country or institutional role without deeper type-code parsing this pipeline doesn't do; bucketing everything as `organizations` was the honest choice over guessing.
- **Entity search only matches contiguous substrings**, not out-of-order multi-word queries (a consequence of using `wildcard` against plain `keyword` fields rather than adding an analyzed sub-field).
- **A few feeds in `feeds.clean.json` have the wrong `language` value** — found twice during testing (a Hindi-language feed and a Hong Kong English-language feed, both mislabeled `"en"`/`"zh"` respectively in the feed config itself). This is a data-quality issue in the feed source list, not in any code in this pipeline — not fixed, since fixing it means auditing 190 feed entries, out of scope for whatever task surfaced it each time.
- **No backfill of historical `content`/`summary` HTML predates the fix for documents not caught by the two backfill runs already performed** (i.e., if this pipeline is pointed at a different index that never had the backfill run against it).
- **The map has no dark tile server** — a CSS filter approximation is used instead of a true dark basemap, since no free dark tile provider was used (consistent with avoiding any keyed external service).
- **No automated tests exist anywhere in this project** — every piece of functionality described above was verified by hand, against real running Elasticsearch/Airflow/FastAPI instances, with real data, at the time it was built. There is no regression safety net if something changes later; re-verification would need to be manual again.

---

# Quick reference: running everything

```bash
# Elasticsearch must already be running at localhost:9200 (started however you normally start it)

# Airflow (DAGs live in dags/, symlinked into your real Airflow dags_folder)
# — gdelt_export (*/15 * * * *), rss_export (0 * * * *), entity_enrichment (*/30 * * * *)

# Backend (from backend/, using the shared project .venv)
cd backend && ../.venv/bin/uvicorn app.main:app --reload --port 8000

# Frontend (from frontend/; Node is installed via nvm, not system-wide —
# run `nvm use --lts` first in a new shell)
cd frontend && npm run dev   # http://localhost:5173

# One-off ES maintenance (from dags/)
python news_es.py --count                    # current doc count
python news_es.py --backfill-html             # re-run the HTML-strip backfill
python news_es.py --backfill-fetch-status     # re-run the fetch_status correction
```
