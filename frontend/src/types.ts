// Mirrors backend/app/schemas.py. Every field beyond id/type is optional --
// docs are genuinely sparse (a GDELT doc has no feed_name, an unenriched RSS
// doc has no entities), so components must not assume presence.

export interface EntitySet {
  persons: string[]
  organizations: string[]
  locations: string[]
}

export interface DocumentSummary {
  id: string
  type: 'gdelt_event' | 'rss_article' | string
  score?: number | null
  content_date?: string | null
  url?: string | null
  domain?: string | null
  title?: string | null
  summary?: string | null
  language?: string | null
  fetch_status?: string | null

  feed_name?: string | null
  feed_category?: string | null
  source_country?: string | null
  author?: string | null

  event_code?: string | null
  event_base_code?: string | null
  event_root_code?: string | null
  quad_class?: number | null
  is_root_event?: boolean | null
  goldstein?: number | null
  avg_tone?: number | null
  num_mentions?: number | null
  num_sources?: number | null
  num_articles?: number | null
  actor1?: string | null
  actor2?: string | null
  actor1_country?: string | null
  actor2_country?: string | null
  place?: string | null
  country?: string | null
  adm1?: string | null

  entities?: EntitySet | null
  entities_en?: EntitySet | null
  highlight?: Record<string, string[]> | null
}

export interface DocumentDetail extends DocumentSummary {
  content?: string | null
  ingested_at?: string | null
  seen_at?: string | null
  enriched_at?: string | null
  interval_ts?: string | null
  guid?: string | null
  geo_type?: number | null
  location?: { lat: number; lon: number } | null
}

export interface SearchResponse {
  total: number
  page: number
  page_size: number
  results: DocumentSummary[]
}

export interface FacetBucket {
  key: string
  count: number
}

export interface FacetsResponse {
  types: FacetBucket[]
  languages: FacetBucket[]
  feed_categories: FacetBucket[]
}

export interface SearchParams {
  q?: string
  type?: string[]
  language?: string[]
  feed_category?: string[]
  entity?: string
  date_from?: string
  date_to?: string
  sort?: 'relevance' | 'date_desc' | 'date_asc'
  page?: number
  page_size?: number
}

export interface DateBucket {
  date: string
  gdelt_event: number
  rss_article: number
}

export interface ToneBucket {
  date: string
  avg_tone: number | null
}

export interface StatsResponse {
  total_docs: number
  total_gdelt_events: number
  total_rss_articles: number
  volume_over_time: DateBucket[]
  quad_class: FacetBucket[]
  event_root_code: FacetBucket[]
  languages: FacetBucket[]
  avg_tone_over_time: ToneBucket[]
}

export interface MapEventPoint {
  geohash: string
  lat: number
  lon: number
  count: number
  place?: string | null
  country?: string | null
  actor1?: string | null
  actor2?: string | null
  event_code?: string | null
  quad_class?: number | null
  content_date?: string | null
  doc_id?: string | null
}

export interface MapEventsResponse {
  total_matched: number
  bucket_count: number
  points: MapEventPoint[]
}

export interface MapFilters {
  date_from?: string
  date_to?: string
  quad_class?: number[]
  event_root_code?: string[]
}
