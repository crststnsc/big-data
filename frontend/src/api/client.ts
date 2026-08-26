import type {
  DocumentDetail,
  FacetsResponse,
  MapEventsResponse,
  MapFilters,
  SearchParams,
  SearchResponse,
  StatsResponse,
} from '../types'

function buildQueryString(params: SearchParams): string {
  const usp = new URLSearchParams()
  if (params.q) usp.set('q', params.q)
  if (params.entity) usp.set('entity', params.entity)
  if (params.date_from) usp.set('date_from', params.date_from)
  if (params.date_to) usp.set('date_to', params.date_to)
  if (params.sort) usp.set('sort', params.sort)
  if (params.page) usp.set('page', String(params.page))
  if (params.page_size) usp.set('page_size', String(params.page_size))
  for (const t of params.type ?? []) usp.append('type', t)
  for (const l of params.language ?? []) usp.append('language', l)
  for (const c of params.feed_category ?? []) usp.append('feed_category', c)
  return usp.toString()
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url)
  if (!res.ok) {
    throw new Error(`Request to ${url} failed: ${res.status}`)
  }
  return res.json() as Promise<T>
}

export function searchDocuments(params: SearchParams): Promise<SearchResponse> {
  return getJson(`/api/search?${buildQueryString(params)}`)
}

export function getDocument(id: string): Promise<DocumentDetail> {
  return getJson(`/api/documents/${encodeURIComponent(id)}`)
}

export function getFacets(): Promise<FacetsResponse> {
  return getJson('/api/facets')
}

export function getStats(): Promise<StatsResponse> {
  return getJson('/api/stats')
}

export function getMapEvents(filters: MapFilters): Promise<MapEventsResponse> {
  const usp = new URLSearchParams()
  if (filters.date_from) usp.set('date_from', filters.date_from)
  if (filters.date_to) usp.set('date_to', filters.date_to)
  for (const q of filters.quad_class ?? []) usp.append('quad_class', String(q))
  for (const c of filters.event_root_code ?? []) usp.append('event_root_code', c)
  return getJson(`/api/map/events?${usp.toString()}`)
}
