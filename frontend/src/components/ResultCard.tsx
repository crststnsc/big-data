import { Link } from 'react-router-dom'
import type { DocumentSummary } from '../types'

function formatDate(iso?: string | null): string | null {
  if (!iso) return null
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return null
  return d.toLocaleDateString()
}

export default function ResultCard({ doc }: { doc: DocumentSummary }) {
  const isGdelt = doc.type === 'gdelt_event'

  const headline = isGdelt
    ? `${doc.actor1 ?? 'Unknown actor'} → ${doc.actor2 ?? 'Unknown actor'}`
    : doc.title ?? doc.domain ?? 'Untitled'

  const snippet =
    doc.highlight?.summary?.[0] ?? doc.highlight?.content?.[0] ?? doc.summary ?? undefined

  const titleHighlight = doc.highlight?.title?.[0]
  const date = formatDate(doc.content_date)

  return (
    <Link to={`/documents/${encodeURIComponent(doc.id)}`} className="result-card">
      <span className={`badge ${isGdelt ? 'badge-event' : 'badge-article'}`}>
        {isGdelt ? 'Event' : 'Article'}
      </span>

      {titleHighlight ? (
        <h3 dangerouslySetInnerHTML={{ __html: titleHighlight }} />
      ) : (
        <h3>{headline}{isGdelt && doc.event_code ? ` (${doc.event_code})` : ''}</h3>
      )}

      {snippet && <p className="snippet" dangerouslySetInnerHTML={{ __html: snippet }} />}

      <div className="meta">
        {date && <span>{date}</span>}
        {doc.domain && <span>{doc.domain}</span>}
        {!isGdelt && doc.feed_name && <span>{doc.feed_name}</span>}
        {!isGdelt && doc.language && <span className="tag">{doc.language.toUpperCase()}</span>}
        {isGdelt && doc.place && <span>{doc.place}</span>}
        {isGdelt && doc.goldstein != null && <span>Goldstein {doc.goldstein.toFixed(1)}</span>}
        {isGdelt && doc.avg_tone != null && <span>Tone {doc.avg_tone.toFixed(1)}</span>}
      </div>
    </Link>
  )
}
