import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { getDocument } from '../api/client'
import type { DocumentDetail, EntitySet } from '../types'

function EntityChips({ label, values }: { label: string; values?: string[] }) {
  if (!values || values.length === 0) return null
  return (
    <div className="entity-group">
      <span className="entity-label">{label}</span>
      {values.map((v) => (
        <span key={v} className="tag">
          {v}
        </span>
      ))}
    </div>
  )
}

function EntitiesBlock({ title, entities }: { title: string; entities?: EntitySet | null }) {
  if (!entities) return null
  const hasAny = entities.persons.length || entities.organizations.length || entities.locations.length
  if (!hasAny) return null
  return (
    <div className="entities-block">
      <h4>{title}</h4>
      <EntityChips label="People" values={entities.persons} />
      <EntityChips label="Organizations" values={entities.organizations} />
      <EntityChips label="Locations" values={entities.locations} />
    </div>
  )
}

export default function DetailPage() {
  const { id } = useParams<{ id: string }>()
  const [doc, setDoc] = useState<DocumentDetail | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    setDoc(null)
    setError(null)
    getDocument(id).then(setDoc).catch((err) => setError(err.message))
  }, [id])

  if (error) return <p className="status-message error">Error: {error}</p>
  if (!doc) return <p className="status-message">Loading…</p>

  const isGdelt = doc.type === 'gdelt_event'
  const date = doc.content_date ? new Date(doc.content_date).toLocaleString() : null

  return (
    <div className="detail-page">
      <Link to="/" className="back-link">
        ← Back to search
      </Link>

      <span className={`badge ${isGdelt ? 'badge-event' : 'badge-article'}`}>
        {isGdelt ? 'GDELT Event' : 'RSS Article'}
      </span>

      <h1>{isGdelt ? `${doc.actor1 ?? 'Unknown actor'} → ${doc.actor2 ?? 'Unknown actor'}` : doc.title ?? 'Untitled'}</h1>

      <div className="meta">
        {date && <span>{date}</span>}
        {doc.domain && <span>{doc.domain}</span>}
        {!isGdelt && doc.feed_name && <span>{doc.feed_name}</span>}
        {!isGdelt && doc.author && <span>By {doc.author}</span>}
        {!isGdelt && doc.language && <span className="tag">{doc.language.toUpperCase()}</span>}
      </div>

      {!isGdelt && (
        <div className="article-body">
          {doc.content ? <p>{doc.content}</p> : doc.summary ? <p>{doc.summary}</p> : (
            <p className="status-message">No article text available.</p>
          )}
        </div>
      )}

      {isGdelt && (
        <dl className="gdelt-facts">
          {doc.event_code && (
            <>
              <dt>Event code</dt>
              <dd>
                {doc.event_code} ({doc.event_root_code})
              </dd>
            </>
          )}
          {doc.place && (
            <>
              <dt>Location</dt>
              <dd>
                {doc.place}
                {doc.country ? ` (${doc.country})` : ''}
              </dd>
            </>
          )}
          {doc.goldstein != null && (
            <>
              <dt>Goldstein scale</dt>
              <dd>{doc.goldstein}</dd>
            </>
          )}
          {doc.avg_tone != null && (
            <>
              <dt>Average tone</dt>
              <dd>{doc.avg_tone}</dd>
            </>
          )}
          {doc.num_mentions != null && (
            <>
              <dt>Mentions / Sources / Articles</dt>
              <dd>
                {doc.num_mentions} / {doc.num_sources} / {doc.num_articles}
              </dd>
            </>
          )}
        </dl>
      )}

      <EntitiesBlock title="Entities" entities={doc.entities} />
      {doc.entities_en && (
        <EntitiesBlock title="Entities (English)" entities={doc.entities_en} />
      )}

      {doc.url && (
        <p>
          <a href={doc.url} target="_blank" rel="noreferrer">
            View original source →
          </a>
        </p>
      )}
    </div>
  )
}
