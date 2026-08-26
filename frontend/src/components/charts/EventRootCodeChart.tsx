import { eventRootCodeLabel } from '../../cameo'
import type { FacetBucket } from '../../types'
import HorizontalBarChart from './HorizontalBarChart'

export default function EventRootCodeChart({ data }: { data: FacetBucket[] }) {
  const top12 = [...data].sort((a, b) => b.count - a.count).slice(0, 12)
  return <HorizontalBarChart data={top12} color="bar1" labelFor={eventRootCodeLabel} />
}
