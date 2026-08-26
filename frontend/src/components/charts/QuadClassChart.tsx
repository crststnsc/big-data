import { Cell, Legend, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts'
import type { FacetBucket } from '../../types'
import { QUAD_CLASS_LABELS, useChartPalette } from './palette'

export default function QuadClassChart({ data }: { data: FacetBucket[] }) {
  const palette = useChartPalette()
  const ordered = ['1', '2', '3', '4']
    .map((key) => data.find((d) => d.key === key))
    .filter((d): d is FacetBucket => d != null)

  return (
    <ResponsiveContainer width="100%" height={280}>
      <PieChart>
        <Pie
          data={ordered}
          dataKey="count"
          nameKey="key"
          cx="50%"
          cy="50%"
          outerRadius={90}
          label={(entry) => QUAD_CLASS_LABELS[String(entry.key)] ?? String(entry.key)}
        >
          {ordered.map((entry, i) => (
            <Cell key={entry.key} fill={palette.quadPie[i]} />
          ))}
        </Pie>
        <Tooltip formatter={(value, _name, item) => [value, QUAD_CLASS_LABELS[String(item.payload.key)]]} />
        <Legend formatter={(value) => QUAD_CLASS_LABELS[String(value)] ?? String(value)} />
      </PieChart>
    </ResponsiveContainer>
  )
}
