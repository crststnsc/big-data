import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { ToneBucket } from '../../types'
import { useChartPalette } from './palette'

export default function ToneChart({ data }: { data: ToneBucket[] }) {
  const palette = useChartPalette()

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={palette.gridline} vertical={false} />
        <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={40} />
        <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
        <Tooltip />
        <ReferenceLine y={0} stroke={palette.baseline} strokeDasharray="4 4" />
        <Line
          type="monotone"
          dataKey="avg_tone"
          name="Avg. tone"
          stroke={palette.line}
          strokeWidth={2}
          dot={false}
          connectNulls={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
