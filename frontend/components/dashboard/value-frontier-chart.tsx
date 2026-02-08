"use client";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ReferenceLine,
} from "recharts";
import { chartColors, formatMoney } from "@/lib/chart-theme";

interface ValueFrontierMovie {
  movie_id: number;
  title: string;
  year: number;
  budget: number;
  revenue: number;
  revenue_multiple: number;
  imdb_rating: number;
  value_score: number;
}

interface Props {
  data: ValueFrontierMovie[];
}

function getScoreColor(score: number, max: number): string {
  const ratio = score / max;
  if (ratio > 0.7) return chartColors.positive;
  if (ratio > 0.4) return chartColors.neutral;
  return chartColors.warning;
}

export function ValueFrontierChart({ data }: Props) {
  const maxScore = Math.max(...data.map((d) => d.value_score), 1);

  return (
    <Card className="col-span-2">
      <CardHeader>
        <CardTitle>Phim &lsquo;đáng tiền&rsquo; — điểm cao và ROI cao tập trung ở đâu?</CardTitle>
        <CardDescription>
          Phim tối ưu Pareto — kết hợp điểm IMDb cao & ROI cao (màu theo value score)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[400px]">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                type="number"
                dataKey="revenue_multiple"
                name="ROI"
                className="text-xs"
                tickFormatter={(v) => `${v}x`}
                label={{ value: "ROI (x)", position: "insideBottom", offset: -5, style: { fill: "var(--muted-foreground)", fontSize: 11 } }}
              />
              <YAxis
                type="number"
                dataKey="imdb_rating"
                name="Điểm IMDb"
                domain={[4, 10]}
                className="text-xs"
                label={{ value: "Điểm IMDb", angle: -90, position: "insideLeft", style: { textAnchor: "middle", fill: "var(--muted-foreground)", fontSize: 12 } }}
              />
              <Tooltip
                content={({ active, payload }) => {
                  if (active && payload && payload.length > 0) {
                    const d = payload[0].payload as ValueFrontierMovie;
                    return (
                      <div
                        style={{
                          backgroundColor: "var(--card)",
                          border: "1px solid var(--border)",
                          padding: "8px 12px",
                          borderRadius: "6px",
                          fontSize: "12px",
                        }}
                      >
                        <p style={{ fontWeight: 600, color: "var(--foreground)" }}>
                          {d.title} ({d.year})
                        </p>
                        <p>Điểm: {d.imdb_rating} — ROI: {d.revenue_multiple}x</p>
                        <p>Ngân sách: {formatMoney(d.budget)}</p>
                        <p>Doanh thu: {formatMoney(d.revenue)}</p>
                        <p>Value Score: {d.value_score}</p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <ReferenceLine x={1} stroke="var(--muted-foreground)" strokeDasharray="5 5" label={{ value: "Hòa vốn", position: "top", fill: "var(--muted-foreground)", fontSize: 11 }} />
              <ReferenceLine y={7} stroke="var(--muted-foreground)" strokeDasharray="5 5" label={{ value: "Điểm 7.0", position: "right", fill: "var(--muted-foreground)", fontSize: 11 }} />
              <Scatter data={data}>
                {data.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={getScoreColor(entry.value_score, maxScore)}
                    fillOpacity={0.7}
                  />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
