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
} from "recharts";

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

function formatMoney(value: number): string {
  if (Math.abs(value) >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(0)}M`;
  return `$${value}`;
}

function getScoreColor(score: number, max: number): string {
  const ratio = score / max;
  if (ratio > 0.7) return "hsl(142, 71%, 45%)";
  if (ratio > 0.4) return "hsl(200, 70%, 50%)";
  return "hsl(220, 60%, 60%)";
}

export function ValueFrontierChart({ data }: Props) {
  const maxScore = Math.max(...data.map((d) => d.value_score), 1);

  return (
    <Card className="col-span-2">
      <CardHeader>
        <CardTitle>Biên giới giá trị (Value Frontier)</CardTitle>
        <CardDescription>
          Phim tối ưu Pareto — kết hợp điểm cao & ROI cao (màu theo value_score)
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
              />
              <YAxis
                type="number"
                dataKey="imdb_rating"
                name="Điểm IMDb"
                domain={[4, 10]}
                className="text-xs"
              />
              <Tooltip
                content={({ active, payload }) => {
                  if (active && payload && payload.length > 0) {
                    const d = payload[0].payload as ValueFrontierMovie;
                    return (
                      <div
                        style={{
                          backgroundColor: "hsl(var(--card))",
                          border: "1px solid hsl(var(--border))",
                          padding: "8px 12px",
                          borderRadius: "6px",
                          fontSize: "12px",
                        }}
                      >
                        <p style={{ fontWeight: 600, color: "hsl(var(--foreground))" }}>
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
