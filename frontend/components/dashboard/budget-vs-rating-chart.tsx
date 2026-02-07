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
} from "recharts";

interface BudgetVsRating {
  budget: number;
  rating: number;
  revenue: number;
  title: string;
  year: number;
}

interface Props {
  data: BudgetVsRating[];
}

function formatMoney(value: number): string {
  if (Math.abs(value) >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(0)}M`;
  return `$${value}`;
}

export function BudgetVsRatingChart({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Ngân sách vs Điểm số</CardTitle>
        <CardDescription>
          Tương quan giữa chi phí sản xuất và chất lượng phim
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                type="number"
                dataKey="budget"
                name="Ngân sách"
                className="text-xs"
                tickFormatter={formatMoney}
              />
              <YAxis
                type="number"
                dataKey="rating"
                name="Điểm"
                domain={[0, 10]}
                className="text-xs"
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                }}
                labelStyle={{ color: "hsl(var(--foreground))" }}
                content={({ active, payload }) => {
                  if (active && payload && payload.length > 0) {
                    const d = payload[0].payload as BudgetVsRating;
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
                        <p>Ngân sách: {formatMoney(d.budget)}</p>
                        <p>Doanh thu: {formatMoney(d.revenue)}</p>
                        <p>Điểm: {d.rating.toFixed(1)}</p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <Scatter
                data={data}
                fill="hsl(var(--chart-1))"
                fillOpacity={0.5}
              />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
