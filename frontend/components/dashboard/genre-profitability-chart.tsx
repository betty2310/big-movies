"use client";

import { useMemo } from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ErrorBar,
} from "recharts";

interface GenreProfitability {
  genre_name: string;
  movie_count: number;
  median_roi: number;
  p25_roi: number;
  p75_roi: number;
  avg_profit: number;
}

interface Props {
  data: GenreProfitability[];
}

function formatMoney(value: number): string {
  if (Math.abs(value) >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(0)}M`;
  return `$${value}`;
}

export function GenreProfitabilityChart({ data }: Props) {
  const chartData = useMemo(
    () =>
      data.map((g) => ({
        ...g,
        errorX: [g.median_roi - g.p25_roi, g.p75_roi - g.median_roi],
      })),
    [data],
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>Lợi nhuận theo thể loại</CardTitle>
        <CardDescription>
          ROI trung vị với khoảng tứ phân vị (P25–P75) — đánh giá rủi ro đầu tư
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[450px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={chartData}
              layout="vertical"
              margin={{ top: 10, right: 30, left: 10, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                type="number"
                className="text-xs"
                tickFormatter={(v) => `${v}x`}
              />
              <YAxis
                type="category"
                dataKey="genre_name"
                className="text-xs"
                width={90}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                }}
                labelStyle={{ color: "hsl(var(--foreground))" }}
                formatter={(_value: number | undefined, _name: string | undefined, props) => {
                  const item = props.payload as GenreProfitability & { errorX: number[] };
                  return [
                    `${item.median_roi}x (P25: ${item.p25_roi}x, P75: ${item.p75_roi}x) — ${item.movie_count} phim, TB lợi nhuận: ${formatMoney(item.avg_profit)}`,
                    "ROI trung vị",
                  ];
                }}
              />
              <Bar dataKey="median_roi" radius={[0, 4, 4, 0]}>
                <ErrorBar dataKey="errorX" width={4} strokeWidth={2} stroke="hsl(var(--foreground))" direction="x" />
                {chartData.map((_, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={`hsl(var(--chart-${(index % 5) + 1}))`}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
