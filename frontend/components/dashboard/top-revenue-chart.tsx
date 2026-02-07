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
} from "recharts";

interface TopRevenueMovie {
  movie_id: number;
  title: string;
  year: number;
  revenue: number;
  budget: number;
  profit: number;
  imdb_rating: number;
}

interface Props {
  data: TopRevenueMovie[];
  title?: string;
  description?: string;
  dataKey?: "revenue" | "budget" | "profit";
}

function formatMoney(value: number): string {
  if (Math.abs(value) >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(0)}M`;
  if (Math.abs(value) >= 1_000) return `$${(value / 1_000).toFixed(0)}K`;
  return `$${value}`;
}

export function TopRevenueChart({
  data,
  title = "Top doanh thu phòng vé",
  description = "Những bộ phim có doanh thu cao nhất",
  dataKey = "revenue",
}: Props) {
  const chartData = useMemo(
    () =>
      data.map((m) => ({
        ...m,
        label: `${m.title} (${m.year})`,
      })),
    [data],
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[400px]">
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
                tickFormatter={formatMoney}
              />
              <YAxis
                type="category"
                dataKey="label"
                className="text-xs"
                width={180}
                tick={{ fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                }}
                labelStyle={{ color: "hsl(var(--foreground))" }}
                formatter={(value) => [formatMoney(Number(value)), dataKey === "revenue" ? "Doanh thu" : dataKey === "budget" ? "Ngân sách" : "Lợi nhuận"]}
              />
              <Bar dataKey={dataKey} radius={[0, 4, 4, 0]}>
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
