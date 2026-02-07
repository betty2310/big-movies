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

interface ROIMovie {
  movie_id: number;
  title: string;
  year: number;
  budget: number;
  revenue: number;
  profit: number;
  revenue_multiple: number;
  imdb_rating: number;
}

interface Props {
  data: ROIMovie[];
}

function formatMoney(value: number): string {
  if (Math.abs(value) >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(0)}M`;
  if (Math.abs(value) >= 1_000) return `$${(value / 1_000).toFixed(0)}K`;
  return `$${value}`;
}

function getROIColor(roi: number): string {
  if (roi >= 10) return "hsl(142, 71%, 45%)";
  if (roi >= 5) return "hsl(142, 60%, 55%)";
  if (roi >= 2) return "hsl(200, 70%, 50%)";
  return "hsl(220, 60%, 60%)";
}

export function ROILeaderboardChart({ data }: Props) {
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
        <CardTitle>Bảng xếp hạng ROI</CardTitle>
        <CardDescription>
          Phim có tỷ suất lợi nhuận cao nhất (doanh thu / ngân sách)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[500px]">
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
                formatter={(value, _name, props) => {
                  const item = props.payload as ROIMovie & { label: string };
                  return [
                    `${Number(value).toFixed(1)}x (Ngân sách: ${formatMoney(item.budget)}, Doanh thu: ${formatMoney(item.revenue)})`,
                    "ROI",
                  ];
                }}
              />
              <Bar dataKey="revenue_multiple" radius={[0, 4, 4, 0]}>
                {chartData.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={getROIColor(entry.revenue_multiple)}
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
