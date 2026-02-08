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
  ReferenceLine,
} from "recharts";
import { chartColors, tooltipStyle, formatMoney } from "@/lib/chart-theme";

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

function getROIColor(roi: number): string {
  if (roi >= 10) return chartColors.positive;
  if (roi >= 5) return chartColors.neutral;
  if (roi >= 2) return chartColors.warning;
  return chartColors.categorical[3];
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
        <CardTitle>Phim nào thu hồi vốn gấp bội?</CardTitle>
        <CardDescription>
          Xếp hạng theo tỷ suất lợi nhuận (doanh thu / ngân sách)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[500px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={chartData}
              layout="vertical"
              margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                type="number"
                className="text-xs"
                tickFormatter={(v) => `${v}x`}
                label={{ value: "ROI (doanh thu / ngân sách)", position: "insideBottom", offset: -5, style: { fill: "var(--muted-foreground)", fontSize: 11 } }}
              />
              <YAxis
                type="category"
                dataKey="label"
                className="text-xs"
                width={180}
                tick={{ fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value, _name, props) => {
                  const item = props.payload as ROIMovie & { label: string };
                  return [
                    `${Number(value).toFixed(1)}x (Ngân sách: ${formatMoney(item.budget)}, Doanh thu: ${formatMoney(item.revenue)})`,
                    "ROI",
                  ];
                }}
              />
              <ReferenceLine
                x={1}
                stroke="var(--muted-foreground)"
                strokeDasharray="5 5"
                label={{ value: "Hòa vốn (1x)", position: "top", fill: "var(--muted-foreground)", fontSize: 11 }}
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
