"use client";

import { useState } from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  ComposedChart,
  Line,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { chartColors, tooltipStyle, formatMoney } from "@/lib/chart-theme";

interface ProfitabilityTrend {
  year: number;
  movie_count: number;
  avg_budget: number;
  avg_revenue: number;
  avg_profit: number;
  median_roi: number;
}

interface Props {
  data: ProfitabilityTrend[];
}

const SERIES = [
  { key: "avg_revenue", name: "Doanh thu TB", color: chartColors.positive },
  { key: "avg_budget", name: "Ngân sách TB", color: chartColors.categorical[3] },
  { key: "avg_profit", name: "Lợi nhuận TB", color: chartColors.neutral },
  { key: "median_roi", name: "ROI trung vị", color: chartColors.warning },
] as const;

export function ProfitabilityTrendChart({ data }: Props) {
  const [visible, setVisible] = useState<Record<string, boolean>>({
    avg_revenue: true,
    avg_budget: true,
    avg_profit: true,
    median_roi: true,
  });

  const toggleSeries = (key: string) => {
    setVisible((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <Card className="col-span-2">
      <CardHeader>
        <CardTitle>Ngân sách và doanh thu tăng, nhưng ROI trung vị biến động</CardTitle>
        <CardDescription>
          Xu hướng ngân sách, doanh thu, lợi nhuận trung bình và ROI trung vị theo năm
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="mb-4 flex flex-wrap gap-4">
          {SERIES.map((s) => (
            <button
              key={s.key}
              onClick={() => toggleSeries(s.key)}
              className="flex items-center gap-2 text-sm transition-opacity"
              style={{ opacity: visible[s.key] ? 1 : 0.4 }}
            >
              <span
                className="h-3 w-3 rounded-full"
                style={{ backgroundColor: s.color }}
              />
              {s.name}
            </button>
          ))}
        </div>
        <div className="h-[350px]">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
              data={data}
              margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey="year" className="text-xs" />
              <YAxis
                yAxisId="money"
                className="text-xs"
                tickFormatter={formatMoney}
                label={{ value: "USD (trung bình)", angle: -90, position: "insideLeft", style: { textAnchor: "middle", fill: "var(--muted-foreground)", fontSize: 12 } }}
              />
              <YAxis
                yAxisId="roi"
                orientation="right"
                className="text-xs"
                tickFormatter={(v) => `${v}x`}
                label={{ value: "ROI (x)", angle: 90, position: "insideRight", style: { textAnchor: "middle", fill: "var(--muted-foreground)", fontSize: 12 } }}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value: number | undefined, name: string | undefined) => {
                  const v = value ?? 0;
                  if (name === "ROI trung vị") return [`${v.toFixed(2)}x`, name];
                  return [formatMoney(v), name ?? ""];
                }}
                labelFormatter={(label) => `Năm: ${label}`}
              />
              <Area
                yAxisId="money"
                type="monotone"
                dataKey="avg_revenue"
                name="Doanh thu TB"
                stroke={chartColors.positive}
                fill={chartColors.positive}
                fillOpacity={0.1}
                strokeWidth={2}
                dot={false}
                hide={!visible.avg_revenue}
              />
              <Area
                yAxisId="money"
                type="monotone"
                dataKey="avg_budget"
                name="Ngân sách TB"
                stroke={chartColors.categorical[3]}
                fill={chartColors.categorical[3]}
                fillOpacity={0.1}
                strokeWidth={2}
                dot={false}
                hide={!visible.avg_budget}
              />
              <Line
                yAxisId="money"
                type="monotone"
                dataKey="avg_profit"
                name="Lợi nhuận TB"
                stroke={chartColors.neutral}
                strokeWidth={2}
                dot={false}
                hide={!visible.avg_profit}
              />
              <Line
                yAxisId="roi"
                type="monotone"
                dataKey="median_roi"
                name="ROI trung vị"
                stroke={chartColors.warning}
                strokeWidth={2}
                strokeDasharray="5 5"
                dot={false}
                hide={!visible.median_roi}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
