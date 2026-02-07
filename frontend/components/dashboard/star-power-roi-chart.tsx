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

interface StarPowerROI {
  person_id: string;
  name: string;
  movie_count: number;
  median_roi: number;
  avg_profit: number;
}

interface Props {
  actors: StarPowerROI[];
  directors: StarPowerROI[];
}

function formatMoney(value: number): string {
  if (Math.abs(value) >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(0)}M`;
  return `$${value}`;
}

function StarPowerBar({ data, label }: { data: StarPowerROI[]; label: string }) {
  const chartData = useMemo(
    () =>
      data.map((p) => ({
        ...p,
        label: `${p.name} (${p.movie_count})`,
      })),
    [data],
  );

  return (
    <div>
      <h4 className="text-sm font-semibold mb-2 text-muted-foreground">{label}</h4>
      <div className="h-[350px]">
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
              width={160}
              tick={{ fontSize: 11 }}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(var(--card))",
                border: "1px solid hsl(var(--border))",
              }}
              labelStyle={{ color: "hsl(var(--foreground))" }}
              formatter={(
                _value: number | undefined,
                _name: string | undefined,
                props: { payload?: StarPowerROI & { label: string } },
              ) => {
                const item = props.payload;
                if (!item) return ["", ""];
                return [
                  `${item.median_roi}x — TB lợi nhuận: ${formatMoney(item.avg_profit)}`,
                  "ROI trung vị",
                ];
              }}
            />
            <Bar dataKey="median_roi" radius={[0, 4, 4, 0]}>
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
    </div>
  );
}

export function StarPowerROIChart({ actors, directors }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Sức mạnh ngôi sao — ROI</CardTitle>
        <CardDescription>
          Diễn viên & đạo diễn xếp hạng theo ROI trung vị của sự nghiệp
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid gap-6 lg:grid-cols-2">
          <StarPowerBar data={actors} label="🎭 Diễn viên" />
          <StarPowerBar data={directors} label="🎬 Đạo diễn" />
        </div>
      </CardContent>
    </Card>
  );
}
