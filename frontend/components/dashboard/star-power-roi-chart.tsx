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
} from "recharts";
import { chartColors, tooltipStyle, formatMoney } from "@/lib/chart-theme";

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

function StarPowerBar({ data, label, color }: { data: StarPowerROI[]; label: string; color: string }) {
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
              contentStyle={{ ...tooltipStyle.contentStyle }}
              labelStyle={{ ...tooltipStyle.labelStyle }}
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
            <Bar dataKey="median_roi" fill={color} radius={[0, 4, 4, 0]} />
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
        <CardTitle>Ngôi sao nào mang lại lợi tức đầu tư tốt nhất?</CardTitle>
        <CardDescription>
          Diễn viên & đạo diễn xếp hạng theo ROI trung vị của sự nghiệp
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid gap-6 lg:grid-cols-2">
          <StarPowerBar data={actors} label="Diễn viên" color={chartColors.categorical[0]} />
          <StarPowerBar data={directors} label="Đạo diễn" color={chartColors.categorical[1]} />
        </div>
      </CardContent>
    </Card>
  );
}
