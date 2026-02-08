"use client";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

interface Props {
  data: { year: number; count: number }[];
}

export function MoviesPerYearChart({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Sản lượng phim tăng mạnh từ thập niên 2000</CardTitle>
        <CardDescription>
          Số lượng phim phát hành mỗi năm từ 1950 đến nay
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart
              data={data}
              margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey="year" className="text-xs" />
              <YAxis
                className="text-xs"
                label={{
                  value: "Số phim",
                  angle: -90,
                  position: "insideLeft",
                  style: {
                    textAnchor: "middle",
                    fill: "var(--muted-foreground)",
                    fontSize: 12,
                  },
                }}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value) => [
                  Number(value).toLocaleString(),
                  "Số phim",
                ]}
                labelFormatter={(label) => `Năm: ${label}`}
              />
              <Area
                dataKey="count"
                stroke={chartColors.categorical[0]}
                fill={chartColors.categorical[0]}
                fillOpacity={0.3}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
