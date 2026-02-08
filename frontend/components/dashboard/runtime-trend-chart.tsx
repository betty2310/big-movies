"use client";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

interface RuntimeTrend {
  year: number;
  avg_runtime: number;
  movie_count: number;
}

interface Props {
  data: RuntimeTrend[];
}

export function RuntimeTrendChart({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Thời lượng trung bình phim tăng nhẹ theo thời gian</CardTitle>
        <CardDescription>
          Độ dài trung bình của phim qua các năm (đơn vị: phút)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={data}
              margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey="year" className="text-xs" />
              <YAxis
                className="text-xs"
                unit=" phút"
                label={{ value: "Phút", angle: -90, position: "insideLeft", style: { textAnchor: "middle", fill: "var(--muted-foreground)", fontSize: 12 } }}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value, _name, props) => {
                  const payload = props.payload as RuntimeTrend;
                  return [
                    `${Number(value).toFixed(1)} phút (${payload.movie_count} phim)`,
                    "TB thời lượng",
                  ];
                }}
              />
              <ReferenceLine y={90} stroke="var(--muted-foreground)" strokeDasharray="5 5" label={{ value: "90 phút", position: "right", fill: "var(--muted-foreground)", fontSize: 11 }} />
              <Line
                type="monotone"
                dataKey="avg_runtime"
                stroke={chartColors.categorical[1]}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 6, fill: chartColors.categorical[0] }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
