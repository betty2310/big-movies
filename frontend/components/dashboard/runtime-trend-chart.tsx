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
} from "recharts";

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
        <CardTitle>Xu hướng thời lượng phim</CardTitle>
        <CardDescription>
          Độ dài trung bình của phim qua các năm
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
              <YAxis className="text-xs" unit=" phút" />
              <Tooltip
                contentStyle={{
                  backgroundColor: "green",
                  border: "1px solid hsl(var(--border))",
                }}
                labelStyle={{ color: "black" }}
                formatter={(value, _name, props) => {
                  const payload = props.payload as RuntimeTrend;
                  return [
                    `${Number(value).toFixed(1)} phút (${payload.movie_count} phim)`,
                    "TB thời lượng",
                  ];
                }}
              />
              <Line
                type="monotone"
                dataKey="avg_runtime"
                stroke="black"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 6, fill: "red" }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
