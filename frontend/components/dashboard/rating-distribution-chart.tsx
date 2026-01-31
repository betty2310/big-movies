"use client";

import { useMemo } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";

interface Props {
  data: { bin: number; count: number }[];
}

export function RatingDistributionChart({ data }: Props) {
  const averageRating = useMemo(() => {
    const totalMovies = data.reduce((sum, item) => sum + item.count, 0);
    const weightedSum = data.reduce((sum, item) => sum + item.bin * item.count, 0);
    return totalMovies > 0 ? weightedSum / totalMovies : 0;
  }, [data]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Phân phối điểm đánh giá</CardTitle>
        <CardDescription>Số lượng phim theo thang điểm IMDb</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey="bin" className="text-xs" />
              <YAxis className="text-xs" />
              <Tooltip
                contentStyle={{ backgroundColor: "hsl(var(--card))", border: "1px solid hsl(var(--border))" }}
                labelStyle={{ color: "hsl(var(--foreground))" }}
                formatter={(value) => [Number(value).toLocaleString(), "Số phim"]}
                labelFormatter={(label) => `Điểm: ${label}`}
              />
              <ReferenceLine
                x={averageRating.toFixed(1)}
                stroke="hsl(var(--chart-3))"
                strokeDasharray="5 5"
                strokeWidth={2}
                label={{
                  value: `TB: ${averageRating.toFixed(1)}`,
                  position: "top",
                  fill: "hsl(var(--chart-3))",
                  fontSize: 12,
                }}
              />
              <Bar dataKey="count" fill="hsl(var(--chart-2))" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
