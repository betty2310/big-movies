"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

interface Props {
  data: { runtime: number; rating: number }[];
}

export function RuntimeVsRatingChart({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Tương quan thời lượng & điểm số</CardTitle>
        <CardDescription>Mối quan hệ giữa độ dài phim và chất lượng</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                type="number"
                dataKey="runtime"
                name="Thời lượng"
                unit=" phút"
                className="text-xs"
              />
              <YAxis
                type="number"
                dataKey="rating"
                name="Điểm"
                domain={[0, 10]}
                className="text-xs"
              />
              <Tooltip
                contentStyle={{ backgroundColor: "hsl(var(--card))", border: "1px solid hsl(var(--border))" }}
                labelStyle={{ color: "hsl(var(--foreground))" }}
                formatter={(value, name) => {
                  const v = Number(value);
                  if (name === "runtime") return [`${v} phút`, "Thời lượng"];
                  return [v.toFixed(1), "Điểm"];
                }}
              />
              <Scatter
                data={data}
                fill="hsl(var(--chart-1))"
                fillOpacity={0.5}
              />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
