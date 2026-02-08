"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from "recharts";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

interface Props {
  data: { runtime: number; rating: number }[];
}

export function RuntimeVsRatingChart({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Thời lượng dài hơn không đồng nghĩa điểm cao hơn</CardTitle>
        <CardDescription>Tương quan giữa thời lượng phim (phút) và điểm IMDb</CardDescription>
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
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value, name) => {
                  const v = Number(value);
                  if (name === "runtime") return [`${v} phút`, "Thời lượng"];
                  return [v.toFixed(1), "Điểm"];
                }}
              />
              <ReferenceLine x={120} stroke="var(--muted-foreground)" strokeDasharray="5 5" label={{ value: "120 phút", position: "top", fill: "var(--muted-foreground)", fontSize: 11 }} />
              <ReferenceLine y={7} stroke="var(--muted-foreground)" strokeDasharray="5 5" label={{ value: "Điểm 7.0", position: "right", fill: "var(--muted-foreground)", fontSize: 11 }} />
              <Scatter
                data={data}
                fill={chartColors.categorical[0]}
                fillOpacity={0.4}
              />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
