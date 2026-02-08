"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LabelList } from "recharts";
import { useMemo } from "react";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";
import type { ActorNetwork } from "@/lib/api";

interface Props {
  data: ActorNetwork[];
}

export function ActorNetworkTable({ data }: Props) {
  const sortedData = useMemo(() => {
    return [...data]
      .sort((a, b) => b.collaborations - a.collaborations)
      .map((item) => ({
        ...item,
        pair: `${item.actor1} & ${item.actor2}`,
      }));
  }, [data]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Cặp diễn viên hợp tác nhiều nhất</CardTitle>
        <CardDescription>Các cặp diễn viên thường xuyên xuất hiện chung trong nhiều bộ phim</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[500px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={sortedData}
              layout="vertical"
              margin={{ top: 10, right: 40, left: 160, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis type="number" className="text-xs" allowDecimals={false} />
              <YAxis type="category" dataKey="pair" className="text-xs" width={150} />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value, _name, props) => {
                  const payload = props.payload as ActorNetwork & { pair: string };
                  return [`${Number(value)} phim chung`, `${payload.actor1} & ${payload.actor2}`];
                }}
              />
              <Bar dataKey="collaborations" fill={chartColors.categorical[4]} radius={[0, 4, 4, 0]}>
                <LabelList dataKey="collaborations" position="right" style={{ fill: "var(--foreground)", fontSize: 11 }} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
