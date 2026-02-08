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
  ErrorBar,
} from "recharts";
import { chartColors, tooltipStyle, formatMoney } from "@/lib/chart-theme";

interface GenreProfitability {
  genre_name: string;
  movie_count: number;
  median_roi: number;
  p25_roi: number;
  p75_roi: number;
  avg_profit: number;
}

interface Props {
  data: GenreProfitability[];
}

export function GenreProfitabilityChart({ data }: Props) {
  const chartData = useMemo(
    () =>
      data.map((g) => ({
        ...g,
        errorX: [g.median_roi - g.p25_roi, g.p75_roi - g.median_roi],
      })),
    [data],
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>Lợi nhuận theo thể loại kèm độ biến thiên</CardTitle>
        <CardDescription>
          ROI trung vị với khoảng tứ phân vị (P25–P75) — đánh giá rủi ro đầu tư
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[450px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={chartData}
              layout="vertical"
              margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                type="number"
                className="text-xs"
                tickFormatter={(v) => `${v}x`}
                label={{ value: "ROI trung vị (x)", position: "insideBottom", offset: -5, style: { fill: "var(--muted-foreground)", fontSize: 11 } }}
              />
              <YAxis
                type="category"
                dataKey="genre_name"
                className="text-xs"
                width={90}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(_value: number | undefined, _name: string | undefined, props) => {
                  const item = props.payload as GenreProfitability & { errorX: number[] };
                  return [
                    `${item.median_roi}x (P25: ${item.p25_roi}x, P75: ${item.p75_roi}x) — ${item.movie_count} phim, TB lợi nhuận: ${formatMoney(item.avg_profit)}`,
                    "ROI trung vị",
                  ];
                }}
              />
              <Bar dataKey="median_roi" fill={chartColors.categorical[1]} radius={[0, 4, 4, 0]}>
                <ErrorBar dataKey="errorX" width={4} strokeWidth={2} stroke="var(--foreground)" direction="x" />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
