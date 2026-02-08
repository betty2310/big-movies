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

interface TopRevenueMovie {
  movie_id: number;
  title: string;
  year: number;
  revenue: number;
  budget: number;
  profit: number;
  imdb_rating: number;
}

interface Props {
  data: TopRevenueMovie[];
  title?: string;
  description?: string;
  dataKey?: "revenue" | "budget" | "profit";
}

export function TopRevenueChart({
  data,
  title = "Top phim doanh thu cao nhất mọi thời đại",
  description = "Xếp hạng phim theo doanh thu phòng vé toàn cầu",
  dataKey = "revenue",
}: Props) {
  const chartData = useMemo(
    () =>
      data.map((m) => ({
        ...m,
        label: `${m.title} (${m.year})`,
      })),
    [data],
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[400px]">
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
                tickFormatter={formatMoney}
              />
              <YAxis
                type="category"
                dataKey="label"
                className="text-xs"
                width={180}
                tick={{ fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value) => [formatMoney(Number(value)), dataKey === "revenue" ? "Doanh thu" : dataKey === "budget" ? "Ngân sách" : "Lợi nhuận"]}
              />
              <Bar
                dataKey={dataKey}
                fill={dataKey === "profit" ? chartColors.positive : chartColors.categorical[0]}
                radius={[0, 4, 4, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
