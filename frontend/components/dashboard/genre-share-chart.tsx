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
import { useMemo } from "react";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

interface GenreData {
  decade: number;
  genre_name: string;
  count: number;
}

interface Props {
  data: GenreData[];
}

export function GenreShareChart({ data }: Props) {
  const { chartData, topGenres } = useMemo(() => {
    const genreTotals = new Map<string, number>();
    data.forEach((d) => {
      genreTotals.set(
        d.genre_name,
        (genreTotals.get(d.genre_name) || 0) + d.count,
      );
    });

    const sortedGenres = [...genreTotals.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([name]) => name);

    const decadeMap = new Map<number, Record<string, number>>();
    data.forEach((d) => {
      if (!sortedGenres.includes(d.genre_name)) return;
      if (!decadeMap.has(d.decade)) {
        decadeMap.set(d.decade, {});
      }
      decadeMap.get(d.decade)![d.genre_name] = d.count;
    });

    const transformed = [...decadeMap.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([decade, genres]) => {
        const total = Object.values(genres).reduce((sum, v) => sum + v, 0);
        const percentages: Record<string, number> = { decade };
        sortedGenres.forEach((genre) => {
          percentages[genre] =
            total > 0 ? ((genres[genre] || 0) / total) * 100 : 0;
        });
        return percentages;
      });

    return { chartData: transformed, topGenres: sortedGenres };
  }, [data]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Drama và Comedy dẫn đầu — cơ cấu thể loại qua thập kỷ</CardTitle>
        <CardDescription>Thị phần top 8 thể loại phim theo từng thập kỷ</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart
              data={chartData}
              margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey="decade" className="text-xs" label={{ value: "Thập kỷ", position: "insideBottom", offset: -2, style: { fontSize: 11 } }} />
              <YAxis
                tickFormatter={(v) => `${v.toFixed(0)}%`}
                className="text-xs"
                label={{ value: "Thị phần (%)", angle: -90, position: "insideLeft", style: { fontSize: 11 } }}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value) => `${Number(value).toFixed(1)}%`}
              />
              {topGenres.map((genre, i) => (
                <Area
                  key={genre}
                  type="monotone"
                  dataKey={genre}
                  stackId="1"
                  stroke={chartColors.categorical[i % chartColors.categorical.length]}
                  fill={chartColors.categorical[i % chartColors.categorical.length]}
                  fillOpacity={0.8}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="flex flex-wrap gap-2 mt-4">
          {topGenres.map((genre, i) => (
            <div key={genre} className="flex items-center gap-1 text-xs">
              <div
                className="w-3 h-3 rounded"
                style={{ backgroundColor: chartColors.categorical[i % chartColors.categorical.length] }}
              />
              <span>{genre}</span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
