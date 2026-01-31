"use client";

import { useState } from "react";
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
  Legend,
} from "recharts";

interface Props {
  data: { year: number; imdb_avg: number; tmdb_avg: number; ml_avg: number }[];
}

const PLATFORMS = [
  { key: "imdb_avg", name: "IMDb", color: "red" },
  { key: "tmdb_avg", name: "TMDB", color: "green" },
  { key: "ml_avg", name: "MovieLens", color: "blue" },
] as const;

export function PlatformComparisonChart({ data }: Props) {
  const [visiblePlatforms, setVisiblePlatforms] = useState<
    Record<string, boolean>
  >({
    imdb_avg: true,
    tmdb_avg: true,
    ml_avg: true,
  });

  const handleLegendClick = (dataKey: string) => {
    setVisiblePlatforms((prev) => ({
      ...prev,
      [dataKey]: !prev[dataKey],
    }));
  };

  return (
    <Card className="col-span-2">
      <CardHeader>
        <CardTitle>So sánh điểm số giữa các nền tảng</CardTitle>
        <CardDescription>
          Điểm trung bình theo năm trên IMDb, TMDB và MovieLens
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="mb-4 flex flex-wrap gap-4">
          {PLATFORMS.map((platform) => (
            <button
              key={platform.key}
              onClick={() => handleLegendClick(platform.key)}
              className="flex items-center gap-2 text-sm transition-opacity"
              style={{ opacity: visiblePlatforms[platform.key] ? 1 : 0.4 }}
            >
              <span
                className="h-3 w-3 rounded-full"
                style={{ backgroundColor: platform.color }}
              />
              {platform.name}
            </button>
          ))}
        </div>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={data}
              margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey="year" className="text-xs" />
              <YAxis domain={[0, 10]} className="text-xs" />
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                }}
                labelStyle={{ color: "hsl(var(--foreground))" }}
                formatter={(value, name) => [Number(value).toFixed(2), name]}
                labelFormatter={(label) => `Năm: ${label}`}
              />
              <Legend content={() => null} />
              {PLATFORMS.map((platform) => (
                <Line
                  key={platform.key}
                  type="monotone"
                  dataKey={platform.key}
                  name={platform.name}
                  stroke={platform.color}
                  strokeWidth={2}
                  dot={false}
                  hide={!visiblePlatforms[platform.key]}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
