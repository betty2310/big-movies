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
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

interface Props {
  data: { year: number; imdb_avg: number; tmdb_avg: number; ml_avg: number }[];
}

const PLATFORMS = [
  {
    key: "imdb_avg",
    name: "IMDb",
    color: chartColors.categorical[0],
    dash: "",
  },
  {
    key: "tmdb_avg",
    name: "TMDB",
    color: chartColors.categorical[1],
    dash: "",
  },
  {
    key: "ml_avg",
    name: "MovieLens",
    color: chartColors.categorical[2],
    dash: "",
  },
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
        <CardTitle>
          Điểm trung bình hội tụ giữa các nền tảng theo thời gian
        </CardTitle>
        <CardDescription>
          So sánh điểm trung bình hằng năm trên IMDb, TMDB và MovieLens (thang
          10)
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
              <YAxis
                domain={[0, 10]}
                className="text-xs"
                label={{
                  value: "Điểm (0–10)",
                  angle: -90,
                  position: "insideLeft",
                  style: {
                    textAnchor: "middle",
                    fill: "var(--muted-foreground)",
                    fontSize: 12,
                  },
                }}
              />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
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
                  strokeDasharray={platform.dash}
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
