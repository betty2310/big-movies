"use client";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Treemap, ResponsiveContainer, Tooltip } from "recharts";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

interface Props {
  data: { language: string; count: number }[];
}

interface TreemapContentProps {
  x: number;
  y: number;
  width: number;
  height: number;
  index: number;
  name: string;
  size: number;
}

function CustomContent({
  x,
  y,
  width,
  height,
  index,
  name,
  size,
}: TreemapContentProps) {
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={chartColors.categorical[index % chartColors.categorical.length]}
        stroke="var(--background)"
        strokeWidth={2}
      />
      {width > 50 && height > 25 && (
        <text
          x={x + 8}
          y={y + 20}
          textAnchor="start"
          dominantBaseline="auto"
          fill="var(--card-foreground)"
          className="text-sm font-semibold"
        >
          {name}
        </text>
      )}
      {width > 60 && height > 45 && (
        <text
          x={x + 8}
          y={y + 38}
          textAnchor="start"
          dominantBaseline="auto"
          fill="var(--card-foreground)"
          className="text-xs"
          opacity={0.7}
        >
          {`${(size as number).toLocaleString()} phim`}
        </text>
      )}
    </g>
  );
}

export function LanguageDistributionChart({ data }: Props) {
  const treemapData = data.map((item) => ({
    name: item.language,
    size: item.count,
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle>Tiếng Anh chiếm đa số — phân bố ngôn ngữ phim</CardTitle>
        <CardDescription>
          Tỷ lệ phim theo ngôn ngữ gốc (kích thước tỷ lệ với số phim)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[500px]">
          <ResponsiveContainer width="100%" height="100%">
            <Treemap data={treemapData} dataKey="size" nameKey="name">
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                formatter={(value) => [
                  Number(value).toLocaleString(),
                  "Số phim",
                ]}
              />
            </Treemap>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
