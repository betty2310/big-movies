"use client";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Treemap, ResponsiveContainer, Tooltip } from "recharts";

interface Props {
  data: { language: string; count: number }[];
}

const COLORS = [
  "hsl(var(--chart-1))",
  "hsl(var(--chart-2))",
  "hsl(var(--chart-3))",
  "hsl(var(--chart-4))",
  "hsl(var(--chart-5))",
];

interface TreemapContentProps {
  x: number;
  y: number;
  width: number;
  height: number;
  index: number;
  name: string;
}

function CustomContent({
  x,
  y,
  width,
  height,
  index,
  name,
}: TreemapContentProps) {
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={COLORS[index % COLORS.length]}
        stroke="hsl(var(--background))"
        strokeWidth={2}
      />
      {width > 50 && height > 30 && (
        <text
          x={x + width / 2}
          y={y + height / 2}
          textAnchor="middle"
          dominantBaseline="middle"
          fill="hsl(var(--card-foreground))"
          className="text-xs font-medium"
        >
          {name}
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
        <CardTitle>Phân bố ngôn ngữ</CardTitle>
        <CardDescription>Tỷ lệ phim theo ngôn ngữ gốc</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <Treemap data={treemapData} dataKey="size" nameKey="name">
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                }}
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
