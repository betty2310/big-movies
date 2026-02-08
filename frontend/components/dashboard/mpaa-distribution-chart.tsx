"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";
import type { MPAADistribution } from "@/lib/api";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

interface Props {
  data: MPAADistribution[];
}

export function MPAADistributionChart({ data }: Props) {
  const topData = data.slice(0, 5);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Phân bố phân loại MPAA</CardTitle>
        <CardDescription>Số lượng phim theo phân loại nội dung</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={topData}
                dataKey="count"
                nameKey="mpaa_rating"
                cx="50%"
                cy="50%"
                outerRadius={80}
                label={({ name }) => name}
              >
                {topData.map((_, index) => (
                  <Cell key={`cell-${index}`} fill={chartColors.categorical[index % chartColors.categorical.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                formatter={(value) => [Number(value).toLocaleString(), "Số phim"]}
              />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
