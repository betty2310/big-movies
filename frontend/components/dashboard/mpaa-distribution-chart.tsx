"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";
import type { MPAADistribution } from "@/lib/api";

interface Props {
  data: MPAADistribution[];
}

const COLORS = [
  "hsl(var(--chart-1))",
  "hsl(var(--chart-2))",
  "hsl(var(--chart-3))",
  "hsl(var(--chart-4))",
  "hsl(var(--chart-5))",
];

export function MPAADistributionChart({ data }: Props) {
  const topData = data.slice(0, 5);

  return (
    <Card>
      <CardHeader>
        <CardTitle>MPAA Rating Distribution</CardTitle>
        <CardDescription>Movies by content rating</CardDescription>
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
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ backgroundColor: "hsl(var(--card))", border: "1px solid hsl(var(--border))" }}
                formatter={(value) => [Number(value).toLocaleString(), "Movies"]}
              />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
