"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from "recharts";
import { useMemo } from "react";

interface GenreRating {
  genre_name: string;
  avg_rating: number;
  movie_count: number;
}

interface Props {
  data: GenreRating[];
}

export function GenreRatingChart({ data }: Props) {
  const sortedData = useMemo(() => {
    return [...data]
      .sort((a, b) => b.avg_rating - a.avg_rating)
      .slice(0, 15);
  }, [data]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Điểm trung bình theo thể loại</CardTitle>
        <CardDescription>So sánh chất lượng giữa các thể loại</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[400px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={sortedData}
              layout="vertical"
              margin={{ top: 10, right: 30, left: 80, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis type="number" domain={[0, 10]} className="text-xs" />
              <YAxis type="category" dataKey="genre_name" className="text-xs" width={70} />
              <Tooltip
                contentStyle={{ backgroundColor: "hsl(var(--card))", border: "1px solid hsl(var(--border))" }}
                labelStyle={{ color: "hsl(var(--foreground))" }}
                formatter={(value, _name, props) => {
                  const payload = props.payload as GenreRating;
                  return [`${Number(value).toFixed(2)} (${payload.movie_count} phim)`, "Điểm TB"];
                }}
              />
              <Bar dataKey="avg_rating" radius={[0, 4, 4, 0]}>
                {sortedData.map((_, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={`hsl(var(--chart-${(index % 5) + 1}))`}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
