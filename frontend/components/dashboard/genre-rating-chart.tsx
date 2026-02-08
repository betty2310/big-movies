"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LabelList } from "recharts";
import { useMemo } from "react";
import { chartColors, tooltipStyle } from "@/lib/chart-theme";

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
        <CardTitle>Thể loại nào được đánh giá cao nhất?</CardTitle>
        <CardDescription>Điểm trung bình IMDb theo thể loại (top 15, sắp xếp giảm dần)</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[400px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={sortedData}
              layout="vertical"
              margin={{ top: 10, right: 40, left: 80, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis type="number" domain={[0, 10]} className="text-xs" label={{ value: "Điểm IMDb", position: "insideBottom", offset: -2, style: { fontSize: 11 } }} />
              <YAxis type="category" dataKey="genre_name" className="text-xs" width={70} label={{ value: "Thể loại", angle: -90, position: "insideLeft", offset: 10, style: { fontSize: 11 } }} />
              <Tooltip
                contentStyle={{ ...tooltipStyle.contentStyle }}
                labelStyle={{ ...tooltipStyle.labelStyle }}
                formatter={(value, _name, props) => {
                  const payload = props.payload as GenreRating;
                  return [`${Number(value).toFixed(2)} (${payload.movie_count} phim)`, "Điểm TB"];
                }}
              />
              <Bar dataKey="avg_rating" fill={chartColors.categorical[1]} radius={[0, 4, 4, 0]}>
                <LabelList dataKey="avg_rating" position="right" formatter={(v) => Number(v).toFixed(1)} style={{ fill: "var(--foreground)", fontSize: 11 }} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
