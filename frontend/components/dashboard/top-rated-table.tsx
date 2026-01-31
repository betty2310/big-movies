"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { TopRated } from "@/lib/api";

interface Props {
  actors: TopRated[];
  directors: TopRated[];
}

export function TopRatedTable({ actors, directors }: Props) {
  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Diễn viên có điểm cao nhất</CardTitle>
          <CardDescription>Điểm trung bình IMDb (tối thiểu 5 phim)</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {actors.map((person, i) => (
              <div key={person.person_id} className="flex items-center gap-3">
                <span className="w-6 h-6 rounded-full bg-chart-3/20 flex items-center justify-center text-xs font-bold">
                  {i + 1}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-medium truncate">{person.name}</p>
                  <p className="text-xs text-muted-foreground">{person.movie_count} phim</p>
                </div>
                <div className="text-sm font-bold text-green-600 dark:text-green-400">
                  ⭐ {person.avg_rating.toFixed(1)}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Đạo diễn có điểm cao nhất</CardTitle>
          <CardDescription>Điểm trung bình IMDb (tối thiểu 5 phim)</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {directors.map((person, i) => (
              <div key={person.person_id} className="flex items-center gap-3">
                <span className="w-6 h-6 rounded-full bg-chart-4/20 flex items-center justify-center text-xs font-bold">
                  {i + 1}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-medium truncate">{person.name}</p>
                  <p className="text-xs text-muted-foreground">{person.movie_count} phim</p>
                </div>
                <div className="text-sm font-bold text-green-600 dark:text-green-400">
                  ⭐ {person.avg_rating.toFixed(1)}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
