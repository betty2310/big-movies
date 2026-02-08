"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { TopRated } from "@/lib/api";
import Link from "next/link";

interface Props {
  actors: TopRated[];
  directors: TopRated[];
}

export function TopRatedTable({ actors, directors }: Props) {
  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Diễn viên được đánh giá cao nhất (≥ 5 phim)</CardTitle>
          <CardDescription>Điểm trung bình IMDb của các phim tham gia (tối thiểu 5 phim)</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {actors.map((person, i) => (
              <Link
                key={person.person_id}
                href={`/people/${person.person_id}`}
                className="flex items-center gap-3 p-2 -mx-2 rounded-lg hover:bg-muted transition-colors"
              >
                <span className="w-6 h-6 rounded-full bg-chart-3/20 flex items-center justify-center text-xs font-bold">
                  {i + 1}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-medium truncate">{person.name}</p>
                  <p className="text-xs text-muted-foreground">{person.movie_count} phim</p>
                </div>
                <div className="text-sm font-bold text-chart-positive">
                  ⭐ {person.avg_rating.toFixed(1)}
                </div>
              </Link>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Đạo diễn được đánh giá cao nhất (≥ 5 phim)</CardTitle>
          <CardDescription>Điểm trung bình IMDb các phim đã đạo diễn (tối thiểu 5 phim)</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {directors.map((person, i) => (
              <Link
                key={person.person_id}
                href={`/people/${person.person_id}`}
                className="flex items-center gap-3 p-2 -mx-2 rounded-lg hover:bg-muted transition-colors"
              >
                <span className="w-6 h-6 rounded-full bg-chart-4/20 flex items-center justify-center text-xs font-bold">
                  {i + 1}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-medium truncate">{person.name}</p>
                  <p className="text-xs text-muted-foreground">{person.movie_count} phim</p>
                </div>
                <div className="text-sm font-bold text-chart-positive">
                  ⭐ {person.avg_rating.toFixed(1)}
                </div>
              </Link>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
