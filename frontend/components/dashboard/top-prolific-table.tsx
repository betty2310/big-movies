"use client";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { TopProlific } from "@/lib/api";

interface Props {
  actors: TopProlific[];
  directors: TopProlific[];
}

export function TopProlificTable({ actors, directors }: Props) {
  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Diễn viên đóng nhiều phim nhất</CardTitle>
          <CardDescription>
            Xếp hạng theo số lượng phim tham gia
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {actors.map((person, i) => (
              <div key={person.person_id} className="flex items-center gap-3">
                <span className="w-6 h-6 rounded-full bg-chart-1/20 flex items-center justify-center text-xs font-bold">
                  {i + 1}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-medium truncate">{person.name}</p>
                </div>
                <div className="text-sm font-mono text-muted-foreground">
                  {person.movie_count} phim
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Đạo diễn nhiều phim nhất</CardTitle>
          <CardDescription>
            Xếp hạng theo số lượng phim đạo diễn
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {directors.map((person, i) => (
              <div key={person.person_id} className="flex items-center gap-3">
                <span className="w-6 h-6 rounded-full bg-chart-2/20 flex items-center justify-center text-xs font-bold">
                  {i + 1}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-medium truncate">{person.name}</p>
                </div>
                <div className="text-sm font-mono text-muted-foreground">
                  {person.movie_count} phim
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
