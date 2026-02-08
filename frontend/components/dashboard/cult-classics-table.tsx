"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import Image from "next/image";
import Link from "next/link";

interface CultClassic {
  movie_id: number;
  title: string;
  year: number;
  poster_url: string | null;
  imdb_rating: number;
  imdb_votes: number;
  tmdb_rating: number;
}

interface Props {
  data: CultClassic[];
}

export function CultClassicsTable({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Viên ngọc ẩn — điểm IMDb cao nhưng ít ai biết đến</CardTitle>
        <CardDescription>Phim có điểm IMDb ≥ 8.0 nhưng ít hơn 10,000 lượt đánh giá</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {data.map((movie) => (
            <Link
              key={movie.movie_id}
              href={`/movies/${movie.movie_id}`}
              className="flex items-center gap-3 p-2 -mx-2 rounded-lg hover:bg-muted transition-colors"
            >
              {movie.poster_url ? (
                <Image
                  src={movie.poster_url}
                  alt={movie.title}
                  width={40}
                  height={60}
                  className="rounded object-cover"
                />
              ) : (
                <div className="w-10 h-15 bg-muted rounded flex items-center justify-center text-muted-foreground text-xs">
                  N/A
                </div>
              )}
              <div className="flex-1 min-w-0">
                <p className="font-medium truncate">{movie.title}</p>
                <p className="text-sm text-muted-foreground">{movie.year}</p>
              </div>
              <div className="text-right">
                <p className="font-medium text-chart-positive">⭐ {movie.imdb_rating.toFixed(1)}</p>
                <p className="text-xs text-muted-foreground">
                  {movie.imdb_votes.toLocaleString()} votes
                </p>
              </div>
            </Link>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
