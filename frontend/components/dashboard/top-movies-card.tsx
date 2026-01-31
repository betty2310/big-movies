"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import Image from "next/image";
import Link from "next/link";

interface Props {
  data: {
    movie_id: number;
    title: string;
    year: number;
    poster_url: string | null;
    imdb_votes: number;
    imdb_rating: number;
  }[];
}

function formatVotes(votes: number): string {
  if (votes >= 1_000_000) {
    return `${(votes / 1_000_000).toFixed(1)}M`;
  }
  if (votes >= 1_000) {
    return `${(votes / 1_000).toFixed(1)}K`;
  }
  return votes.toString();
}

export function TopMoviesCard({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Phim phổ biến nhất</CardTitle>
        <CardDescription>Các phim có lượt đánh giá cao nhất</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
          {data.map((movie) => (
            <Link
              key={movie.movie_id}
              href={`/movies/${movie.movie_id}`}
              className="flex flex-col gap-2 group"
            >
              <div className="relative aspect-[2/3] w-full overflow-hidden rounded-lg bg-muted group-hover:ring-2 group-hover:ring-primary transition-all">
                {movie.poster_url ? (
                  <Image
                    src={movie.poster_url}
                    alt={movie.title}
                    fill
                    className="object-cover"
                    sizes="(max-width: 640px) 50vw, (max-width: 1024px) 33vw, 20vw"
                  />
                ) : (
                  <div className="flex h-full w-full items-center justify-center text-muted-foreground text-sm">
                    N/A
                  </div>
                )}
              </div>
              <div className="space-y-1">
                <p className="font-medium text-sm leading-tight line-clamp-2 group-hover:text-primary transition-colors">{movie.title}</p>
                <p className="text-xs text-muted-foreground">
                  {movie.year} • ⭐ {movie.imdb_rating?.toFixed(1) ?? "N/A"}
                </p>
                <p className="text-xs text-muted-foreground">
                  {formatVotes(movie.imdb_votes)} lượt đánh giá
                </p>
              </div>
            </Link>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
