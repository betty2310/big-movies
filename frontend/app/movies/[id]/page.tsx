import { api } from "@/lib/api";
import { notFound } from "next/navigation";
import Image from "next/image";
import Link from "next/link";
import { ArrowLeft, Clock, Calendar, Star, Users } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export const dynamic = "force-dynamic";

interface Props {
  params: Promise<{ id: string }>;
}

export default async function MovieDetailPage({ params }: Props) {
  const { id } = await params;

  let data;
  try {
    data = await api.movies.detail(id);
  } catch {
    notFound();
  }

  const { movie, genres, cast } = data;
  const actors = cast.filter((c) => c.category === "actor");
  const directors = cast.filter((c) => c.category === "director");

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b bg-card">
        <div className="container mx-auto px-4 py-4">
          <Link
            href="/"
            className="inline-flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
            Quay lại Dashboard
          </Link>
        </div>
      </header>

      <main className="container mx-auto px-4 py-8">
        <div className="grid gap-8 lg:grid-cols-[300px_1fr]">
          <div className="relative aspect-[2/3] overflow-hidden rounded-lg bg-muted">
            {movie.poster_url ? (
              <Image
                src={movie.poster_url}
                alt={movie.title}
                fill
                className="object-cover"
                priority
              />
            ) : (
              <div className="flex h-full w-full items-center justify-center text-muted-foreground">
                No Poster
              </div>
            )}
          </div>

          <div className="space-y-6">
            <div>
              <h1 className="text-3xl font-bold">{movie.title}</h1>
              {movie.original_title !== movie.title && (
                <p className="text-lg text-muted-foreground mt-1">
                  {movie.original_title}
                </p>
              )}
            </div>

            <div className="flex flex-wrap gap-2">
              {genres.map((genre) => (
                <Badge key={genre.genre_id} variant="secondary">
                  {genre.genre_name}
                </Badge>
              ))}
            </div>

            <div className="flex flex-wrap gap-6 text-sm">
              <div className="flex items-center gap-2">
                <Calendar className="h-4 w-4 text-muted-foreground" />
                <span>{movie.year}</span>
              </div>
              {movie.runtime && (
                <div className="flex items-center gap-2">
                  <Clock className="h-4 w-4 text-muted-foreground" />
                  <span>{movie.runtime} phút</span>
                </div>
              )}
              {movie.mpaa_rating && (
                <Badge variant="outline">{movie.mpaa_rating}</Badge>
              )}
            </div>

            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              {movie.imdb_rating && (
                <Card>
                  <CardContent className="pt-4">
                    <div className="text-2xl font-bold text-yellow-500">
                      ⭐ {movie.imdb_rating.toFixed(1)}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      IMDb ({movie.imdb_votes?.toLocaleString()} votes)
                    </p>
                  </CardContent>
                </Card>
              )}
              {movie.tmdb_rating && (
                <Card>
                  <CardContent className="pt-4">
                    <div className="text-2xl font-bold text-blue-500">
                      {movie.tmdb_rating.toFixed(1)}
                    </div>
                    <p className="text-xs text-muted-foreground">TMDB</p>
                  </CardContent>
                </Card>
              )}
              {movie.tomatometer_score && (
                <Card>
                  <CardContent className="pt-4">
                    <div className="text-2xl font-bold text-red-500">
                      🍅 {movie.tomatometer_score}%
                    </div>
                    <p className="text-xs text-muted-foreground">Tomatometer</p>
                  </CardContent>
                </Card>
              )}
              {movie.audience_score && (
                <Card>
                  <CardContent className="pt-4">
                    <div className="text-2xl font-bold text-green-500">
                      🍿 {movie.audience_score}%
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Audience Score
                    </p>
                  </CardContent>
                </Card>
              )}
            </div>

            {movie.plot_summary && (
              <div>
                <h2 className="text-lg font-semibold mb-2">Nội dung</h2>
                <p className="text-muted-foreground leading-relaxed">
                  {movie.plot_summary}
                </p>
              </div>
            )}

            {directors.length > 0 && (
              <div>
                <h2 className="text-lg font-semibold mb-2">Đạo diễn</h2>
                <div className="flex flex-wrap gap-2">
                  {directors.map((director) => (
                    <Link
                      key={director.person_id}
                      href={`/people/${director.person_id}`}
                      className="inline-flex items-center gap-1 px-3 py-1 bg-muted rounded-full hover:bg-muted/80 transition-colors"
                    >
                      <Users className="h-3 w-3" />
                      {director.name}
                    </Link>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        {actors.length > 0 && (
          <Card className="mt-8">
            <CardHeader>
              <CardTitle>Diễn viên</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {actors.map((actor) => {
                  let characters: string[] = [];
                  if (actor.characters) {
                    try {
                      characters = JSON.parse(actor.characters);
                    } catch {
                      characters = [actor.characters];
                    }
                  }

                  return (
                    <Link
                      key={actor.person_id}
                      href={`/people/${actor.person_id}`}
                      className="flex items-center gap-3 p-3 rounded-lg hover:bg-muted transition-colors"
                    >
                      <div className="w-10 h-10 rounded-full bg-muted-foreground/20 flex items-center justify-center text-sm font-medium">
                        {actor.name.charAt(0)}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-medium truncate">{actor.name}</p>
                        {characters.length > 0 && (
                          <p className="text-sm text-muted-foreground truncate">
                            {characters.join(", ")}
                          </p>
                        )}
                      </div>
                    </Link>
                  );
                })}
              </div>
            </CardContent>
          </Card>
        )}
      </main>
    </div>
  );
}
