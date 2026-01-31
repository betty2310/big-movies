import { api } from "@/lib/api";
import { notFound } from "next/navigation";
import Link from "next/link";
import { ArrowLeft, Film, Star, Calendar } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export const dynamic = "force-dynamic";

interface Props {
  params: Promise<{ id: string }>;
}

export default async function PersonDetailPage({ params }: Props) {
  const { id } = await params;

  let data;
  try {
    data = await api.person.detail(id);
  } catch {
    notFound();
  }

  const { person, filmography } = data;

  const actorFilms = filmography.filter((f) => f.category === "actor");
  const directorFilms = filmography.filter((f) => f.category === "director");
  const otherFilms = filmography.filter(
    (f) => f.category !== "actor" && f.category !== "director"
  );

  const avgRating =
    filmography.filter((f) => f.imdb_rating).length > 0
      ? filmography
          .filter((f) => f.imdb_rating)
          .reduce((sum, f) => sum + (f.imdb_rating || 0), 0) /
        filmography.filter((f) => f.imdb_rating).length
      : null;

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
        <div className="max-w-4xl mx-auto">
          <div className="flex items-start gap-6 mb-8">
            <div className="w-24 h-24 rounded-full bg-muted flex items-center justify-center text-3xl font-bold text-muted-foreground">
              {person.name.charAt(0)}
            </div>
            <div className="flex-1">
              <h1 className="text-3xl font-bold">{person.name}</h1>
              <div className="flex flex-wrap gap-4 mt-2 text-sm text-muted-foreground">
                {person.birth_year && (
                  <div className="flex items-center gap-1">
                    <Calendar className="h-4 w-4" />
                    Sinh năm {person.birth_year}
                  </div>
                )}
                {person.primary_profession && (
                  <Badge variant="secondary">
                    {person.primary_profession}
                  </Badge>
                )}
              </div>
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-3 mb-8">
            <Card>
              <CardContent className="pt-4">
                <div className="flex items-center gap-2">
                  <Film className="h-5 w-5 text-muted-foreground" />
                  <span className="text-2xl font-bold">{filmography.length}</span>
                </div>
                <p className="text-sm text-muted-foreground">Tổng số phim</p>
              </CardContent>
            </Card>
            {avgRating && (
              <Card>
                <CardContent className="pt-4">
                  <div className="flex items-center gap-2">
                    <Star className="h-5 w-5 text-yellow-500" />
                    <span className="text-2xl font-bold">
                      {avgRating.toFixed(1)}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground">
                    Điểm trung bình IMDb
                  </p>
                </CardContent>
              </Card>
            )}
            <Card>
              <CardContent className="pt-4">
                <div className="text-2xl font-bold">
                  {filmography.length > 0
                    ? `${Math.min(...filmography.map((f) => f.year))} - ${Math.max(...filmography.map((f) => f.year))}`
                    : "N/A"}
                </div>
                <p className="text-sm text-muted-foreground">Hoạt động</p>
              </CardContent>
            </Card>
          </div>

          {actorFilms.length > 0 && (
            <FilmographySection title="Diễn viên" films={actorFilms} />
          )}

          {directorFilms.length > 0 && (
            <FilmographySection title="Đạo diễn" films={directorFilms} />
          )}

          {otherFilms.length > 0 && (
            <FilmographySection title="Vai trò khác" films={otherFilms} />
          )}
        </div>
      </main>
    </div>
  );
}

interface FilmographySectionProps {
  title: string;
  films: {
    movie_id: number;
    title: string;
    year: number;
    category: string;
    characters: string | null;
    imdb_rating: number | null;
  }[];
}

function FilmographySection({ title, films }: FilmographySectionProps) {
  const sortedFilms = [...films].sort((a, b) => b.year - a.year);

  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>
          {title} ({films.length} phim)
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {sortedFilms.map((film) => {
            let characters: string[] = [];
            if (film.characters) {
              try {
                characters = JSON.parse(film.characters);
              } catch {
                characters = [film.characters];
              }
            }

            return (
              <Link
                key={`${film.movie_id}-${film.category}`}
                href={`/movies/${film.movie_id}`}
                className="flex items-center gap-4 p-3 rounded-lg hover:bg-muted transition-colors"
              >
                <div className="text-sm text-muted-foreground w-12">
                  {film.year}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-medium truncate">{film.title}</p>
                  {characters.length > 0 && (
                    <p className="text-sm text-muted-foreground truncate">
                      {characters.join(", ")}
                    </p>
                  )}
                </div>
                {film.imdb_rating && (
                  <div className="text-sm font-medium text-yellow-600">
                    ⭐ {film.imdb_rating.toFixed(1)}
                  </div>
                )}
              </Link>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
