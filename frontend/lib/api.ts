const API_BASE =
  process.env.NEXT_PUBLIC_API_URL || "http://77.42.41.13:8000/api";

async function fetchAPI<T>(
  endpoint: string,
  params?: Record<string, string | number>,
): Promise<T> {
  const url = new URL(`${API_BASE}${endpoint}`);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        url.searchParams.append(key, String(value));
      }
    });
  }
  const res = await fetch(url.toString(), { next: { revalidate: 3600 } });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// 1. Market Overview
export interface MoviesPerYear {
  year: number;
  count: number;
}

export interface TopMovie {
  movie_id: string;
  title: string;
  year: number;
  poster_url: string | null;
  imdb_votes: number;
  tmdb_popularity: number;
  imdb_rating: number;
}

export interface LanguageDistribution {
  language: string;
  count: number;
}

// 2. Ratings & Reception
export interface RatingDistribution {
  bin: number;
  count: number;
}

export interface PlatformComparison {
  year: number;
  imdb_avg: number;
  tmdb_avg: number;
  ml_avg: number;
}

export interface CultClassic {
  movie_id: number;
  title: string;
  year: number;
  poster_url: string | null;
  imdb_rating: number;
  imdb_votes: number;
  tmdb_rating: number;
}

export interface RuntimeVsRating {
  runtime: number;
  rating: number;
}

// 3. Genre Evolution
export interface GenreShareByDecade {
  decade: number;
  genre_name: string;
  count: number;
}

export interface GenreAvgRating {
  genre_name: string;
  avg_rating: number;
  movie_count: number;
}

export interface GenreCoOccurrence {
  genre1: string;
  genre2: string;
  count: number;
}

// 4. People Analytics
export interface TopProlific {
  person_id: string;
  name: string;
  movie_count: number;
}

export interface TopRated {
  person_id: string;
  name: string;
  avg_rating: number;
  movie_count: number;
}

export interface ActorNetwork {
  actor1: string;
  actor2: string;
  collaborations: number;
}

// 5. Temporal
export interface RuntimeTrend {
  year: number;
  avg_runtime: number;
  movie_count: number;
}

export interface MPAADistribution {
  mpaa_rating: string;
  count: number;
  avg_rating: number;
}

// 6. Movies Detail
export interface MovieDetail {
  movie_id: number;
  title: string;
  original_title: string;
  year: number;
  runtime: number | null;
  mpaa_rating: string | null;
  plot_summary: string | null;
  poster_url: string | null;
  imdb_rating: number | null;
  imdb_votes: number | null;
  tmdb_rating: number | null;
  tomatometer_score: number | null;
  audience_score: number | null;
}

export interface MovieGenre {
  genre_id: number;
  genre_name: string;
}

export interface MovieCast {
  person_id: string;
  name: string;
  category: string;
  ordering: number;
  characters: string | null;
}

export interface MovieDetailResponse {
  movie: MovieDetail;
  genres: MovieGenre[];
  cast: MovieCast[];
}

// 7. Person Detail
export interface PersonInfo {
  person_id: string;
  name: string;
  birth_year: number | null;
  primary_profession: string | null;
}

export interface Filmography {
  movie_id: number;
  title: string;
  year: number;
  category: string;
  characters: string | null;
  imdb_rating: number | null;
}

export interface PersonDetailResponse {
  person: PersonInfo;
  filmography: Filmography[];
}

export const api = {
  overview: {
    moviesPerYear: (startYear = 1980, endYear = 2025) =>
      fetchAPI<MoviesPerYear[]>("/overview/movies-per-year", {
        start_year: startYear,
        end_year: endYear,
      }),
    topPopular: (limit = 10, metric = "imdb_votes") =>
      fetchAPI<TopMovie[]>("/overview/top-popular", { limit, metric }),
    languageDistribution: () =>
      fetchAPI<LanguageDistribution[]>("/overview/language-distribution"),
  },
  ratings: {
    distribution: (source = "imdb") =>
      fetchAPI<RatingDistribution[]>("/ratings/distribution", { source }),
    platformComparison: (startYear = 1970, endYear = 2024) =>
      fetchAPI<PlatformComparison[]>("/ratings/platform-comparison", {
        start_year: startYear,
        end_year: endYear,
      }),
    cultClassics: (limit = 20, minRating = 8.0, maxVotes = 10000) =>
      fetchAPI<CultClassic[]>("/ratings/cult-classics", {
        limit,
        min_rating: minRating,
        max_votes: maxVotes,
      }),
    runtimeVsRating: (sampleSize = 500) =>
      fetchAPI<RuntimeVsRating[]>("/ratings/runtime-vs-rating", {
        sample_size: sampleSize,
      }),
  },
  genres: {
    shareByDecade: () =>
      fetchAPI<GenreShareByDecade[]>("/genres/share-by-decade"),
    averageRating: () => fetchAPI<GenreAvgRating[]>("/genres/average-rating"),
    coOccurrence: (limit = 20) =>
      fetchAPI<GenreCoOccurrence[]>("/genres/co-occurrence", { limit }),
  },
  people: {
    topProlific: (category = "actor", limit = 20) =>
      fetchAPI<TopProlific[]>("/people/top-prolific", { category, limit }),
    topRated: (category = "actor", minFilms = 5, limit = 20) =>
      fetchAPI<TopRated[]>("/people/top-rated", {
        category,
        min_films: minFilms,
        limit,
      }),
    actorNetwork: (minCollaborations = 3, limit = 50) =>
      fetchAPI<ActorNetwork[]>("/people/actor-network", {
        min_collaborations: minCollaborations,
        limit,
      }),
  },
  temporal: {
    runtimeTrend: (startYear = 1950, endYear = 2025) =>
      fetchAPI<RuntimeTrend[]>("/temporal/runtime-trend", {
        start_year: startYear,
        end_year: endYear,
      }),
    mpaaDistribution: () =>
      fetchAPI<MPAADistribution[]>("/temporal/mpaa-distribution"),
  },
  movies: {
    detail: (movieId: string) =>
      fetchAPI<MovieDetailResponse>(`/movies/${movieId}`),
  },
  person: {
    detail: (personId: string) =>
      fetchAPI<PersonDetailResponse>(`/people/${personId}`),
  },
};
