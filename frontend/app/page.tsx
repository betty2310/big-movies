import { api } from "@/lib/api";
import {
  MoviesPerYearChart,
  TopMoviesCard,
  LanguageDistributionChart,
  RatingDistributionChart,
  PlatformComparisonChart,
  CultClassicsTable,
  RuntimeVsRatingChart,
  GenreShareChart,
  GenreRatingChart,
  RuntimeTrendChart,
  GenreCoOccurrenceTable,
  TopProlificTable,
  TopRatedTable,
  ActorNetworkTable,
  TopRevenueChart,
  ROILeaderboardChart,
  GenreProfitabilityChart,
  ProfitabilityTrendChart,
  BudgetVsRatingChart,
  StarPowerROIChart,
  ValueFrontierChart,
} from "@/components/dashboard";
import { Film, Star, TrendingUp, Globe } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const [
    moviesPerYear,
    topMovies,
    languageDistribution,
    ratingDistribution,
    platformComparison,
    cultClassics,
    runtimeVsRating,
    genreShare,
    genreRating,
    genreCoOccurrence,
    runtimeTrend,
    topProlificActors,
    topProlificDirectors,
    topRatedActors,
    topRatedDirectors,
    actorNetwork,
    topRevenue,
    topProfit,
    roiLeaderboard,
    genreProfitability,
    profitabilityTrend,
    budgetVsRating,
    starPowerActors,
    starPowerDirectors,
    valueFrontier,
  ] = await Promise.all([
    api.overview.moviesPerYear(1950, 2025),
    api.overview.topPopular(10),
    api.overview.languageDistribution(),
    api.ratings.distribution("imdb"),
    api.ratings.platformComparison(1970, 2022),
    api.ratings.cultClassics(15),
    api.ratings.runtimeVsRating(500),
    api.genres.shareByDecade(),
    api.genres.averageRating(),
    api.genres.coOccurrence(5),
    api.temporal.runtimeTrend(1950, 2022),
    api.people.topProlific("actor", 10),
    api.people.topProlific("director", 10),
    api.people.topRated("actor", 5, 10),
    api.people.topRated("director", 5, 10),
    api.people.actorNetwork(3, 15),
    api.finance.topRevenue(10),
    api.finance.topProfit(10, "best"),
    api.finance.roiLeaderboard(15),
    api.finance.genreProfitability(),
    api.finance.profitabilityTrend(1990, 2022),
    api.finance.budgetVsRating(500),
    api.finance.starPowerRoi("actor", 10),
    api.finance.starPowerRoi("director", 10),
    api.finance.valueFrontier(30),
  ]);

  const totalMovies = moviesPerYear.reduce((sum, y) => sum + y.count, 0);
  const topYear = moviesPerYear.reduce(
    (max, y) => (y.count > max.count ? y : max),
    moviesPerYear[0],
  );
  const avgRating =
    ratingDistribution.reduce((sum, d) => sum + d.bin * d.count, 0) /
    ratingDistribution.reduce((sum, d) => sum + d.count, 0);
  const totalLanguages = languageDistribution.length;

  const platformComparison_ = platformComparison.map((p) => ({
    year: p.year,
    imdb_avg: p.imdb_avg,
    tmdb_avg: p.tmdb_avg,
    ml_avg: p.ml_avg * 2,
  }));

  const genreCoOccurrence_ = genreCoOccurrence.slice(1, 10);
  return (
    <div className="min-h-screen bg-background">
      <header className="border-b bg-card">
        <div className="container mx-auto px-4 py-6">
          <h1 className="text-3xl font-bold">🎬 Big Movies Dashboard</h1>
          <p className="text-muted-foreground">
            Trực quan hóa dữ liệu phim từ MovieLens, IMDb, TMDB & Rotten
            Tomatoes
          </p>
        </div>
      </header>

      <main className="container mx-auto px-4 py-8 space-y-12">
        {/* Stats Overview */}
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Tổng số phim
              </CardTitle>
              <Film className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {totalMovies.toLocaleString()}
              </div>
              <p className="text-xs text-muted-foreground">Từ năm 1950-2025</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Điểm trung bình
              </CardTitle>
              <Star className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{avgRating.toFixed(1)}</div>
              <p className="text-xs text-muted-foreground">Thang điểm IMDb</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Năm đỉnh cao
              </CardTitle>
              <TrendingUp className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{topYear.year}</div>
              <p className="text-xs text-muted-foreground">
                {topYear.count.toLocaleString()} phim
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Số ngôn ngữ</CardTitle>
              <Globe className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{totalLanguages}</div>
              <p className="text-xs text-muted-foreground">Ngôn ngữ gốc</p>
            </CardContent>
          </Card>
        </div>

        {/* Section 1: Market Overview */}
        <section>
          <h2 className="text-2xl font-bold mb-6">📊 Tổng quan thị trường</h2>
          <div className="grid gap-6 lg:grid-cols-2">
            <MoviesPerYearChart data={moviesPerYear} />
            <LanguageDistributionChart data={languageDistribution} />
          </div>
          <div className="mt-6">
            <TopMoviesCard data={topMovies} />
          </div>
        </section>

        {/* Section 2: Ratings & Reception */}
        <section>
          <h2 className="text-2xl font-bold mb-6">⭐ Chất lượng & Đánh giá</h2>
          <div className="grid gap-6 lg:grid-cols-2">
            <RatingDistributionChart data={ratingDistribution} />
            <RuntimeVsRatingChart data={runtimeVsRating} />
          </div>
          <div className="mt-6">
            <PlatformComparisonChart data={platformComparison_} />
          </div>
          <div className="mt-6">
            <CultClassicsTable data={cultClassics} />
          </div>
        </section>

        {/* Section 3: Genre Evolution */}
        <section>
          <h2 className="text-2xl font-bold mb-6">🎭 Xu hướng thể loại</h2>
          <div className="grid gap-6 lg:grid-cols-2">
            <GenreShareChart data={genreShare} />
            <GenreRatingChart data={genreRating} />
          </div>
          <div className="mt-6">
            <GenreCoOccurrenceTable data={genreCoOccurrence_} />
          </div>
        </section>

        {/* Section 4: People Analytics */}
        <section>
          <h2 className="text-2xl font-bold mb-6">👥 Phân tích nhân sự</h2>
          <TopProlificTable
            actors={topProlificActors}
            directors={topProlificDirectors}
          />
          <div className="mt-6">
            <TopRatedTable
              actors={topRatedActors}
              directors={topRatedDirectors}
            />
          </div>
          <div className="mt-6">
            <ActorNetworkTable data={actorNetwork} />
          </div>
        </section>

        {/* Section 5: Finance & Box Office */}
        <section>
          <h2 className="text-2xl font-bold mb-6">💰 Tài chính & Phòng vé</h2>
          <div className="grid gap-6 lg:grid-cols-2">
            <TopRevenueChart data={topRevenue} />
            <TopRevenueChart
              data={topProfit}
              title="Top lợi nhuận"
              description="Những bộ phim có lợi nhuận cao nhất"
              dataKey="profit"
            />
          </div>
          <div className="mt-6">
            <ProfitabilityTrendChart data={profitabilityTrend} />
          </div>
          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            <ROILeaderboardChart data={roiLeaderboard} />
            <GenreProfitabilityChart data={genreProfitability} />
          </div>
          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            <BudgetVsRatingChart data={budgetVsRating} />
          </div>
          <div className="mt-6">
            <StarPowerROIChart
              actors={starPowerActors}
              directors={starPowerDirectors}
            />
          </div>
          <div className="mt-6">
            <ValueFrontierChart data={valueFrontier} />
          </div>
        </section>

        {/* Section 6: Temporal Features */}
        <section>
          <h2 className="text-2xl font-bold mb-6">⏱️ Phân tích thời gian</h2>
          <RuntimeTrendChart data={runtimeTrend} />
        </section>
      </main>

      <footer className="border-t py-6 mt-12">
        <div className="container mx-auto px-4 text-center text-muted-foreground text-sm">
          Big Movies Analytics Dashboard • Dữ liệu từ MovieLens, IMDb, TMDB,
          Rotten Tomatoes
        </div>
      </footer>
    </div>
  );
}
