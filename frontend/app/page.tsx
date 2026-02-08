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
    api.ratings.platformComparison(1970, 2023),
    api.ratings.cultClassics(8),
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
    tmdb_avg: p.tmdb_avg * 2,
    ml_avg: p.ml_avg * 2,
  }));

  const genreCoOccurrence_ = genreCoOccurrence.slice(1, 10);
  return (
    <div className="min-h-screen bg-background">
      <header className="border-b bg-card">
        <div className="mx-auto max-w-[1600px] px-2 py-6">
          <h1 className="text-3xl font-bold">🎬 Big Movies</h1>
          <p className="text-muted-foreground">
            Hành trình tìm các bộ phim trong 75 năm — dữ liệu từ MovieLens,
            IMDb, TMDB &amp; Rotten Tomatoes
          </p>
        </div>
      </header>

      <main className="mx-auto max-w-[1600px] px-2 py-8 space-y-12">
        {/* ═══════════════════════════════════════════════════════════════
            ACT I · INTRODUCTION
            "Điện ảnh bùng nổ về số lượng và đa dạng ngôn ngữ"
        ═══════════════════════════════════════════════════════════════ */}
        <section>
          <h2 className="text-2xl font-bold mb-2">
            🌍 Chương 1 — Điện ảnh bùng nổ về số lượng và đa dạng ngôn ngữ
          </h2>
          <p className="text-muted-foreground mb-6">
            Thị trường phim đã phình to ra sao trong 75 năm qua? Bao nhiêu phim,
            bao nhiêu ngôn ngữ, và đỉnh điểm nằm ở đâu?
          </p>

          {/* 1. Stats Cards */}
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
                <p className="text-xs text-muted-foreground">
                  Từ năm 1950-2025
                </p>
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
                <CardTitle className="text-sm font-medium">
                  Số ngôn ngữ
                </CardTitle>
                <Globe className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{totalLanguages}</div>
                <p className="text-xs text-muted-foreground">Ngôn ngữ gốc</p>
              </CardContent>
            </Card>
          </div>

          {/* 2. Movies Per Year */}
          <div className="mt-6">
            <MoviesPerYearChart data={moviesPerYear} />
          </div>

          {/* 3. Language Distribution */}
          <div className="mt-6">
            <LanguageDistributionChart data={languageDistribution} />
          </div>
        </section>

        {/* ═══════════════════════════════════════════════════════════════
            ACT II · RISING ACTION
            "Rating không chỉ là một con số"
        ═══════════════════════════════════════════════════════════════ */}
        <section>
          <h2 className="text-2xl font-bold mb-2">
            ⭐ Chương 2 — Rating không chỉ là một con số
          </h2>
          <p className="text-muted-foreground mb-6">
            Phim &ldquo;hay&rdquo; là hay theo nghĩa nào? Rating phân bố ra sao,
            thời gian phim có liên quan không, và tại sao các nền tảng chấm điểm
            khác nhau?
          </p>

          {/* 4. Rating Distribution */}
          <div className="grid gap-6 lg:grid-cols-2">
            <RatingDistributionChart data={ratingDistribution} />
            {/* 5. Runtime vs Rating */}
            <RuntimeVsRatingChart data={runtimeVsRating} />
          </div>

          {/* 6. Platform Comparison */}
          <div className="mt-6">
            <PlatformComparisonChart data={platformComparison_} />
          </div>
        </section>

        {/* ═══════════════════════════════════════════════════════════════
            ACT III · CLIMAX
            "Phim 'đáng xem' nhất nằm ở giao điểm: rating cao + ROI cao"
        ═══════════════════════════════════════════════════════════════ */}
        <section>
          <h2 className="text-2xl font-bold mb-2">
            💎 Chương 3 — Phim vừa hay vừa sinh lời
          </h2>
          <p className="text-muted-foreground mb-6">
            Ngân sách tăng, doanh thu tăng — nhưng ROI (
            <span className="font-bold">Return on Investment</span>) lại biến
            động. Đâu là nhóm phim tối ưu, vừa được đánh giá cao vừa thu hồi vốn
            gấp bội?
          </p>

          {/* 7. Profitability Trend — bối cảnh tài chính */}
          <ProfitabilityTrendChart data={profitabilityTrend} />

          {/* 8. ROI Leaderboard */}
          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            {/* 10. Top Profit */}
            <TopRevenueChart
              data={topProfit}
              title="Top lợi nhuận"
              description="Những bộ phim có lợi nhuận cao nhất"
              dataKey="profit"
            />
            <div className="grid gap-6">
              {/* 9. Top Revenue */}
              <TopRevenueChart data={topRevenue} />
            </div>
          </div>
        </section>

        {/* ═══════════════════════════════════════════════════════════════
            ACT IV · FALLING ACTION
            "Thể loại, công thức kết hợp và star power là động cơ phía sau"
        ═══════════════════════════════════════════════════════════════ */}
        <section>
          <h2 className="text-2xl font-bold mb-2">
            🎭 Chương 4 — Vì sao những phim đó thắng? Vai trò của thể loại và
            star power
          </h2>
          <p className="text-muted-foreground mb-6">
            Thể loại nào vừa hay vừa lời? Chi nhiều hơn có phải lúc nào cũng tốt
            hơn? Và ngôi sao nào thực sự tạo ra giá trị?
          </p>

          {/* 12. Genre Share by Decade */}
          <div className="grid gap-6 lg:grid-cols-2">
            <GenreShareChart data={genreShare} />
            {/* 13. Runtime Trend */}
            <RuntimeTrendChart data={runtimeTrend} />
          </div>

          {/* 14. Genre Rating + 15. Genre Profitability */}
          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            <GenreRatingChart data={genreRating} />
            <GenreProfitabilityChart data={genreProfitability} />
          </div>

          {/* 16. Genre Co-Occurrence */}
          <div className="mt-6">
            <GenreCoOccurrenceTable data={genreCoOccurrence_} />
          </div>

          {/* 17. Budget vs Rating */}
          <div className="mt-6">
            <BudgetVsRatingChart data={budgetVsRating} />
          </div>

          {/* 18. Star Power ROI */}
          <div className="mt-6">
            <StarPowerROIChart
              actors={starPowerActors}
              directors={starPowerDirectors}
            />
          </div>
        </section>

        {/* ═══════════════════════════════════════════════════════════════
            ACT V · DENOUEMENT
            "Danh sách khám phá: xem gì, theo ai, và săn hidden gems"
            Mục tiêu: Call-to-action, kết nhẹ
        ═══════════════════════════════════════════════════════════════ */}
        <section>
          <h2 className="text-2xl font-bold mb-2">
            🔍 Chương 5 — Bạn nên xem gì tiếp và theo dõi ai?
          </h2>
          <p className="text-muted-foreground mb-6">
            Từ insight đến hành động: những người làm phim đáng theo dõi, mạng
            lưới cộng tác, hidden gems chờ khám phá, và top phim phổ biến nhất.
          </p>

          {/* 19. Top Prolific */}
          <TopProlificTable
            actors={topProlificActors}
            directors={topProlificDirectors}
          />

          {/* 20. Top Rated */}
          <div className="mt-6">
            <TopRatedTable
              actors={topRatedActors}
              directors={topRatedDirectors}
            />
          </div>

          {/* 22. Cult Classics — Hidden Gems */}
          <div className="mt-6">
            <CultClassicsTable data={cultClassics} />
          </div>

          {/* 23. Top Movies — điểm chốt nhẹ */}
          <div className="mt-6">
            <TopMoviesCard data={topMovies} />
          </div>
        </section>
      </main>

      <footer className="border-t py-6 mt-12">
        <div className="mx-auto max-w-[1600px] px-2 text-center text-muted-foreground text-sm">
          Big Movies Analytics Dashboard • Dữ liệu từ MovieLens, IMDb, TMDB,
          Rotten Tomatoes
        </div>
      </footer>
    </div>
  );
}
