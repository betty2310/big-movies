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
  TopRevenueChart,
  GenreProfitabilityChart,
  ProfitabilityTrendChart,
  BudgetVsRatingChart,
  StarPowerROIChart,
} from "@/components/dashboard";
import { Film, Star, TrendingUp, Globe } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export async function IntroSection() {
  const [moviesPerYear, languageDistribution, ratingDistribution] =
    await Promise.all([
      api.overview.moviesPerYear(1950, 2025),
      api.overview.languageDistribution(),
      api.ratings.distribution("imdb"),
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

  return (
    <section>
      <h2 className="text-2xl font-bold mb-2">
        🌍 Chương 1 — Điện ảnh bùng nổ về số lượng và đa dạng ngôn ngữ
      </h2>
      <p className="text-muted-foreground mb-6">
        Thị trường phim đã phình to ra sao trong 75 năm qua? Bao nhiêu phim, bao
        nhiêu ngôn ngữ, và đỉnh điểm nằm ở đâu?
      </p>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Tổng số phim</CardTitle>
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
            <CardTitle className="text-sm font-medium">Điểm trung bình</CardTitle>
            <Star className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{avgRating.toFixed(1)}</div>
            <p className="text-xs text-muted-foreground">Thang điểm IMDb</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Năm đỉnh cao</CardTitle>
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

      <div className="mt-6">
        <MoviesPerYearChart data={moviesPerYear} />
      </div>

      <div className="mt-6">
        <LanguageDistributionChart data={languageDistribution} />
      </div>
    </section>
  );
}

export async function RatingsSection() {
  const [ratingDistribution, runtimeVsRating, platformComparison] =
    await Promise.all([
      api.ratings.distribution("imdb"),
      api.ratings.runtimeVsRating(500),
      api.ratings.platformComparison(1970, 2023),
    ]);
  const platformComparison_ = platformComparison.map((p) => ({
    year: p.year,
    imdb_avg: p.imdb_avg,
    tmdb_avg: p.tmdb_avg * 2,
    ml_avg: p.ml_avg * 2,
  }));

  return (
    <section>
      <h2 className="text-2xl font-bold mb-2">
        ⭐ Chương 2 — Rating không chỉ là một con số
      </h2>
      <p className="text-muted-foreground mb-6">
        Phim &ldquo;hay&rdquo; là hay theo nghĩa nào? Rating phân bố ra sao, thời
        gian phim có liên quan không, và tại sao các nền tảng chấm điểm khác
        nhau?
      </p>

      <div className="mt-6 grid gap-6 lg:grid-cols-2">
        <RatingDistributionChart data={ratingDistribution} />
        <RuntimeVsRatingChart data={runtimeVsRating} />
      </div>

      <div className="mt-6">
        <PlatformComparisonChart data={platformComparison_} />
      </div>
    </section>
  );
}

export async function ProfitabilitySection() {
  const [profitabilityTrend, topRevenue, topProfit] = await Promise.all([
    api.finance.profitabilityTrend(1990, 2022),
    api.finance.topRevenue(10),
    api.finance.topProfit(10, "best"),
  ]);

  return (
    <section>
      <h2 className="text-2xl font-bold mb-2">💎 Chương 3 — Phim vừa hay vừa sinh lời</h2>
      <p className="text-muted-foreground mb-6">
        Ngân sách tăng, doanh thu tăng — nhưng ROI (
        <span className="font-bold">Return on Investment</span>) lại biến động.
        Đâu là nhóm phim tối ưu, vừa được đánh giá cao vừa thu hồi vốn gấp bội?
      </p>

      <ProfitabilityTrendChart data={profitabilityTrend} />

      <div className="mt-6 grid gap-6 lg:grid-cols-2">
        <TopRevenueChart
          data={topProfit}
          title="Top lợi nhuận"
          description="Những bộ phim có lợi nhuận cao nhất"
          dataKey="profit"
        />
        <div className="grid gap-6">
          <TopRevenueChart data={topRevenue} />
        </div>
      </div>
    </section>
  );
}

export async function GenreAndStarPowerSection() {
  const [
    genreShare,
    runtimeTrend,
    genreRating,
    genreProfitability,
    genreCoOccurrence,
    budgetVsRating,
    starPowerActors,
    starPowerDirectors,
  ] = await Promise.all([
    api.genres.shareByDecade(),
    api.temporal.runtimeTrend(1950, 2022),
    api.genres.averageRating(),
    api.finance.genreProfitability(),
    api.genres.coOccurrence(5),
    api.finance.budgetVsRating(500),
    api.finance.starPowerRoi("actor", 10),
    api.finance.starPowerRoi("director", 10),
  ]);

  const genreCoOccurrence_ = genreCoOccurrence.slice(1, 10);

  return (
    <section>
      <h2 className="text-2xl font-bold mb-2">
        🎭 Chương 4 — Vì sao những phim đó thắng? Vai trò của thể loại và star power
      </h2>
      <p className="text-muted-foreground mb-6">
        Thể loại nào vừa hay vừa lời? Chi nhiều hơn có phải lúc nào cũng tốt hơn?
        Và ngôi sao nào thực sự tạo ra giá trị?
      </p>

      <div className="grid gap-6 lg:grid-cols-2">
        <GenreShareChart data={genreShare} />
        <RuntimeTrendChart data={runtimeTrend} />
      </div>

      <div className="mt-6 grid gap-6 lg:grid-cols-2">
        <GenreRatingChart data={genreRating} />
        <GenreProfitabilityChart data={genreProfitability} />
      </div>

      <div className="mt-6">
        <GenreCoOccurrenceTable data={genreCoOccurrence_} />
      </div>

      <div className="mt-6">
        <BudgetVsRatingChart data={budgetVsRating} />
      </div>

      <div className="mt-6">
        <StarPowerROIChart
          actors={starPowerActors}
          directors={starPowerDirectors}
        />
      </div>
    </section>
  );
}

export async function DiscoverySection() {
  const [
    topProlificActors,
    topProlificDirectors,
    topRatedActors,
    topRatedDirectors,
    cultClassics,
    topMovies,
  ] = await Promise.all([
    api.people.topProlific("actor", 10),
    api.people.topProlific("director", 10),
    api.people.topRated("actor", 5, 10),
    api.people.topRated("director", 5, 10),
    api.ratings.cultClassics(8),
    api.overview.topPopular(10),
  ]);

  return (
    <section>
      <h2 className="text-2xl font-bold mb-2">🔍 Chương 5 — Bạn nên xem gì tiếp và theo dõi ai?</h2>
      <p className="text-muted-foreground mb-6">
        Từ insight đến hành động: những người làm phim đáng theo dõi, mạng lưới
        cộng tác, hidden gems chờ khám phá, và top phim phổ biến nhất.
      </p>

      <TopProlificTable
        actors={topProlificActors}
        directors={topProlificDirectors}
      />

      <div className="mt-6">
        <TopRatedTable actors={topRatedActors} directors={topRatedDirectors} />
      </div>

      <div className="mt-6">
        <CultClassicsTable data={cultClassics} />
      </div>

      <div className="mt-6">
        <TopMoviesCard data={topMovies} />
      </div>
    </section>
  );
}
