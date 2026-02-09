import { Suspense } from "react";
import {
  IntroSection,
  RatingsSection,
  ProfitabilitySection,
  GenreAndStarPowerSection,
  DiscoverySection,
} from "@/components/dashboard/sections";

export const dynamic = "force-dynamic";

export default async function DashboardPage() {
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
        <Suspense fallback={<div className="h-[420px]" />}>
          <IntroSection />
        </Suspense>

        <Suspense fallback={<div className="h-[420px]" />}>
          <RatingsSection />
        </Suspense>

        <Suspense fallback={<div className="h-[420px]" />}>
          <ProfitabilitySection />
        </Suspense>

        <Suspense fallback={<div className="h-[420px]" />}>
          <GenreAndStarPowerSection />
        </Suspense>

        <Suspense fallback={<div className="h-[420px]" />}>
          <DiscoverySection />
        </Suspense>
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
