import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Film, Users, Star, TrendingUp } from "lucide-react";

interface Props {
  totalMovies: number;
  avgRating: number;
  topYear: { year: number; count: number };
  topGenre: string;
}

export function StatsCards({ totalMovies, avgRating, topYear, topGenre }: Props) {
  const stats = [
    {
      title: "Total Movies",
      value: totalMovies.toLocaleString(),
      icon: Film,
      description: "Movies in database",
    },
    {
      title: "Average Rating",
      value: avgRating.toFixed(1),
      icon: Star,
      description: "IMDb average",
    },
    {
      title: "Peak Year",
      value: topYear.year.toString(),
      icon: TrendingUp,
      description: `${topYear.count.toLocaleString()} movies`,
    },
    {
      title: "Top Genre",
      value: topGenre,
      icon: Users,
      description: "Most common genre",
    },
  ];

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      {stats.map((stat) => (
        <Card key={stat.title}>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">{stat.title}</CardTitle>
            <stat.icon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stat.value}</div>
            <p className="text-xs text-muted-foreground">{stat.description}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
