"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { GenreCoOccurrence } from "@/lib/api";

interface Props {
  data: GenreCoOccurrence[];
}

export function GenreCoOccurrenceTable({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Thể loại thường đi cùng nhau</CardTitle>
        <CardDescription>Các cặp thể loại hay xuất hiện chung trong một bộ phim</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b">
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">Thể loại 1</th>
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">Thể loại 2</th>
                <th className="text-right py-3 px-2 font-medium text-muted-foreground">Số phim</th>
              </tr>
            </thead>
            <tbody>
              {data.map((item, i) => (
                <tr key={i} className="border-b last:border-0 hover:bg-muted/50">
                  <td className="py-3 px-2">
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-chart-1/20 text-foreground">
                      {item.genre1}
                    </span>
                  </td>
                  <td className="py-3 px-2">
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-chart-2/20 text-foreground">
                      {item.genre2}
                    </span>
                  </td>
                  <td className="py-3 px-2 text-right font-mono">
                    {item.count.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}
