"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { ActorNetwork } from "@/lib/api";

interface Props {
  data: ActorNetwork[];
}

export function ActorNetworkTable({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Cặp diễn viên hay đóng chung</CardTitle>
        <CardDescription>Các diễn viên thường xuyên hợp tác trong nhiều bộ phim</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b">
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">#</th>
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">Diễn viên 1</th>
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">Diễn viên 2</th>
                <th className="text-right py-3 px-2 font-medium text-muted-foreground">Số phim chung</th>
              </tr>
            </thead>
            <tbody>
              {data.map((item, i) => (
                <tr key={i} className="border-b last:border-0 hover:bg-muted/50">
                  <td className="py-3 px-2 text-muted-foreground">{i + 1}</td>
                  <td className="py-3 px-2 font-medium">{item.actor1}</td>
                  <td className="py-3 px-2 font-medium">{item.actor2}</td>
                  <td className="py-3 px-2 text-right">
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-bold bg-chart-5/20">
                      {item.collaborations} phim
                    </span>
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
