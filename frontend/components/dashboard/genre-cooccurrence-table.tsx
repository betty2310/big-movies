"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScatterChart, Scatter, XAxis, YAxis, Tooltip } from "recharts";
import type { ScatterShapeProps } from "recharts";
import { useMemo } from "react";
import { tooltipStyle } from "@/lib/chart-theme";
import type { GenreCoOccurrence } from "@/lib/api";

interface Props {
  data: GenreCoOccurrence[];
}

interface MatrixCell {
  x: number;
  y: number;
  genre1: string;
  genre2: string;
  count: number;
  opacity: number;
}

export function GenreCoOccurrenceTable({ data }: Props) {
  const { cells, genres } = useMemo(() => {
    const genreSet = new Set<string>();
    for (const d of data) {
      genreSet.add(d.genre1);
      genreSet.add(d.genre2);
    }
    const genres = Array.from(genreSet).sort();

    const matrix = new Map<string, number>();
    for (const d of data) {
      matrix.set(`${d.genre1}|${d.genre2}`, d.count);
      matrix.set(`${d.genre2}|${d.genre1}`, d.count);
    }

    const maxCount = Math.max(...data.map((d) => d.count), 1);

    const cells: MatrixCell[] = [];
    for (let yi = 0; yi < genres.length; yi++) {
      for (let xi = 0; xi < genres.length; xi++) {
        const count = matrix.get(`${genres[xi]}|${genres[yi]}`) ?? 0;
        if (count > 0) {
          cells.push({
            x: xi,
            y: yi,
            genre1: genres[xi],
            genre2: genres[yi],
            count,
            opacity: 0.15 + (count / maxCount) * 0.85,
          });
        }
      }
    }

    return { cells, genres };
  }, [data]);

  const yAxisWidth = 70;
  const margin = { top: 10, right: 10, bottom: 80, left: 10 };
  const cellSize = genres.length > 0 ? Math.max(Math.floor(500 / genres.length), 24) : 30;
  const plotSize = cellSize * genres.length;
  const totalWidth = plotSize + margin.left + margin.right + yAxisWidth;
  const totalHeight = plotSize + margin.top + margin.bottom;
  const gap = 2;

  const renderCell = (props: ScatterShapeProps) => {
    const { cx, cy, payload } = props as unknown as { cx: number; cy: number; payload: MatrixCell };
    return (
      <rect
        x={(cx as number) - (cellSize - gap) / 2}
        y={(cy as number) - (cellSize - gap) / 2}
        width={cellSize - gap}
        height={cellSize - gap}
        fill="var(--chart-1)"
        fillOpacity={payload.opacity}
        stroke="var(--background)"
        strokeWidth={1}
      />
    );
  };

  const CustomTooltip = ({ active, payload }: { active?: boolean; payload?: Array<{ payload: MatrixCell }> }) => {
    if (!active || !payload?.[0]) return null;
    const cell = payload[0].payload;
    return (
      <div style={{ ...tooltipStyle.contentStyle, padding: "8px 12px" }}>
        <p style={{ ...tooltipStyle.labelStyle, marginBottom: 4 }}>
          {cell.genre1} × {cell.genre2}
        </p>
        <p style={{ fontSize: 12 }}>Số phim: {cell.count.toLocaleString()}</p>
      </div>
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Thể loại thường kết hợp với nhau nhiều nhất</CardTitle>
        <CardDescription>Ma trận thể hiện tần suất kết hợp giữa các cặp thể loại phim</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto flex justify-center">
          <ScatterChart width={totalWidth} height={totalHeight} margin={margin}>
            <XAxis
              type="number"
              dataKey="x"
              domain={[-0.5, genres.length - 0.5]}
              ticks={genres.map((_, i) => i)}
              tickFormatter={(i: number) => genres[i] ?? ""}
              angle={-45}
              textAnchor="end"
              interval={0}
              className="text-xs"
            />
            <YAxis
              type="number"
              dataKey="y"
              domain={[-0.5, genres.length - 0.5]}
              ticks={genres.map((_, i) => i)}
              tickFormatter={(i: number) => genres[i] ?? ""}
              interval={0}
              className="text-xs"
              width={70}
            />
            <Tooltip content={<CustomTooltip />} cursor={false} />
            <Scatter data={cells} shape={renderCell} />
          </ScatterChart>
        </div>
      </CardContent>
    </Card>
  );
}
