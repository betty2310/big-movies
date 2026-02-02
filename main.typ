#import "@preview/slydst:0.1.5": *

#show: slides.with(
  title: "Hệ thống Phân tích Dữ liệu lớn & Khám phá Tri thức Phim ảnh",
  subtitle: "Big Data Integration",
  authors: "Học viên: Dương Hữu Huynh & Đoàn Sỹ Nguyên",
  layout: "medium",
  ratio: 16/9,
  title-color: navy,
)

#show raw: set block(fill: luma(240), width: 100%, inset: 1em, radius: 5pt)

== Nội dung trình bày

#outline()

= 1. Tổng quan & Mục tiêu

== Bối cảnh & Mục tiêu

*Vấn đề:* Dữ liệu phim ảnh bị phân mảnh trên nhiều nền tảng với đặc thù khác nhau (hành vi người dùng, metadata chuyên sâu, đánh giá phê bình, hình ảnh).

*Mục tiêu:* Xây dựng hệ thống Data Warehouse tập trung:
- Hợp nhất dữ liệu từ "nhiều" nguồn không đồng nhất.
- Cung cấp nhiều góc nhìn đề phim ảnh (Tài chính, Nghệ thuật, Thị hiếu).
- Khám phá tri thức.

= 2. Nguồn dữ liệu & Đặc trưng Big Data

== Các nguồn dữ liệu (Variety & Volume)

Hệ thống tích hợp dữ liệu từ 4 nguồn với cấu trúc khác nhau:

#table(
  columns: (1fr, 1fr, 1fr, 2fr),
  inset: 10pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Nguồn*], [*Loại dữ liệu*], [*Phương thức*], [*Đặc điểm chính*],
  [**MovieLens**], [Structured (CSV)], [Download], [32M ratings, hành vi người dùng cốt lõi.],
  [**IMDb**], [Structured (TSV)], [Daily Dump], [Metadata phong phú, Crew/Cast graph.],
  [**Rotten Tomatoes**], [Unstructured (HTML)], [Scraping], [Sentiment (Critic vs Audience), Box Office.],
  [**TMDB**], [Semi-structured (JSON)], [REST API], [Poster, Plot summary, Multimedia.]
)

== Chi tiết Nguồn dữ liệu (1/2): Nhóm Structured Data

Đây là nhóm dữ liệu có cấu trúc định hình sẵn, đóng vai trò là "xương sống" cho hệ thống.

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *1. MovieLens (CSV)* \
      _Nguồn hành vi người dùng & Mapping_
      - **ratings.csv**: `userId`, `movieId`, `rating` (0.5-5.0), `timestamp`.
      - **movies.csv**: `movieId`, `title` (kèm năm), `genres` (phân cách bởi `|`).
      - **links.csv**: Bảng ánh xạ quan trọng nhất (`movieId` $arrow$ `imdbId` $arrow$ `tmdbId`).
    ]
  ],
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *2. IMDb (TSV)* \
      _Metadata chuyên sâu & Knowledge Graph_
      - **title.basics**: `tconst` (ID), `primaryTitle`, `startYear`, `runtime`.
      - **title.ratings**: `averageRating`, `numVotes` (độ phổ biến toàn cầu).
      - **title.principals**: Liên kết `tconst` (Phim) $arrow$ `nconst` (Người) cùng vai trò (`category`).
      - **name.basics**: `primaryName`, `birthYear`.
    ]
  ]
)

== Chi tiết Nguồn dữ liệu (2/2): Nhóm Semi & Unstructured

Nhóm dữ liệu làm giàu, bổ sung các góc nhìn về tài chính, cảm xúc và hình ảnh.

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *3. Rotten Tomatoes (HTML/Scraped)* \
      _Dữ liệu phi cấu trúc: Sentiment & Doanh thu_
      - **Scores**: `tomatometer_score` (Chuyên gia), `audience_score` (Khán giả).
      - **Finance**: `box_office` (Chuỗi ký tự cần xử lý, VD: "\$100M").
      - **Meta**: `mpaa_rating` (Phân loại độ tuổi), `rt_id` (Slug URL).
    ]
  ],
  [
    #block(fill: orange.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *4. TMDB (JSON API)* \
      _Dữ liệu bán cấu trúc: Hình ảnh & Ngữ cảnh_
      - **Visuals**: `poster_path`, `backdrop_path`.
      - **Content**: `overview`.
      - **Stats**: `popularity` (Chỉ số trending), `vote_average`.
      - **Structure**: Dữ liệu trả về dạng JSON nested.
    ]
  ]
)


== Thách thức về Dữ liệu (Veracity & Heterogeneity)

Trong quá trình thu thập và phân tích, các vấn đề sau đã được xác định:

1.  **Định dạng không đồng nhất:** CSV, TSV (tab-separated), NDJSON, HTML.
2.  **Biểu diễn giá trị Null:**
    - MovieLens: Chuỗi rỗng.
    - IMDb: Ký tự `\N`.
    - JSON: `null`.
3.  **Hệ thống định danh (ID) phân mảnh:**
    - MovieLens dùng `movieId` (Int).
    - IMDb dùng `tconst` (String `tt0114709`).
    - Rotten Tomatoes dùng `slug` (`/m/toy_story`).
4.  **Chất lượng dữ liệu (Data Quality):**
    - Doanh thu (Box Office) dạng chuỗi: `$100M`, `$1.2B`.
    - Thiếu liên kết giữa Rotten Tomatoes và các nguồn còn lại.

= 3. Kiến trúc Hệ thống & Tích hợp

== Kiến trúc High-Level

#figure(
    image("architec.png", width: 100%),
  caption: [Kiến trúc tổng quan hệ thống]
)


== Giải pháp Tích hợp: Entity Resolution (1/2)

Khó khăn lớn nhất là ánh xạ (Mapping) một bộ phim giữa các nguồn.

*Chiến lược "Xương sống" (The Spine Strategy):*
- Sử dụng file `links.csv` của MovieLens làm nguồn sự thật (Source of Truth).
- Mapping trực tiếp: `MovieLens ID` $arrow.l.r$ `IMDb ID` $arrow.l.r$ `TMDB ID`.

*Code Logic xử lý ID Normalization:*

= 4. Xử lý Dữ liệu (ETL với Apache Spark)

== Kiến trúc Medallion (Bronze → Silver → Gold)

Sử dụng kiến trúc *Medallion Architecture* với 3 tầng xử lý trên AWS EMR:

#table(
  columns: (1fr, 1fr, 2fr),
  inset: 10pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Tầng*], [*Định dạng*], [*Mục đích*],
  [**Bronze**], [Parquet], [Parsing thô, chuẩn hóa null, thêm `ingest_date`],
  [**Silver**], [Parquet], [Làm sạch, chuẩn hóa ID, Entity Resolution],
  [**Gold**], [Parquet], [Star Schema (Dimensions + Facts + Bridges)],
  [**PostgreSQL**], [JDBC], [Data Warehouse phục vụ Analytics],
)

#v(1em)

*Công nghệ:*
- *Apache Spark 3.5+* trên AWS EMR (1 Primary + 1 Core Node)
- *S3* làm Data Lake lưu trữ Parquet
- *Supabase PostgreSQL* làm Data Warehouse cuối

== Bronze Layer: Raw → Parquet

*Mục tiêu:* Chuyển đổi dữ liệu thô sang định dạng Parquet với schema được định nghĩa sẵn.

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: red.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Vấn đề: Null Heterogeneity* \
      Mỗi nguồn biểu diễn null khác nhau:
      - MovieLens: Chuỗi rỗng `""`
      - IMDb: Ký tự đặc biệt `\N`
      - JSON sources: `null`
    ]
  ],
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Giải pháp: Unified Null Handling* \
      ```python
      df = spark.read.csv(
        path, header=True, sep="\t",
        nullValue="\\N"  # IMDb specific
      )
      ```
    ]
  ]
)

#v(0.5em)

#table(
  columns: (1.5fr, 1fr, 1fr, 2fr),
  inset: 8pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Job*], [*Input*], [*Output*], [*Xử lý chính*],
  [`ingest_movielens.py`], [CSV], [Parquet], [Add `ingest_date`, preserve schema],
  [`ingest_imdb.py`], [TSV], [Parquet], [Replace `\N` → `null`],
  [`ingest_tmdb.py`], [NDJSON], [Parquet], [Flatten JSON structure],
  [`ingest_rotten_tomatoes.py`], [NDJSON], [Parquet], [Preserve nullable fields],
)

== Silver Layer: Entity Resolution (1/2)

*Vấn đề cốt lõi:* Mỗi nguồn dùng hệ thống ID riêng biệt, không tương thích.

#table(
  columns: (1.5fr, 1.5fr, 1fr, 1.5fr),
  inset: 10pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Nguồn*], [*Primary ID*], [*Kiểu*], [*Ví dụ*],
  [MovieLens], [`movieId`], [Integer], [`1`],
  [IMDb], [`tconst`], [String], [`tt0114709`],
  [TMDB], [`id`], [Integer], [`862`],
  [Rotten Tomatoes], [`rt_id`], [Slug], [`toy_story`],
)

#v(0.5em)

*Giải pháp: Entity Spine Table* \
Xây dựng bảng ánh xạ trung tâm từ `links.csv` của MovieLens:

```
movie_id (hash) │ ml_id │ imdb_id    │ tmdb_id │ rt_slug
────────────────┼───────┼────────────┼─────────┼──────────
  xxhash64()    │   1   │ tt0114709  │   862   │ toy_story
```

== Silver Layer: Entity Resolution (2/2)

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: red.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Vấn đề: IMDb ID Format* \
      MovieLens lưu `imdbId = 114709` (Integer) \
      IMDb dùng `tconst = tt0114709` (String có prefix)
    ]
  ],
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Giải pháp: ID Normalization* \
      ```python
      imdb_id = concat(
        lit("tt"),
        lpad(col("imdbId"), 7, "0")
      )
      # 114709 → tt0114709
      ```
    ]
  ]
)

#v(0.5em)

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: red.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Vấn đề: RT không có IMDb ID* \
      Rotten Tomatoes không cung cấp ID mapping với các nguồn khác
    ]
  ],
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Giải pháp: Title + Year Fuzzy Match* \
      ```python
      # Normalize: lowercase, no punct
      normalize_title(col("title"))
      # Match: title + year exact
      on=(norm_title, year)
      ```
    ]
  ]
)

== Silver Layer: Data Cleaning

*Các phép biến đổi chính cho từng nguồn dữ liệu:*

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *MovieLens Cleaning* \
      - Tách năm từ title: `"Toy Story (1995)"` → `year=1995`
      - Split genres: `"Action|Comedy"` → `["Action", "Comedy"]`
      - Aggregate ratings: `AVG(rating)`, `COUNT(*)`
    ]
  ],
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *IMDb Cleaning* \
      - Filter: `titleType = 'movie'` only
      - Cast types: `startYear` → Integer
      - Split genres: `"Action,Comedy"` → Array
    ]
  ]
)

#v(0.5em)

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Rotten Tomatoes Cleaning* \
      - Parse Box Office: `$100M` → `100000000`
      - Divisive Score: `|tomatometer - audience|`
      - Split directors: `"Bush, Howard"` → Array
    ]
  ],
  [
    #block(fill: orange.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *TMDB Cleaning* \
      - Build poster URL: `/path.jpg` → Full URL
      - Map `genre_ids` (int) → genre names
      - Extract year from `release_date`
    ]
  ]
)

== Silver Layer: Box Office Parsing

*Vấn đề:* Doanh thu được lưu dạng chuỗi không chuẩn hóa.

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: red.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Các định dạng gặp phải:*
      - `$100M` (Million)
      - `$1.2B` (Billion)
      - `$950K` (Thousand)
      - `$12,345,678` (Raw number)
    ]
  ],
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Giải pháp: UDF Parser*
      ```python
      def parse_box_office(value):
        multipliers = {
          "K": 1_000,
          "M": 1_000_000,
          "B": 1_000_000_000
        }
        # $100M → 100000000
      ```
    ]
  ]
)

#v(0.5em)

*Kết quả:* Chuyển đổi thành công ~85% các giá trị box office raw thành số nguyên.

== Gold Layer: Star Schema Design

*Mô hình Star Schema* cho Data Warehouse:

#align(center)[
  #block(fill: luma(240), inset: 15pt, radius: 5pt)[
    ```
                    ┌─────────────┐
                    │  dim_time   │
                    └──────┬──────┘
                           │
    ┌───────────┐   ┌──────┴──────┐   ┌───────────┐
    │dim_person │───│fact_metrics │───│ dim_genre │
    └───────────┘   └──────┬──────┘   └───────────┘
                           │
                    ┌──────┴──────┐
                    │  dim_movie  │
                    └─────────────┘
    ```
  ]
]

*Các bảng Bridge (Many-to-Many):*
- `bridge_movie_cast`: Movie ↔ Person (với `category`, `ordering`)
- `bridge_movie_genres`: Movie ↔ Genre

== Gold Layer: Dimension Tables

#table(
  columns: (1.2fr, 2fr, 2fr),
  inset: 8pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Dimension*], [*Key Columns*], [*Source Priority*],
  [`dim_movie`], [`movie_id`, `title`, `year`, `runtime`, `poster_url`], [IMDb > MovieLens > TMDB > RT],
  [`dim_person`], [`person_id`, `name`, `birth_year`, `primary_profession`], [IMDb `name.basics`],
  [`dim_genre`], [`genre_id`, `genre_name`], [Canonical mapping file],
  [`dim_time`], [`time_id`, `year`, `quarter`, `month`], [Generated (1950-2030)],
)

#v(0.5em)

*Data Fusion trong `dim_movie`:*
```python
# Ưu tiên nguồn theo thứ tự độ tin cậy
title = coalesce(imdb_title, ml_title, tmdb_title, rt_title)
year  = coalesce(imdb_year, ml_year, tmdb_year, rt_year)
```

== Gold Layer: Fact Table

*`fact_movie_metrics`* - Tổng hợp tất cả metrics từ các nguồn:

#table(
  columns: (1.5fr, 1fr, 2fr),
  inset: 8pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Metric*], [*Type*], [*Source*],
  [`ml_avg_rating`], [FLOAT], [MovieLens `AVG(rating)`],
  [`ml_rating_count`], [INT], [MovieLens `COUNT(*)`],
  [`imdb_rating`], [FLOAT], [IMDb `averageRating`],
  [`imdb_votes`], [INT], [IMDb `numVotes`],
  [`tomatometer_score`], [INT], [RT Critics (0-100)],
  [`audience_score`], [INT], [RT Audience (0-100)],
  [`divisive_score`], [INT], [Calculated: `|tomatometer - audience|`],
  [`box_office_revenue`], [BIGINT], [RT (parsed to integer)],
)

== Gold Layer: Divisive Score Metric

*Metric độc đáo:* Đo lường sự "gây tranh cãi" của phim.

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Công thức:*
      $ "divisive\_score" = |"tomatometer" - "audience"| $

      *Ý nghĩa:*
      - Score cao = Phim gây chia rẽ (critics vs audience)
      - Score thấp = Đồng thuận cao
    ]
  ],
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Ví dụ phim "Divisive":*
      - Critics yêu, khán giả ghét
      - Hoặc ngược lại

      *Code:*
      ```python
      divisive = spark_abs(
        col("tomatometer") -
        col("audience")
      )
      ```
    ]
  ]
)

== ETL Pipeline: Job Dependencies

*Thứ tự thực thi các Spark jobs:*

#align(center)[
  #block(fill: luma(240), inset: 12pt, radius: 5pt)[
    ```
    Raw Data (S3)
         │
    ┌────┴────┬────────────┬───────────┐
    ▼         ▼            ▼           ▼
  Bronze   Bronze       Bronze     Bronze
  (ML)    (IMDb)       (TMDB)      (RT)
    │         │            │           │
    └────┬────┴────────────┴───────────┘
         ▼
    Entity Spine (links.csv mapping)
         │
    ┌────┴────┬────────────┬───────────┐
    ▼         ▼            ▼           ▼
  Silver   Silver       Silver     Silver
  (ML)    (IMDb)       (TMDB)      (RT)
         │
    ┌────┴────┬────────────┬───────────┐
    ▼         ▼            ▼           ▼
  dim_     dim_        dim_       dim_
  time    genre       person     movie
         │
    ┌────┴────┬────────────┐
    ▼         ▼            ▼
   fact    bridge      bridge
  metrics   cast       genres
         │
         ▼
    PostgreSQL (JDBC Load)
    ```
  ]
]

== Load to PostgreSQL

*Bước cuối: Load Gold Layer vào PostgreSQL via JDBC*

```python
TABLES = ["dim_time", "dim_genre", "dim_person", "dim_movie",
          "fact_movie_metrics", "bridge_movie_cast", "bridge_movie_genres"]

for table in TABLES:
    df = spark.read.parquet(f"s3://bucket/gold/{table}")
    df.write.jdbc(url=jdbc_url, table=table, mode="overwrite",
                  properties={"batchsize": "10000"})
```

#v(0.5em)

#table(
  columns: (2fr, 1fr, 2fr),
  inset: 8pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Table*], [*~Rows*], [*Primary Key*],
  [`dim_movie`], [~87,000], [`movie_id` (BIGINT)],
  [`dim_person`], [~500,000], [`person_id` (TEXT)],
  [`fact_movie_metrics`], [~87,000], [`metric_id` (BIGINT)],
  [`bridge_movie_cast`], [~2,000,000], [`(movie_id, person_id, ordering)`],
)

== Tổng kết ETL Processing

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Đã giải quyết:*
      - ✓ Chuẩn hóa 4 định dạng file khác nhau
      - ✓ Entity Resolution qua ID mapping
      - ✓ Xử lý null heterogeneity
      - ✓ Parse box office strings
      - ✓ Build Star Schema hoàn chỉnh
    ]
  ],
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Công nghệ sử dụng:*
      - Apache Spark 3.5 (PySpark)
      - AWS EMR Cluster
      - S3 Data Lake (Parquet)
      - PostgreSQL (Supabase)
      - JDBC Batch Loading
    ]
  ]
)

#v(0.5em)

*Kết quả:* Pipeline ETL hoàn chỉnh từ 4 nguồn dữ liệu không đồng nhất → Star Schema phục vụ Analytics.

= 5. Khám phá Tri thức & Xây dựng Dashboard

== Kiến trúc Backend API

*REST API* phục vụ Dashboard analytics, xây dựng với FastAPI + PostgreSQL:

#table(
  columns: (1.5fr, 2fr, 2fr),
  inset: 8pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Module*], [*Endpoints*], [*Mục đích*],
  [**Overview**], [`/movies-per-year`, `/top-popular`, `/language-distribution`], [Tổng quan thị trường],
  [**Ratings**], [`/distribution`, `/platform-comparison`, `/cult-classics`], [Phân tích đánh giá],
  [**Genres**], [`/share-by-decade`, `/average-rating`, `/co-occurrence`], [Xu hướng thể loại],
  [**People**], [`/top-prolific`, `/top-rated`, `/actor-network`], [Phân tích nhân vật],
  [**Temporal**], [`/runtime-trend`, `/quality-by-month`, `/mpaa-distribution`], [Phân tích thời gian],
)

#v(0.5em)

*Tech Stack:* FastAPI + asyncpg + Supabase PostgreSQL

== Insight 1: Xu hướng Sản lượng Phim theo Năm

*API:* `GET /api/overview/movies-per-year`

```sql
SELECT year, COUNT(*) as count FROM dim_movie
WHERE year BETWEEN $1 AND $2 GROUP BY year ORDER BY year
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - Sản lượng phim tăng đột biến từ năm 2000
      - Đỉnh cao: 2015-2019 (trước COVID)
      - Sụt giảm rõ rệt 2020-2021
      - Phục hồi từ 2022
    ]
  ],
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Line Chart \
      *X-axis:* Year (1950-2025) \
      *Y-axis:* Movie count \
      *Insight:* Xu hướng tăng trưởng ngành công nghiệp điện ảnh
    ]
  ]
)

== Insight 2: So sánh Rating giữa các Platform

*API:* `GET /api/ratings/platform-comparison`

```sql
SELECT m.year, AVG(f.imdb_rating) as imdb_avg,
       AVG(f.tmdb_rating) as tmdb_avg, AVG(f.ml_avg_rating) as ml_avg
FROM dim_movie m JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
GROUP BY m.year ORDER BY m.year
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - IMDb có xu hướng cho điểm cao hơn TMDB
      - MovieLens (thang 5) x2 ≈ tương đương IMDb
      - Rating trung bình giảm dần theo thời gian
      - "Rating inflation" ở các phim cũ
    ]
  ],
  [
    #block(fill: orange.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Multi-line Chart \
      *3 đường:* IMDb, TMDB, MovieLens \
      *Insight:* Sự khác biệt đánh giá giữa các cộng đồng
    ]
  ]
)

== Insight 3: Phân bố Rating (Histogram)

*API:* `GET /api/ratings/distribution?source=imdb`

```sql
SELECT FLOOR(imdb_rating)::int as bin, COUNT(*) as count
FROM fact_movie_metrics WHERE imdb_rating IS NOT NULL
GROUP BY bin ORDER BY bin
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - Phân bố lệch phải (right-skewed)
      - Đỉnh tập trung ở 6-7 điểm
      - Rất ít phim dưới 4 điểm (survivorship bias)
      - Phim 8+ điểm chiếm ~15%
    ]
  ],
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Histogram/Bar Chart \
      *X-axis:* Rating bins (0-10) \
      *Y-axis:* Movie count \
      *Insight:* Hiểu phân bố chất lượng phim
    ]
  ]
)

== Insight 4: Cult Classics - Viên ngọc Ẩn

*API:* `GET /api/ratings/cult-classics?min_rating=8.0&max_votes=10000`

```sql
SELECT m.title, m.year, f.imdb_rating, f.imdb_votes
FROM dim_movie m JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
WHERE f.imdb_rating >= $1 AND f.imdb_votes <= $2 AND f.imdb_votes > 0
ORDER BY f.imdb_rating DESC, f.imdb_votes ASC
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: red.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Định nghĩa "Cult Classic":*
      - Rating cao (≥ 8.0)
      - Số vote thấp (≤ 10,000)
      - Phim chất lượng nhưng ít người biết
    ]
  ],
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Ứng dụng:*
      - Recommendation System
      - Khám phá phim mới
      - Hidden gems discovery
    ]
  ]
)

== Insight 5: Thị phần Thể loại theo Thập kỷ

*API:* `GET /api/genres/share-by-decade`

```sql
SELECT (m.year / 10) * 10 as decade, g.genre_name, COUNT(*) as count
FROM dim_movie m
JOIN bridge_movie_genres bg ON m.movie_id = bg.movie_id
JOIN dim_genre g ON bg.genre_id = g.genre_id
GROUP BY decade, g.genre_name ORDER BY decade, count DESC
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - Drama luôn chiếm ưu thế qua các thập kỷ
      - Action tăng mạnh từ 1980s
      - Horror boom trong 2010s
      - Documentary tăng trưởng gần đây
    ]
  ],
  [
    #block(fill: orange.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Stacked Area Chart \
      *X-axis:* Decades (1920-2020) \
      *Y-axis:* Movie count (stacked) \
      *Insight:* Sự thay đổi thị hiếu khán giả
    ]
  ]
)

== Insight 6: Ma trận Kết hợp Thể loại

*API:* `GET /api/genres/co-occurrence`

```sql
SELECT g1.genre_name as genre1, g2.genre_name as genre2, COUNT(*) as count
FROM bridge_movie_genres bg1
JOIN bridge_movie_genres bg2 ON bg1.movie_id = bg2.movie_id
  AND bg1.genre_id < bg2.genre_id
JOIN dim_genre g1 ON bg1.genre_id = g1.genre_id
JOIN dim_genre g2 ON bg2.genre_id = g2.genre_id
GROUP BY g1.genre_name, g2.genre_name HAVING COUNT(*) > 50
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - Drama + Romance: Kết hợp phổ biến nhất
      - Action + Thriller: Cặp đôi kinh điển
      - Comedy + Romance: Rom-com formula
      - Sci-Fi + Action: Blockbuster pattern
    ]
  ],
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Heatmap/Matrix \
      *Rows/Cols:* Genre names \
      *Value:* Co-occurrence count \
      *Insight:* Genre pairing patterns
    ]
  ]
)

== Insight 7: Top Diễn viên/Đạo diễn

*API:* `GET /api/people/top-prolific?category=actor`

```sql
SELECT p.name, COUNT(DISTINCT bc.movie_id) as movie_count
FROM dim_person p
JOIN bridge_movie_cast bc ON p.person_id = bc.person_id
WHERE bc.category = $1
GROUP BY p.person_id, p.name ORDER BY movie_count DESC
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Top Prolific (Số lượng):*
      - Diễn viên đóng nhiều phim nhất
      - Đạo diễn làm việc nhiều nhất
      - Career longevity analysis
    ]
  ],
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Top Rated (Chất lượng):*
      - Diễn viên có rating TB cao nhất
      - Filter: tối thiểu 5 phim
      - Quality vs Quantity analysis
    ]
  ]
)

== Insight 8: Mạng lưới Hợp tác Diễn viên

*API:* `GET /api/people/actor-network?min_collaborations=3`

```sql
SELECT p1.name as actor1, p2.name as actor2, COUNT(*) as collaborations
FROM bridge_movie_cast bc1
JOIN bridge_movie_cast bc2 ON bc1.movie_id = bc2.movie_id
  AND bc1.person_id < bc2.person_id
JOIN dim_person p1 ON bc1.person_id = p1.person_id
JOIN dim_person p2 ON bc2.person_id = p2.person_id
WHERE bc1.category = 'actor' AND bc2.category = 'actor'
GROUP BY p1.name, p2.name HAVING COUNT(*) >= $1
```

#block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
  *Biểu đồ:* Network Graph (Force-directed) \
  *Nodes:* Actors \
  *Edges:* Collaboration count \
  *Insight:* Khám phá các "cặp đôi vàng" và cliques trong Hollywood
]

== Insight 9: Xu hướng Runtime theo Thời gian

*API:* `GET /api/temporal/runtime-trend`

```sql
SELECT year, AVG(runtime) as avg_runtime, COUNT(*) as movie_count
FROM dim_movie WHERE runtime BETWEEN 30 AND 300
GROUP BY year ORDER BY year
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - Runtime TB tăng từ 1950s đến nay
      - Phim 1950s: ~90 phút
      - Phim 2020s: ~110-115 phút
      - Blockbuster effect
    ]
  ],
  [
    #block(fill: orange.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Line Chart with Band \
      *X-axis:* Year \
      *Y-axis:* Average runtime (minutes) \
      *Insight:* Phim ngày càng dài hơn
    ]
  ]
)

== Insight 10: Phân tích Mùa Oscar

*API:* `GET /api/temporal/quality-by-month`

```sql
SELECT t.month, AVG(f.imdb_rating) as avg_rating, COUNT(*) as movie_count
FROM fact_movie_metrics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.imdb_rating IS NOT NULL GROUP BY t.month ORDER BY t.month
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - Tháng 11-12: Rating cao nhất (Oscar bait)
      - Tháng 1-2: "Dump months" rating thấp
      - Mùa hè: Blockbuster season
      - Pattern rõ ràng theo mùa
    ]
  ],
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Bar Chart / Polar Chart \
      *X-axis:* Month (1-12) \
      *Y-axis:* Average rating \
      *Insight:* Chiến lược phát hành phim
    ]
  ]
)

== Insight 11: Phân bố MPAA Rating

*API:* `GET /api/temporal/mpaa-distribution`

```sql
SELECT mpaa_rating, COUNT(*) as count, AVG(f.imdb_rating) as avg_rating
FROM dim_movie m LEFT JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
WHERE mpaa_rating IS NOT NULL GROUP BY mpaa_rating ORDER BY count DESC
```

#table(
  columns: (1fr, 1.5fr, 1.5fr, 2fr),
  inset: 8pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*MPAA*], [*Count*], [*Avg Rating*], [*Insight*],
  [R], [~25,000], [6.5], [Chiếm ưu thế, mature content],
  [PG-13], [~18,000], [6.2], [Mainstream blockbusters],
  [PG], [~8,000], [6.0], [Family-friendly],
  [G], [~2,000], [6.8], [Ít phim, thường animation],
  [NC-17], [~500], [5.5], [Rất ít, controversial],
)

== Insight 12: Runtime vs Rating Correlation

*API:* `GET /api/ratings/runtime-vs-rating?sample_size=1000`

```sql
SELECT m.runtime, f.imdb_rating as rating
FROM dim_movie m JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
WHERE m.runtime BETWEEN 30 AND 300 AND f.imdb_rating IS NOT NULL
ORDER BY RANDOM() LIMIT $1
```

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Phát hiện:*
      - Tương quan dương yếu
      - Phim dài hơn *có xu hướng* rating cao hơn
      - Sweet spot: 100-150 phút
      - Outliers ở cả hai đầu
    ]
  ],
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Biểu đồ:* Scatter Plot \
      *X-axis:* Runtime (minutes) \
      *Y-axis:* IMDb Rating \
      *Insight:* Phim dài = phim hay?
    ]
  ]
)

== Dashboard Demo

*Giao diện Dashboard* được xây dựng với Next.js + Recharts:

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Section 1: Tổng quan thị trường*
      - Movies per year (*Area Chart*)
      - Language distribution (*Treemap*)
      - Top 10 Popular movies (*Cards Grid*)
    ]
  ],
  [
    #block(fill: luma(240), inset: 10pt, radius: 5pt, width: 100%)[
      *Section 2: Chất lượng & Đánh giá*
      - Rating distribution (*Bar Chart* + Reference Line)
      - Runtime vs Rating (*Scatter Chart*)
      - Platform comparison (*Multi-Line Chart*)
      - Cult classics (*Data Table*)
    ]
  ]
)

#v(0.5em)

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Section 3: Xu hướng thể loại*
      - Genre share by decade (*Stacked Area Chart*)
      - Genre ratings (*Horizontal Bar Chart*)
      - Co-occurrence (*Data Table*)
    ]
  ],
  [
    #block(fill: orange.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Section 4: Phân tích nhân sự*
      - Top Prolific actors/directors (*Data Table*)
      - Top Rated actors/directors (*Data Table*)
      - Actor network collaborations (*Data Table*)
    ]
  ]
)

#v(0.5em)

#block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
  *Section 5: Phân tích thời gian*
  - Runtime trend by year (*Line Chart*)
]

== Tổng kết Tri thức Khám phá

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    #block(fill: green.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Giá trị từ Data Integration:*
      - ✓ So sánh rating đa nguồn
      - ✓ Phát hiện cult classics
      - ✓ Phân tích xu hướng thể loại
      - ✓ Mạng lưới hợp tác nghệ sĩ
      - ✓ Phân tích mùa phát hành
    ]
  ],
  [
    #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
      *Insights không thể có từ 1 nguồn:*
      - Divisive Score (RT only)
      - Rating correlation across platforms
      - Box office vs Critic score
      - Complete cast/crew network
      - Multi-dimensional popularity
    ]
  ]
)

#v(0.5em)

*Kết luận:* Việc tích hợp 4 nguồn dữ liệu cho phép khám phá tri thức mà không thể có được từ bất kỳ nguồn đơn lẻ nào.

= Appendix

== Chi tiết dữ liệu
