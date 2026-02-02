#import "@preview/slydst:0.1.5": *

#show: slides.with(
  title: "Hệ thống Phân tích Dữ liệu lớn & Khám phá Tri thức Phim ảnh",
  subtitle: "Big Data Integration - GVHD: TS. Vũ Tuyết Trinh",
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
    image("movie-architecture.png", width: 50%),
  caption: [Kiến trúc tổng quan hệ thống]
)

== Pha Thu thập Dữ liệu (Ingestion)


*Mục tiêu:* Thu thập dữ liệu thô từ 4 nguồn và lưu vào S3 Raw Zone.



#table(
  columns: (1fr, 1fr, 1fr, 1.5fr),
  inset: 10pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Nguồn*], [*Script*], [*Output*], [*Đích S3*],
  [MovieLens], [`ingestion/movielens`], [CSV], [`raw/movielens/{date}/`],
  [IMDb], [`ingestion/imdb`], [TSV], [`raw/imdb/{date}/`],
  [Rotten Tomatoes], [`ingestion/rotten_tomatoes`], [NDJSON], [`raw/rotten_tomatoes/{date}/`],
  [TMDB], [`ingestion/tmdb`], [NDJSON], [`raw/tmdb/{date}/`],
)



== Kỹ thuật xử lý lỗi & Khả năng phục hồi

#table(
  columns: (1.2fr, 2fr, 2fr),
  inset: 10pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Kỹ thuật*], [*Mô tả*], [*Áp dụng*],
  [*Retry với Backoff*], [Thử lại với delay tăng dần: 2s $arrow$ 4s $arrow$ 8s], [Tất cả nguồn],
  [*Checksum Detection*], [So sánh MD5 để phát hiện thay đổi], [MovieLens, IMDb],
  [*SQLite State*], [Lưu trạng thái crawl để resume], [Rotten Tomatoes],
  [*JSON State File*], [Lưu year/page đang xử lý], [TMDB],
  [*Rotating Headers*], [Thay đổi User-Agent mỗi request], [Rotten Tomatoes],
  [*Chunked Upload*], [Upload từng phần 1000 records], [RT, TMDB],
)

== Kết quả Thu thập Dữ liệu

#table(
  columns: (1fr, 1.5fr, 1fr, 1.5fr),
  inset: 10pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*Nguồn*], [*Số lượng*], [*Dung lượng*], [*Files*],
  
  [MovieLens], [87k movies\ 32M ratings\ 2M tags], [~300MB], [4 CSV files\ (ratings, movies, tags, links)],
  
  [IMDb], [~10M titles\ ~12M persons\ Cast/crew relationships\ Global ratings], [~4GB], [7 TSV files\ (title.basics, title.ratings,\ name.basics, title.principals,\ title.crew, title.akas,\ title.episode)],
  
  [Rotten Tomatoes], [~200 movies\ Critics + Audience scores\ Box office data], [~20MB], [NDJSON chunks\ + metadata.json],
  
  [TMDB], [~600k movies\ Posters + metadata\ Years 1950-present], [~500MB], [NDJSON chunks\ + metadata.json],
)<ingestion-results>

#text(size: 9pt)[
  *Ghi chú:* IMDb bao gồm thông tin về ~10M titles (movies, TV series, episodes) và ~12M persons (actors, directors, writers) với các mối quan hệ cast/crew được lưu trong title.principals và title.crew.
]


*Output location:* `s3://movies-datalake-2310/raw/{source}/{date}/`


== Xử lý Dữ liệu (ETL với Apache Spark)

=== Kiến trúc Bronze → Silver → Gold

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

=== Bronze Layer: Raw → Parquet

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

#v(1.5em)

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


== Gold Layer: Star Schema Design

#figure(
    image("schema.png", width: 100%),
  caption: [Data warehouse schema]
)


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

= 4. Khám phá Tri thức, xây dựng Dashboard


== Xu hướng Sản lượng Phim theo Năm


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
  // [
  //   #block(fill: blue.lighten(90%), inset: 10pt, radius: 5pt, width: 100%)[
  //     *Biểu đồ:* Line Chart \
  //     *X-axis:* Year (1950-2025) \
  //     *Y-axis:* Movie count \
  //     *Insight:* Xu hướng tăng trưởng ngành công nghiệp điện ảnh
  //   ]
  // ]
)

#image("CleanShot 2026-02-01 at 11.38.41@2x.png")



== Xu hướng Runtime theo Thời gian


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
#image("CleanShot 2026-02-01 at 11.42.05@2x.png")

Xem đầy đủ các xu hướng khác tại: https://big-movies.vercel.app/

== Dashboard Demo

*Giao diện Dashboard* thể hiện tất cả các insight

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



== Cảm ơn thầy cô và các bạn đã lắng nghe

= Apendix
==

Link website: https://big-movies.vercel.app/

Link source code: https://github.com/betty2310/big-movies/




== Chi tiết dữ liệu ingestion
=== Movielens
- *URL*: `https://files.grouplens.org/datasets/movielens/ml-32m.zip`
- *Format*: ZIP archive containing CSV files
- *Collection Method*: Periodic download with checksum comparison

*Files Collected*

#table(
  columns: (1.5fr, 3fr),
  inset: 8pt,
  align: horizon,
  fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
  [*File*], [*Description*],
  [`ratings.csv`], [User ratings (32M+ entries)],
  [`movies.csv`], [Movie metadata (87k movies)],
  [`tags.csv`], [User-generated tags (2M+ tags)],
  [`links.csv`], [External ID mappings (IMDb, TMDB)],
)

=== Schemas (MovieLens)

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    *ratings.csv*
    #table(
      columns: (1fr, 1fr, 2fr),
      inset: 6pt,
      fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
      [*Field*], [*Type*], [*Description*],
      [`userId`], [Integer], [Unique user identifier],
      [`movieId`], [Integer], [MovieLens movie ID],
      [`rating`], [Float], [0.5-5.0 scale],
      [`timestamp`], [Integer], [Unix timestamp],
    )
  ],
  [
    *movies.csv*
    #table(
      columns: (1fr, 1fr, 2fr),
      inset: 6pt,
      fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
      [*Field*], [*Type*], [*Description*],
      [`movieId`], [Integer], [Primary key],
      [`title`], [String], [Title with year],
      [`genres`], [String], [Pipe-separated],
    )
  ]
)

#v(1em)

#grid(
  columns: (1fr, 1fr),
  gutter: 1em,
  [
    *tags.csv*
    #table(
      columns: (1fr, 1fr, 2fr),
      inset: 6pt,
      fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
      [*Field*], [*Type*], [*Description*],
      [`userId`], [Integer], [Tagging user],
      [`movieId`], [Integer], [Movie ID],
      [`tag`], [String], [Free-form text],
    )
  ],
  [
    *links.csv*
    #table(
      columns: (1fr, 1fr, 2fr),
      inset: 6pt,
      fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
      [*Field*], [*Type*], [*Description*],
      [`movieId`], [Integer], [MovieLens ID],
      [`imdbId`], [String], [IMDb ID],
      [`tmdbId`], [Integer], [TMDB ID],
    )
  ]
)


=== IMDb

Base URL: ⁠https://datasets.imdbws.com

Format: Gzipped TSV files (tab-separated)

Collection Method: Daily download of public datasets
Files Collected
#table(
columns: (1.5fr, 3fr),
inset: 8pt,
align: horizon,
fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
[File], [Description],
[⁠name.basics.tsv], [Person information (actors, directors, etc.)],
[⁠title.akas.tsv], [Alternative titles by region],
[⁠title.basics.tsv], [Core movie metadata],
[⁠title.crew.tsv], [Director and writer associations],
[⁠title.episode.tsv], [TV episode information],
[⁠title.principals.tsv], [Principal cast/crew for titles],
[⁠title.ratings.tsv], [IMDb ratings and vote counts],
)
=== Schemas (IMDb)
#grid(
columns: (1fr, 1fr),
gutter: 1em,
[
title.basics.tsv
#table(
columns: (1fr, 2fr),
inset: 6pt,
[Field], [Description],
[⁠tconst], [Unique title ID (tt...)],
[⁠titleType], [movie, short, tvSeries],
[⁠primaryTitle], [Display title],
[⁠originalTitle], [Original language title],
[⁠startYear], [Release year],
[⁠genres], [Comma-separated genres],
)
],
[
title.ratings.tsv & name.basics.tsv
#table(
columns: (1fr, 2fr),
inset: 6pt,
[Field], [Description],
[⁠averageRating], [Weighted average (0-10)],
[⁠numVotes], [Total number of votes],
[⁠nconst], [Person ID (nm...)],
[⁠primaryName], [Name as credited],
[⁠knownForTitles], [Comma-separated title IDs],
)
]
)

=== Rotten Tomatoes
=== Source & Strategy
	Base URL: ⁠https://www.rottentomatoes.com 
 
 Format: HTML pages (unstructured)
 
 Collection Method: Web scraping with rate limiting

=== Schema: MovieRecord (Output)
#table(
columns: (1.2fr, 1fr, 2.5fr),
inset: 8pt,
align: horizon,
fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
[Field], [Type], [Description],
[⁠rt_id], [String], [Rotten Tomatoes identifier (slug)],
[⁠slug], [String], [URL slug (e.g., "the_godfather")],
[⁠title], [String], [Movie title],
[⁠tomatometer_score], [Int | null], [Critics score (0-100%)],
[⁠audience_score], [Int | null], [Audience score (0-100%)],
[⁠box_office], [String | null], [Revenue (e.g., "\$100M")],
[⁠release_date], [String | null], [Theatrical release date],
[⁠director], [String | null], [Director name(s)],
[⁠genre], [String | null], [Primary genre(s)],
[⁠scraped_at], [String], [ISO timestamp],
)

=== TMDB (The Movie Database)
=== Source & API
Base URL: ⁠https://api.themoviedb.org/3

Format: JSON API responses

Method: REST API with bearer token authentication

````
 GET /discover/movie?primary_release_year={year}&sort_by=popularity.desc&page={page}&include_adult=false
````
  === Schema: TMDB Movie Response
#table(
columns: (1.2fr, 1fr, 2.5fr),
inset: 8pt,
align: horizon,
fill: (_, row) => if calc.odd(row) { luma(240) } else { white },
[Field], [Type], [Description],
[⁠id], [Integer], [TMDB movie ID],
[⁠title], [String], [Movie title],
[⁠overview], [String], [Plot summary],
[⁠poster_path], [String], [Poster image path],
[⁠release_date], [String], [Release date (YYYY-MM-DD)],
[⁠popularity], [Float], [Popularity score],
[⁠vote_average], [Float], [Average user rating (0-10)],
[⁠genre_ids], [Array], [Array of genre ID integers],
[⁠original_language], [String], [ISO 639-1 language code],
)
