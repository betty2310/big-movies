import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { Database } from "bun:sqlite";
import * as cheerio from "cheerio";
import { createReadStream } from "node:fs";
import { writeFile, rm, stat, mkdir } from "node:fs/promises";
import { join } from "node:path";

const BUCKET = "movies-datalake-2310";
const REGION = "ap-southeast-1";
const RAW_ZONE_PREFIX = "raw/rotten_tomatoes";
const BASE_URL = "https://www.rottentomatoes.com";
const DATA_DIR = "./rt-data";
const DB_FILE = "./rt_crawl_state.db";
const CHUNK_SIZE = 1000;
const MIN_DELAY_MS = 800;
const MAX_DELAY_MS = 2000;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 3000;
const START_YEAR = 1950;

const s3Client = new S3Client({ region: REGION });

const USER_AGENTS = [
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
];

const GENRES = [
  "action",
  "adventure",
  "animation",
  "comedy",
  "crime",
  "documentary",
  "drama",
  "fantasy",
  "history",
  "horror",
  "music",
  "mystery",
  "romance",
  "scifi",
  "sport",
  "thriller",
  "war",
  "western",
];

const CRITICS_FILTERS = ["certified_fresh", "fresh", "rotten"];
const AUDIENCE_FILTERS = ["upright", "spilled"];
const SORT_OPTIONS = ["newest", "popular", "a_z"];

function generateBrowseUrls(): string[] {
  const urls: string[] = [];
  const bases = ["/browse/movies_at_home", "/browse/movies_in_theaters"];

  for (const base of bases) {
    urls.push(base);

    for (const sort of SORT_OPTIONS) {
      urls.push(`${base}/sort:${sort}`);
    }

    for (const genre of GENRES) {
      urls.push(`${base}/genres:${genre}`);
      urls.push(`${base}/genres:${genre}~sort:newest`);
      urls.push(`${base}/genres:${genre}~sort:popular`);
    }

    for (const critic of CRITICS_FILTERS) {
      urls.push(`${base}/critics:${critic}`);
      for (const genre of GENRES) {
        urls.push(`${base}/critics:${critic}~genres:${genre}`);
      }
    }

    for (const audience of AUDIENCE_FILTERS) {
      urls.push(`${base}/audience:${audience}`);
    }
  }

  urls.push("/browse/movies_coming_soon");

  for (const genre of GENRES) {
    urls.push(`/browse/movies_coming_soon/genres:${genre}`);
  }

  return urls;
}

const BROWSE_URLS = generateBrowseUrls();

interface MovieRecord {
  rt_id: string;
  slug: string;
  title: string;
  tomatometer_score: number | null;
  audience_score: number | null;
  mpaa_rating: string | null;
  box_office: string | null;
  release_date: string | null;
  release_year: number | null;
  director: string | null;
  genre: string | null;
  scraped_at: string;
}

function generateRtId(slug: string): string {
  return slug.replace(/_(19|20)\d{2}$/, "");
}

function getRandomUserAgent(): string {
  const idx = Math.floor(Math.random() * USER_AGENTS.length);
  return USER_AGENTS[idx] as string;
}

function getHeaders(): Record<string, string> {
  return {
    "User-Agent": getRandomUserAgent(),
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    Referer: "https://www.rottentomatoes.com/",
    Connection: "keep-alive",
    "Cache-Control": "no-cache",
  };
}

async function delay(min: number, max: number = min): Promise<void> {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url: string): Promise<string | null> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, {
        headers: getHeaders(),
        signal: AbortSignal.timeout(30000),
      });

      if (response.status === 429 || response.status === 403) {
        console.log(
          `  Rate limited (${response.status}). Waiting ${RETRY_DELAY_MS * attempt}ms...`,
        );
        await delay(RETRY_DELAY_MS * attempt);
        continue;
      }

      if (response.status === 404) {
        return null;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.text();
    } catch (error) {
      console.error(`  Attempt ${attempt}/${MAX_RETRIES} failed:`, error);

      if (attempt < MAX_RETRIES) {
        const backoff = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
        console.log(`  Retrying in ${backoff / 1000}s...`);
        await delay(backoff);
      }
    }
  }
  return null;
}

function initDatabase(): Database {
  const db = new Database(DB_FILE);

  db.run(`
    CREATE TABLE IF NOT EXISTS movies (
      slug TEXT PRIMARY KEY,
      status TEXT DEFAULT 'pending',
      retry_count INTEGER DEFAULT 0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  db.run(`CREATE INDEX IF NOT EXISTS idx_status ON movies(status)`);

  return db;
}

async function discoverMovieSlugs(db: Database): Promise<number> {
  console.log("\n=== Phase 1: Discovering movie slugs from browse pages ===\n");

  const insertStmt = db.prepare(
    `INSERT OR IGNORE INTO movies (slug) VALUES (?)`,
  );
  let totalDiscovered = 0;

  for (const browsePath of BROWSE_URLS) {
    const url = `${BASE_URL}${browsePath}`;
    console.log(`Fetching: ${browsePath}`);

    const html = await fetchWithRetry(url);
    if (!html) {
      console.log(`  Failed to fetch ${browsePath}`);
      continue;
    }

    const slugMatches = html.match(/href="\/m\/([^"]+)"/g) || [];
    const slugs = new Set<string>();

    for (const match of slugMatches) {
      const slug = match.replace('href="/m/', "").replace('"', "");
      if (slug && !slug.includes("/")) {
        slugs.add(slug);
      }
    }

    let pageInserted = 0;
    for (const slug of slugs) {
      const result = insertStmt.run(slug);
      if (result.changes > 0) {
        pageInserted++;
        totalDiscovered++;
      }
    }

    console.log(`  Found ${slugs.size} movies, ${pageInserted} new`);
    await delay(MIN_DELAY_MS, MAX_DELAY_MS);
  }

  console.log(`\nTotal discovered: ${totalDiscovered} new movie slugs`);
  return totalDiscovered;
}

interface JsonLdMovie {
  name?: string;
  director?: { name?: string }[] | { name?: string };
  dateCreated?: string;
  contentRating?: string;
  genre?: string[];
  aggregateRating?: {
    ratingValue?: string | number;
    ratingCount?: number;
  };
}

function parseMoviePage(html: string, slug: string): MovieRecord | null {
  const $ = cheerio.load(html);

  let title = "";
  let director: string | null = null;
  let releaseDate: string | null = null;
  let releaseYear: number | null = null;
  let mpaaRating: string | null = null;
  let genre: string | null = null;
  let tomatometer: number | null = null;
  let audienceScore: number | null = null;
  let boxOffice: string | null = null;

  const jsonLdScript = $('script[type="application/ld+json"]').first().html();
  if (jsonLdScript) {
    try {
      const jsonLd = JSON.parse(jsonLdScript) as JsonLdMovie;

      title = jsonLd.name || "";

      if (jsonLd.director) {
        const directors = Array.isArray(jsonLd.director)
          ? jsonLd.director
          : [jsonLd.director];
        director = directors
          .map((d) => d.name)
          .filter(Boolean)
          .join(", ");
      }

      if (jsonLd.dateCreated) {
        releaseDate = jsonLd.dateCreated;
        const yearMatch = releaseDate.match(/\b(19\d{2}|20\d{2})\b/);
        if (yearMatch && yearMatch[1]) {
          releaseYear = parseInt(yearMatch[1], 10);
        }
      }

      mpaaRating = jsonLd.contentRating || null;
      genre = jsonLd.genre?.join(", ") || null;

      if (jsonLd.aggregateRating?.ratingValue) {
        tomatometer = parseInt(String(jsonLd.aggregateRating.ratingValue), 10);
      }
    } catch {
      // JSON-LD parse failed
    }
  }

  if (!title) {
    title = $("h1").first().text().trim() || slug;
  }

  const scoreBoard = $("score-board").first();
  if (scoreBoard.length) {
    if (tomatometer === null) {
      const tmScore = scoreBoard.attr("tomatometerscore");
      if (tmScore) tomatometer = parseInt(tmScore, 10);
    }
    const audScore = scoreBoard.attr("audiencescore");
    if (audScore) audienceScore = parseInt(audScore, 10);
  }

  $('[data-qa="movie-info-item"], .info-item, li').each((_, el) => {
    const label = $(el)
      .find('[data-qa="movie-info-item-label"], .info-item-label, dt, b')
      .text()
      .trim()
      .toLowerCase();
    const value = $(el)
      .find(
        '[data-qa="movie-info-item-value"], .info-item-value, dd, span, time',
      )
      .text()
      .trim();

    if (label.includes("box office") && value && !boxOffice) {
      boxOffice = value;
    }
    if (
      (label.includes("release date") || label.includes("in theaters")) &&
      value &&
      !releaseDate
    ) {
      releaseDate = value;
      const yearMatch = value.match(/\b(19\d{2}|20\d{2})\b/);
      if (yearMatch && yearMatch[1]) {
        releaseYear = parseInt(yearMatch[1], 10);
      }
    }
    if (label.includes("director") && value && !director) {
      director = value;
    }
    if (label.includes("rating") && value && !mpaaRating) {
      mpaaRating = value;
    }
  });

  if (releaseYear && releaseYear < START_YEAR) {
    return null;
  }

  return {
    rt_id: generateRtId(slug),
    slug,
    title,
    tomatometer_score: tomatometer,
    audience_score: audienceScore,
    mpaa_rating: mpaaRating,
    box_office: boxOffice,
    release_date: releaseDate,
    release_year: releaseYear,
    director,
    genre,
    scraped_at: new Date().toISOString(),
  };
}

async function scrapeMovieDetails(db: Database): Promise<MovieRecord[]> {
  console.log("\n=== Phase 2: Scraping movie detail pages ===\n");

  const pendingMovies = db
    .prepare(
      `SELECT slug FROM movies WHERE status = 'pending' AND retry_count < ? ORDER BY created_at`,
    )
    .all(MAX_RETRIES) as { slug: string }[];

  console.log(`Found ${pendingMovies.length} movies to scrape\n`);

  const updateStmt = db.prepare(
    `UPDATE movies SET status = ?, retry_count = retry_count + 1 WHERE slug = ?`,
  );

  const records: MovieRecord[] = [];
  let processed = 0;
  let scraped = 0;
  let filtered = 0;
  let failed = 0;

  for (const movie of pendingMovies) {
    const url = `${BASE_URL}/m/${movie.slug}`;

    const html = await fetchWithRetry(url);

    if (!html) {
      updateStmt.run("failed", movie.slug);
      failed++;
    } else {
      const record = parseMoviePage(html, movie.slug);

      if (record === null) {
        updateStmt.run("filtered", movie.slug);
        filtered++;
      } else {
        records.push(record);
        updateStmt.run("completed", movie.slug);
        scraped++;
      }
    }

    processed++;

    if (processed % 25 === 0 || processed === pendingMovies.length) {
      console.log(
        `Progress: ${processed}/${pendingMovies.length} | Scraped: ${scraped} | Filtered (pre-${START_YEAR}): ${filtered} | Failed: ${failed}`,
      );
    }

    await delay(MIN_DELAY_MS, MAX_DELAY_MS);
  }

  console.log(`\nScraping complete: ${scraped} movies ready for upload`);
  return records;
}

async function uploadFile(filePath: string, key: string): Promise<void> {
  const fileStream = createReadStream(filePath);
  const fileStats = await stat(filePath);

  console.log(
    `Uploading ${filePath} (${(fileStats.size / 1024 / 1024).toFixed(2)} MB) to s3://${BUCKET}/${key}`,
  );

  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET,
      Key: key,
      Body: fileStream,
      ContentType: "application/x-ndjson",
    },
    queueSize: 4,
    partSize: 10 * 1024 * 1024,
  });

  upload.on("httpUploadProgress", (progress) => {
    if (progress.loaded && progress.total) {
      const percent = ((progress.loaded / progress.total) * 100).toFixed(1);
      process.stdout.write(`\r  Progress: ${percent}%`);
    }
  });

  await upload.done();
  console.log(`\n  ✓ Uploaded successfully`);
}

async function writeChunk(
  movies: MovieRecord[],
  chunkNumber: number,
  timestamp: string,
): Promise<void> {
  const filename = `movies_${String(chunkNumber).padStart(4, "0")}.ndjson`;
  const localPath = join(DATA_DIR, filename);
  const s3Key = `${RAW_ZONE_PREFIX}/${timestamp}/${filename}`;

  const ndjson = movies.map((m) => JSON.stringify(m)).join("\n") + "\n";
  await writeFile(localPath, ndjson);

  await uploadFile(localPath, s3Key);
  await rm(localPath, { force: true });
}

async function uploadToS3(movies: MovieRecord[]): Promise<void> {
  console.log("\n=== Phase 3: Uploading to S3 ===\n");

  if (movies.length === 0) {
    console.log("No movies to upload.");
    return;
  }

  await mkdir(DATA_DIR, { recursive: true });

  const timestamp = new Date().toISOString().split("T")[0] ?? "unknown";
  let chunkNumber = 1;

  for (let i = 0; i < movies.length; i += CHUNK_SIZE) {
    const chunk = movies.slice(i, i + CHUNK_SIZE);
    console.log(`Writing chunk ${chunkNumber} (${chunk.length} movies)...`);
    await writeChunk(chunk, chunkNumber, timestamp);
    chunkNumber++;
  }

  const metadata = {
    source: "rotten_tomatoes",
    timestamp,
    total_movies: movies.length,
    total_chunks: chunkNumber - 1,
    year_filter: `>= ${START_YEAR}`,
    completed_at: new Date().toISOString(),
    fields: [
      "rt_id",
      "slug",
      "title",
      "tomatometer_score",
      "audience_score",
      "mpaa_rating",
      "box_office",
      "release_date",
      "release_year",
      "director",
      "genre",
      "scraped_at",
    ],
  };

  const metadataPath = join(DATA_DIR, "metadata.json");
  await writeFile(metadataPath, JSON.stringify(metadata, null, 2));
  await uploadFile(
    metadataPath,
    `${RAW_ZONE_PREFIX}/${timestamp}/metadata.json`,
  );

  await rm(DATA_DIR, { recursive: true, force: true });

  console.log(
    `\n✓ Uploaded to s3://${BUCKET}/${RAW_ZONE_PREFIX}/${timestamp}/`,
  );
}

async function main(): Promise<void> {
  console.log("=== Rotten Tomatoes Data Ingestion to S3 Raw Zone ===\n");

  const db = initDatabase();

  try {
    const existingCount = (
      db.prepare("SELECT COUNT(*) as count FROM movies").get() as {
        count: number;
      }
    ).count;
    const pendingCount = (
      db
        .prepare(
          "SELECT COUNT(*) as count FROM movies WHERE status = 'pending'",
        )
        .get() as { count: number }
    ).count;

    if (existingCount === 0) {
      await discoverMovieSlugs(db);
    } else if (pendingCount === 0) {
      console.log(`Found ${existingCount} movies in database, all processed.`);
      console.log("Delete rt_crawl_state.db to start fresh discovery.\n");
    } else {
      console.log(
        `Resuming: ${pendingCount} pending movies out of ${existingCount} total\n`,
      );
    }

    const movies = await scrapeMovieDetails(db);

    if (movies.length > 0) {
      await uploadToS3(movies);
    }

    const stats = db
      .prepare(`SELECT status, COUNT(*) as count FROM movies GROUP BY status`)
      .all() as {
      status: string;
      count: number;
    }[];

    console.log("\n=== Final Statistics ===");
    for (const { status, count } of stats) {
      console.log(`  ${status}: ${count}`);
    }
  } finally {
    db.close();
  }

  console.log("\n=== Ingestion Complete ===");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
