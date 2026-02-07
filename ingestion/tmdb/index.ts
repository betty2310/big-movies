import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { createReadStream } from "node:fs";
import { writeFile, rm, stat, mkdir, readFile } from "node:fs/promises";
import { join } from "node:path";

const BUCKET = "movies-datalake-2310";
const REGION = "ap-southeast-1";
const RAW_ZONE_PREFIX = "raw/tmdb";
const BASE_URL = "https://api.themoviedb.org/3";
const DATA_DIR = "./tmdb-data";
const STATE_FILE = "./.ingestion_state";
const CHUNK_SIZE = 1000;
const REQUEST_DELAY_MS = 5;
const DETAIL_DELAY_MS = 2;
const DETAIL_CONCURRENCY = 30;
const REQUESTS_PER_WINDOW = 35;
const RATE_WINDOW_MS = 10_000;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const MAX_PAGES_PER_SORT = 500;
const START_YEAR = 1950;
const END_YEAR = new Date().getFullYear();
const YEAR_PARALLELISM = 2;

class TokenBucket {
  private timestamps: number[] = [];

  constructor(
    public readonly token: string,
    public readonly label: string,
  ) {}

  async acquire(): Promise<void> {
    const now = Date.now();
    this.timestamps = this.timestamps.filter(
      (ts) => now - ts < RATE_WINDOW_MS,
    );

    if (this.timestamps.length >= REQUESTS_PER_WINDOW) {
      const earliest = this.timestamps[0]!;
      const wait = RATE_WINDOW_MS - (now - earliest);
      if (wait > 0) {
        await delay(wait);
        return this.acquire();
      }
    }

    this.timestamps.push(Date.now());
  }
}

const tokens: TokenBucket[] = [];

function loadTokens(): void {
  const t0 = process.env.API_READ_ACCESS_TOKEN;
  const t1 = process.env.API_READ_ACCESS_TOKEN_1;

  if (t0) tokens.push(new TokenBucket(t0, "token-0"));
  if (t1) tokens.push(new TokenBucket(t1, "token-1"));

  if (tokens.length === 0) {
    console.error(
      "ERROR: At least one API_READ_ACCESS_TOKEN[_N] environment variable is required",
    );
    process.exit(1);
  }

  console.log(`Loaded ${tokens.length} API token(s)\n`);
}

let roundRobinIndex = 0;
function nextToken(): TokenBucket {
  const bucket = tokens[roundRobinIndex % tokens.length]!;
  roundRobinIndex++;
  return bucket;
}

const s3Client = new S3Client({ region: REGION });

interface DiscoverResponse {
  page: number;
  results: Record<string, unknown>[];
  total_pages: number;
  total_results: number;
}

interface IngestionState {
  currentYear: number;
  currentPage: number;
  processedMovieIds: number[];
  chunksUploaded: number;
}

interface MovieDetails {
  id: number;
  budget: number;
  revenue: number;
  [key: string]: unknown;
}

async function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry<T>(
  url: string,
  bucket?: TokenBucket,
): Promise<T> {
  const b = bucket ?? nextToken();

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      await b.acquire();
      const response = await fetch(url, {
        headers: {
          Authorization: `Bearer ${b.token}`,
          "Content-Type": "application/json",
        },
        signal: AbortSignal.timeout(30000),
      });

      if (response.status === 429) {
        const retryAfter = parseInt(
          response.headers.get("Retry-After") || "5",
        );
        console.log(`  [${b.label}] Rate limited. Waiting ${retryAfter}s...`);
        await delay(retryAfter * 1000);
        continue;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return (await response.json()) as T;
    } catch (error) {
      console.error(
        `  [${b.label}] Attempt ${attempt}/${MAX_RETRIES} failed:`,
        error,
      );

      if (attempt < MAX_RETRIES) {
        const backoff = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
        console.log(`  Retrying in ${backoff / 1000}s...`);
        await delay(backoff);
      } else {
        throw error;
      }
    }
  }
  throw new Error("Max retries exceeded");
}

async function discoverMovies(
  year: number,
  page: number,
): Promise<DiscoverResponse> {
  const url = `${BASE_URL}/discover/movie?primary_release_year=${year}&sort_by=popularity.desc&page=${page}&include_adult=false&include_video=false`;
  return fetchWithRetry<DiscoverResponse>(url);
}

async function fetchMovieDetails(movieId: number): Promise<MovieDetails> {
  const url = `${BASE_URL}/movie/${movieId}`;
  return fetchWithRetry<MovieDetails>(url);
}

async function enrichWithBoxOffice(
  movies: Record<string, unknown>[],
): Promise<Record<string, unknown>[]> {
  const enriched: Record<string, unknown>[] = [];

  for (let i = 0; i < movies.length; i += DETAIL_CONCURRENCY) {
    const batch = movies.slice(i, i + DETAIL_CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map(async (movie) => {
        const movieId = movie.id as number;
        const details = await fetchMovieDetails(movieId);
        return {
          ...movie,
          budget: details.budget ?? 0,
          revenue: details.revenue ?? 0,
        };
      }),
    );

    for (let j = 0; j < results.length; j++) {
      const result = results[j]!;
      const original = batch[j]!;
      if (result.status === "fulfilled") {
        enriched.push(result.value);
      } else {
        console.warn(
          `  ⚠ Failed to fetch details for movie ${original.id}, keeping without box office`,
        );
        enriched.push({ ...original, budget: null, revenue: null });
      }
    }

    if (i + DETAIL_CONCURRENCY < movies.length) {
      await delay(DETAIL_DELAY_MS);
    }
  }

  return enriched;
}

async function loadState(): Promise<IngestionState> {
  try {
    const content = await readFile(STATE_FILE, "utf-8");
    return JSON.parse(content);
  } catch {
    return {
      currentYear: START_YEAR,
      currentPage: 1,
      processedMovieIds: [],
      chunksUploaded: 0,
    };
  }
}

async function saveState(state: IngestionState): Promise<void> {
  await writeFile(STATE_FILE, JSON.stringify(state, null, 2));
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
  movies: Record<string, unknown>[],
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

async function discoverYear(
  year: number,
  startPage: number,
  seenIds: Set<number>,
): Promise<Record<string, unknown>[]> {
  const movies: Record<string, unknown>[] = [];

  for (let page = startPage; page <= MAX_PAGES_PER_SORT; page++) {
    const response = await discoverMovies(year, page);

    if (response.results.length === 0) {
      console.log(`  Year ${year} | No more results at page ${page}`);
      break;
    }

    const maxPage = Math.min(response.total_pages, MAX_PAGES_PER_SORT);
    console.log(
      `  Year ${year} | Page ${page}/${maxPage} - ${response.results.length} movies`,
    );

    for (const movie of response.results) {
      const movieId = movie.id as number;
      if (!seenIds.has(movieId)) {
        seenIds.add(movieId);
        movies.push(movie);
      }
    }

    if (page >= maxPage) {
      console.log(`  Reached max page for year ${year}`);
      break;
    }

    await delay(REQUEST_DELAY_MS);
  }

  return movies;
}

async function main(): Promise<void> {
  console.log("=== TMDB Data Ingestion to S3 Raw Zone ===\n");

  loadTokens();
  await mkdir(DATA_DIR, { recursive: true });

  const state = await loadState();
  const timestamp = new Date().toISOString().split("T")[0];
  const seenIds = new Set<number>(state.processedMovieIds);
  let chunkNumber = state.chunksUploaded + 1;

  console.log(
    `Resuming from: year=${state.currentYear}, page=${state.currentPage}`,
  );
  console.log(
    `Already processed: ${seenIds.size} unique movies, ${state.chunksUploaded} chunks\n`,
  );

  const allYears: number[] = [];
  for (let y = state.currentYear; y <= END_YEAR; y++) {
    allYears.push(y);
  }

  let movieBuffer: Record<string, unknown>[] = [];

  for (let i = 0; i < allYears.length; i += YEAR_PARALLELISM) {
    const yearBatch = allYears.slice(i, i + YEAR_PARALLELISM);
    console.log(`\n--- Fetching years [${yearBatch.join(", ")}] in parallel ---\n`);

    try {
      const yearResults = await Promise.all(
        yearBatch.map((year) => {
          const startPage =
            year === state.currentYear ? state.currentPage : 1;
          return discoverYear(year, startPage, seenIds);
        }),
      );

      for (const movies of yearResults) {
        movieBuffer.push(...movies);
      }

      while (movieBuffer.length >= CHUNK_SIZE) {
        const chunk = movieBuffer.splice(0, CHUNK_SIZE);
        console.log(
          `\n  Enriching chunk ${chunkNumber} with box office data (${chunk.length} movies)...`,
        );
        const enriched = await enrichWithBoxOffice(chunk);
        console.log(
          `  Writing chunk ${chunkNumber} (${enriched.length} movies)...`,
        );
        await writeChunk(enriched, chunkNumber, timestamp || "");

        state.processedMovieIds = Array.from(seenIds);
        state.chunksUploaded = chunkNumber;
        state.currentYear = yearBatch[yearBatch.length - 1]! + 1;
        state.currentPage = 1;
        await saveState(state);

        chunkNumber++;
      }
    } catch (error) {
      console.error(`\nError fetching years [${yearBatch.join(", ")}]:`, error);
      state.currentYear = yearBatch[0]!;
      state.currentPage = 1;
      state.processedMovieIds = Array.from(seenIds);
      await saveState(state);
      console.log("State saved. Run again to resume.");
      process.exit(1);
    }
  }

  if (movieBuffer.length > 0) {
    console.log(
      `\n  Enriching final chunk ${chunkNumber} with box office data (${movieBuffer.length} movies)...`,
    );
    const enriched = await enrichWithBoxOffice(movieBuffer);
    console.log(
      `  Writing final chunk ${chunkNumber} (${enriched.length} movies)...`,
    );
    await writeChunk(enriched, chunkNumber, timestamp || "");
  }

  const metadata = {
    timestamp,
    totalMovies: seenIds.size,
    totalChunks: chunkNumber,
    yearRange: { start: START_YEAR, end: END_YEAR },
    tokenCount: tokens.length,
    completedAt: new Date().toISOString(),
  };

  const metadataPath = join(DATA_DIR, "metadata.json");
  await writeFile(metadataPath, JSON.stringify(metadata, null, 2));
  await uploadFile(
    metadataPath,
    `${RAW_ZONE_PREFIX}/${timestamp}/metadata.json`,
  );

  await rm(DATA_DIR, { recursive: true, force: true });
  await rm(STATE_FILE, { force: true });

  console.log("\n=== Ingestion Complete ===");
  console.log(`Total unique movies: ${seenIds.size}`);
  console.log(`Total chunks: ${chunkNumber}`);
  console.log(`Uploaded to: s3://${BUCKET}/${RAW_ZONE_PREFIX}/${timestamp}/`);
}

main();
