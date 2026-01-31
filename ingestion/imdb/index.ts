import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { createReadStream, createWriteStream } from "node:fs";
import { readFile, writeFile, rm, stat, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { createGunzip } from "node:zlib";
import { pipeline } from "node:stream/promises";

const BUCKET = "movies-datalake-2310";
const REGION = "ap-southeast-1";
const RAW_ZONE_PREFIX = "raw/imdb";
const BASE_URL = "https://datasets.imdbws.com";
const DATA_DIR = "./imdb-data";
const CHECKSUM_FILE = "./.last_checksum";

const s3Client = new S3Client({ region: REGION });

const IMDB_DATASETS = [
  "name.basics.tsv.gz",
  "title.akas.tsv.gz",
  "title.basics.tsv.gz",
  "title.crew.tsv.gz",
  "title.episode.tsv.gz",
  "title.principals.tsv.gz",
  "title.ratings.tsv.gz",
];

async function downloadFile(filename: string): Promise<void> {
  const url = `${BASE_URL}/${filename}`;
  const outputPath = join(DATA_DIR, filename);
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(
        `Downloading ${filename}... (attempt ${attempt}/${MAX_RETRIES})`,
      );

      const response = await fetch(url, {
        signal: AbortSignal.timeout(600000), // 10 minutes timeout
      });

      if (!response.ok) {
        throw new Error(`Failed to download: ${response.statusText}`);
      }

      if (!response.body) {
        throw new Error("Response body is null");
      }

      const fileStream = createWriteStream(outputPath);
      const reader = response.body.getReader();

      let downloadedBytes = 0;
      const contentLength = parseInt(
        response.headers.get("content-length") || "0",
      );

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          fileStream.write(value);
          downloadedBytes += value.length;

          if (contentLength > 0) {
            const percent = ((downloadedBytes / contentLength) * 100).toFixed(
              1,
            );
            const mb = (downloadedBytes / 1024 / 1024).toFixed(2);
            const totalMb = (contentLength / 1024 / 1024).toFixed(2);
            process.stdout.write(
              `\r  Progress: ${percent}% (${mb}/${totalMb} MB)`,
            );
          }
        }

        fileStream.end();
        await new Promise((resolve, reject) => {
          fileStream.on("finish", resolve);
          fileStream.on("error", reject);
        });

        console.log(`\n  ✓ Downloaded ${filename}`);
        return;
      } catch (error) {
        fileStream.destroy();
        await rm(outputPath, { force: true });
        throw error;
      }
    } catch (error) {
      console.error(`\n  ✗ Attempt ${attempt} failed:`, error);

      if (attempt < MAX_RETRIES) {
        console.log(`  Retrying in ${RETRY_DELAY / 1000} seconds...`);
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
      } else {
        throw new Error(
          `Failed to download ${filename} after ${MAX_RETRIES} attempts: ${error}`,
        );
      }
    }
  }
}

async function extractGzFile(gzPath: string): Promise<string> {
  const outputPath = gzPath.replace(".gz", "");
  console.log(`Extracting ${gzPath}...`);

  const source = createReadStream(gzPath);
  const destination = createWriteStream(outputPath);
  const gunzip = createGunzip();

  await pipeline(source, gunzip, destination);

  console.log(`  ✓ Extracted to ${outputPath}`);
  return outputPath;
}

async function computeChecksums(files: string[]): Promise<string> {
  const checksums: string[] = [];

  for (const file of files) {
    const filePath = join(DATA_DIR, file);
    try {
      const content = await readFile(filePath);
      const checksum = createHash("md5").update(content).digest("hex");
      checksums.push(`${file}:${checksum}`);
    } catch {
      checksums.push(`${file}:missing`);
    }
  }

  return createHash("md5").update(checksums.join("|")).digest("hex");
}

async function getLastChecksum(): Promise<string | null> {
  try {
    return (await readFile(CHECKSUM_FILE, "utf-8")).trim();
  } catch {
    return null;
  }
}

async function saveChecksum(checksum: string): Promise<void> {
  await writeFile(CHECKSUM_FILE, checksum);
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
      ContentType: "text/tab-separated-values",
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

async function main(): Promise<void> {
  console.log("=== IMDb Data Ingestion to S3 Raw Zone ===\n");

  await mkdir(DATA_DIR, { recursive: true });

  console.log("Step 1: Downloading all IMDb datasets...\n");
  for (const file of IMDB_DATASETS) {
    await downloadFile(file);
  }

  const currentChecksum = await computeChecksums(IMDB_DATASETS);
  const lastChecksum = await getLastChecksum();

  console.log(`\nCurrent checksum: ${currentChecksum}`);
  console.log(`Last checksum:    ${lastChecksum || "(none)"}`);

  if (currentChecksum === lastChecksum) {
    console.log("\n✓ Data unchanged. Skipping upload.");
    await rm(DATA_DIR, { recursive: true, force: true });
    return;
  }

  console.log("\n⚡ Data changed. Proceeding with extraction and upload...\n");

  console.log("Step 2: Extracting .gz files...\n");
  const extractedFiles: string[] = [];
  for (const file of IMDB_DATASETS) {
    const gzPath = join(DATA_DIR, file);
    const extractedPath = await extractGzFile(gzPath);
    extractedFiles.push(extractedPath);
  }

  console.log("\nStep 3: Uploading TSV files to S3...\n");
  const timestamp = new Date().toISOString().split("T")[0];

  for (const extractedFile of extractedFiles) {
    const filename = extractedFile.split("/").pop()!;
    const s3Key = `${RAW_ZONE_PREFIX}/${timestamp}/${filename}`;

    try {
      await uploadFile(extractedFile, s3Key);
    } catch (error) {
      console.error(`✗ Failed to upload ${filename}:`, error);
      process.exit(1);
    }
  }

  await saveChecksum(currentChecksum);

  await rm(DATA_DIR, { recursive: true, force: true });

  console.log("\n=== Ingestion Complete ===");
  console.log(
    `All files uploaded to s3://${BUCKET}/${RAW_ZONE_PREFIX}/${timestamp}/`,
  );
  console.log("\nUploaded files:");
  for (const file of IMDB_DATASETS) {
    console.log(`  - ${file.replace(".gz", "")}`);
  }
}

main();
