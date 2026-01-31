import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { createReadStream, createWriteStream } from "node:fs";
import { readFile, writeFile, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { pipeline } from "node:stream/promises";

const BUCKET = "movies-datalake-2310";
const REGION = "ap-southeast-1";
const RAW_ZONE_PREFIX = "raw/movielens";
const DATA_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip";
const DATA_DIR = "./ml-32m";
const ZIP_FILE = "./ml-32m.zip";
const CHECKSUM_FILE = "./.last_checksum";

const s3Client = new S3Client({ region: REGION });

const CSV_FILES = ["ratings.csv", "movies.csv", "tags.csv", "links.csv"];

async function downloadZip(): Promise<void> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000; // 2 seconds

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(
        `Downloading ${DATA_URL}... (attempt ${attempt}/${MAX_RETRIES})`,
      );

      const response = await fetch(DATA_URL, {
        signal: AbortSignal.timeout(300000), // 5 minutes timeout
      });

      if (!response.ok) {
        throw new Error(`Failed to download: ${response.statusText}`);
      }

      if (!response.body) {
        throw new Error("Response body is null");
      }

      // Stream the download to avoid memory issues with large files
      const fileStream = createWriteStream(ZIP_FILE);
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

        console.log("\n  ✓ Download complete");
        return; // Success - exit the retry loop
      } catch (error) {
        fileStream.destroy();
        await rm(ZIP_FILE, { force: true });
        throw error;
      }
    } catch (error) {
      console.error(`\n  ✗ Attempt ${attempt} failed:`, error);

      if (attempt < MAX_RETRIES) {
        console.log(`  Retrying in ${RETRY_DELAY / 1000} seconds...`);
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
      } else {
        throw new Error(
          `Failed to download after ${MAX_RETRIES} attempts: ${error}`,
        );
      }
    }
  }
}

async function computeChecksum(filePath: string): Promise<string> {
  const content = await readFile(filePath);
  return createHash("md5").update(content).digest("hex");
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

async function extractZip(): Promise<void> {
  console.log("Extracting zip file...");
  await rm(DATA_DIR, { recursive: true, force: true });
  const proc = Bun.spawn(["unzip", "-o", ZIP_FILE, "-d", "."], {
    stdout: "ignore",
    stderr: "pipe",
  });
  await proc.exited;
  if (proc.exitCode !== 0) {
    throw new Error("Failed to extract zip file");
  }
  console.log("  ✓ Extraction complete");
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
      ContentType: "text/csv",
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
  console.log("=== MovieLens Data Ingestion to S3 Raw Zone ===\n");

  await downloadZip();

  const currentChecksum = await computeChecksum(ZIP_FILE);
  const lastChecksum = await getLastChecksum();

  console.log(`\nCurrent checksum: ${currentChecksum}`);
  console.log(`Last checksum:    ${lastChecksum || "(none)"}`);

  if (currentChecksum === lastChecksum) {
    console.log("\n✓ Data unchanged. Skipping upload.");
    await rm(ZIP_FILE, { force: true });
    return;
  }

  console.log("\n⚡ Data changed. Proceeding with upload...\n");

  await extractZip();

  const timestamp = new Date().toISOString().split("T")[0];

  for (const file of CSV_FILES) {
    const filePath = join(DATA_DIR, file);
    const s3Key = `${RAW_ZONE_PREFIX}/${timestamp}/${file}`;

    try {
      await uploadFile(filePath, s3Key);
    } catch (error) {
      console.error(`✗ Failed to upload ${file}:`, error);
      process.exit(1);
    }
  }

  await saveChecksum(currentChecksum);

  await rm(ZIP_FILE, { force: true });

  console.log("\n=== Ingestion Complete ===");
  console.log(
    `All files uploaded to s3://${BUCKET}/${RAW_ZONE_PREFIX}/${timestamp}/`,
  );
}

main();
