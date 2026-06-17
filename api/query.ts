import { AdbcDatabase } from "@apache-arrow/adbc-driver-manager";
import { RecordBatchStreamWriter } from "apache-arrow";
import { pipeline } from "node:stream/promises";
import type { VercelRequest, VercelResponse } from "@vercel/node";
import { ensureDriver } from "./_drivers.js";

// A minimal ADBC gateway: POST { uri, sql }, always streams the result back as
// an Arrow IPC stream. The URI is an ADBC connection string like
// '<driver>://...' — the scheme selects a driver from the ADBC Driver Registry
// (https://dbc-cdn.columnar.tech), which is fetched on demand. Everything
// needed to connect is carried in the URI and forwarded to the driver verbatim.
export default async function handler(
  req: VercelRequest,
  res: VercelResponse,
): Promise<void> {
  const { uri, sql } = req.body;
  const driver = uri.slice(0, uri.indexOf(":"));

  const { sharedLibPath, entrypoint } = await ensureDriver(driver);
  const db = new AdbcDatabase({
    driver: sharedLibPath,
    entrypoint,
    databaseOptions: { uri },
  });
  const connection = await db.connect();

  res.setHeader("content-type", "application/vnd.apache.arrow.stream");
  const reader = await connection.queryStream(sql);
  await pipeline(reader.toNodeStream(), RecordBatchStreamWriter.throughNode(), res);

  await connection.close();
  await db.close();
}
