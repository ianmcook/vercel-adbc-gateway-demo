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

  // Embedded drivers (duckdb, sqlite) take a database path, not a connection
  // string: a bare 'duckdb://' / 'sqlite://' means "no database", so pass no
  // uri and let them open in-memory. Everything else is forwarded verbatim.
  const isEmbedded = driver === "duckdb" || driver === "sqlite";
  const databaseOptions: Record<string, string> =
    isEmbedded && uri.slice(uri.indexOf("://") + 3).length === 0
      ? {}
      : { uri };

  const { sharedLibPath, entrypoint } = await ensureDriver(driver);
  const db = new AdbcDatabase({
    driver: sharedLibPath,
    entrypoint,
    databaseOptions,
  });
  const connection = await db.connect();

  // DataFusion emits string columns as the Arrow StringView type, which the
  // apache-arrow JS library cannot decode (the stream stalls on such a batch).
  // Disable view types so strings come back as plain String.
  if (driver === "datafusion") {
    await connection.execute(
      "SET datafusion.execution.parquet.schema_force_view_types = false",
    );
    await connection.execute(
      "SET datafusion.sql_parser.map_string_types_to_utf8view = false",
    );
  }

  res.setHeader("content-type", "application/vnd.apache.arrow.stream");
  const reader = await connection.queryStream(sql);
  await pipeline(reader.toNodeStream(), RecordBatchStreamWriter.throughNode(), res);

  await connection.close();
  await db.close();
}
