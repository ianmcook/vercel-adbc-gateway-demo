import { tableFromIPC } from "apache-arrow";

const [uri, sql] = process.argv.slice(2);

// Point at your deployment with the URL env var, e.g.
//   URL=https://your-deployment.vercel.app/api/query node client.mjs ...
const endpoint = process.env.URL ?? "http://localhost:3000/api/query";

if (!uri || !sql) {
  console.error("Usage: node client.mjs <uri> <sql>");
  console.error(
    'Example: node client.mjs "snowflake://user:pass@account/DB/SCHEMA" "SELECT 1"',
  );
  process.exit(1);
}

const response = await fetch(endpoint, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ uri, sql }),
});

if (!response.ok) {
  console.error(`HTTP ${response.status} ${response.statusText}`);
  console.error(await response.text());
  process.exit(1);
}

const table = await tableFromIPC(response);
console.table(table.toArray());
