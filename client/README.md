# vercel-adbc-gateway-demo-client

A tiny Node CLI that posts a SQL query to the
[`/api/query`](../README.md) Vercel function and prints the result as a table.

The function runs the query against the ADBC driver named by the URI scheme
(any driver in the [ADBC Driver Registry](https://dbc-cdn.columnar.tech)) and
streams back an Arrow IPC stream, which the client decodes with
[`apache-arrow`](https://www.npmjs.com/package/apache-arrow).

## Setup

Requires Node.js 20 or newer.

```sh
npm install
```

## Usage

Set `URL` to your deployment's endpoint (it defaults to
`http://localhost:3000/api/query` for use with `vercel dev`):

```sh
URL=https://<your-deployment>.vercel.app/api/query \
  node client.mjs "snowflake://user:pass@account/DB/SCHEMA?warehouse=WH" "SELECT 1 AS x"
```

The first argument is the ADBC URI — the scheme selects the driver and all
credentials are carried in it — and the second is the SQL to run. See the
[URI convention](../README.md#uri-convention) for the connection-string format.
