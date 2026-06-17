# vercel-adbc-gateway-demo

A minimal [ADBC](https://arrow.apache.org/adbc/) gateway running as a Vercel
Function. A single endpoint accepts a `POST` with a JSON body
`{ "uri": "<adbc-uri>", "sql": "<query>" }`, runs the query against the ADBC
driver named by the URI scheme, and streams the result back as an
[Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
stream.

Any driver in the [ADBC Driver Registry](https://dbc-cdn.columnar.tech) works —
the scheme in the URI (e.g. `snowflake://`, `postgresql://`, `duckdb://`,
`bigquery://`) selects the driver, and the latest version is downloaded on
demand on first use. No driver is bundled or hardcoded.

> [!NOTE]
> Vercel Functions are not really the ideal architecture for this. The
> ephemeral filesystem, per-request driver downloads, cold starts, and
> execution time limits all work against a long-lived database gateway, and a
> persistent server would handle it more naturally. This project exists to
> demonstrate that running ADBC drivers on Vercel Functions is nonetheless
> possible — not to recommend it as a production pattern.

## `/api/query`

`POST` a JSON body and get back an Arrow IPC stream
(`Content-Type: application/vnd.apache.arrow.stream`, chunked — no
`Content-Length`):

```sh
curl -X POST https://vercel-adbc-gateway-demo.vercel.app/api/query \
  -H 'content-type: application/json' \
  --data '{"uri":"snowflake://user:pass@account/DB/SCHEMA?warehouse=WH","sql":"SELECT 1 AS x"}' \
  --output result.arrows
```

## URI convention

The `uri` is an ADBC connection string of the form `<driver>://...`. The scheme
must match a driver `path` in the [registry index](https://dbc-cdn.columnar.tech/index.yaml)
(`snowflake`, `postgresql`, `mysql`, `bigquery`, `redshift`, `databricks`,
`clickhouse`, `trino`, `duckdb`, `sqlite`, and more). Everything needed to
connect — host, credentials, database, and driver-specific parameters — is
carried in the URI, which is forwarded to the driver verbatim. For example:

```
snowflake://alice:s3cret@myorg-myacct/ANALYTICS/PUBLIC?warehouse=COMPUTE_WH
postgresql://user:pass@host:5432/dbname
```

Each driver defines its own connection-string format; see the
[ADBC driver docs](https://docs.adbc-drivers.org/) for the driver you are
using.

## How it works

The function runs on the `@vercel/node` runtime. On the first request for a
given scheme, [`api/_drivers.ts`](./api/_drivers.ts):

1. fetches the registry index (`index.yaml`) and finds the driver whose `path`
   equals the URI scheme,
2. selects its latest version (preferring stable releases),
3. downloads the Linux/amd64 tarball and extracts it to `/tmp`,
4. reads the shared-library filename and (if any) the ADBC entrypoint from the
   tarball's `MANIFEST`.

The driver is cached in `/tmp` and reused by later requests on the same
instance. Because Vercel's `/tmp` is limited to 500 MB, the cache is bounded:
the download tarball is deleted after extraction, and when a new driver would
exceed the budget the least-recently-used cached drivers are evicted to make
room (so a single warm instance can serve many drivers without overflowing).

## Benchmarking

Don't time the first request: it pays one-time costs — a cold boot plus the
driver download and extraction — that won't recur, so it badly overstates
steady-state latency. Send a warm-up request for the same driver first and
discard it, then measure several runs against the warm instance and report the
median (cold starts and evictions still cause occasional outliers). Bear in
mind a driver evicted to stay under the `/tmp` budget will be re-downloaded on
its next use, so benchmarking many different drivers in one run can reintroduce
download latency — pin your test to a single driver to measure query
performance alone.

## Client

A tiny Node CLI in [`client/`](./client) posts a query and prints the result as
a table. See [`client/README.md`](./client/README.md).

```sh
cd client
npm install
URL=https://vercel-adbc-gateway-demo.vercel.app/api/query \
  node client.mjs "snowflake://user:pass@account/DB/SCHEMA" "SELECT 1 AS x"
```

## Repository layout

| Path                | What it is                                                       |
| ------------------- | ---------------------------------------------------------------- |
| `api/query.ts`      | The function: connect, run the query, stream Arrow IPC back      |
| `api/_drivers.ts`   | Resolves + downloads the driver for a URI scheme from the registry |
| `client/`           | Standalone Node CLI (its own `package.json`)                     |
| `vercel.json`       | Function runtime + `maxDuration` configuration                   |

## Deploy

```sh
npm install
vercel --prod
```
