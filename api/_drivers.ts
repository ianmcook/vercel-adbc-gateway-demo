import { mkdir, readdir, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createWriteStream } from "node:fs";
import { x as tarExtract } from "tar";
import { load as parseYaml } from "js-yaml";

const CDN_BASE = "https://dbc-cdn.columnar.tech";
const INDEX_URL = `${CDN_BASE}/index.yaml`;

// Vercel Functions run on Linux/amd64, so that is the only build we fetch.
const PLATFORM = "linux_amd64";

// Vercel's /tmp is capped at 500 MB. Bound the extracted-driver cache below
// that so accumulating many drivers on one warm instance can't overflow it;
// when a new driver won't fit, the least-recently-used drivers are evicted.
const CACHE_BUDGET_BYTES = 400 * 1024 * 1024;

export interface DriverInfo {
  sharedLibPath: string;
  entrypoint?: string;
}

// Shape of the registry index at https://dbc-cdn.columnar.tech/index.yaml.
interface RegistryPackage {
  platform: string;
  url: string;
}
interface RegistryRelease {
  version: string;
  packages: RegistryPackage[];
}
interface RegistryDriver {
  name: string;
  path: string; // the URI scheme, e.g. "snowflake", "postgresql", "duckdb"
  pkginfo: RegistryRelease[];
}
interface Registry {
  drivers: RegistryDriver[];
}

const installRoot = join(tmpdir(), "adbc-drivers");

const inflight = new Map<string, Promise<DriverInfo>>();

// Driver directories currently being downloaded/extracted. Eviction must never
// touch these, or a concurrent request could delete another's partial install.
const building = new Set<string>();

// LRU recency for cached driver directories, keyed by absolute path. A
// monotonic counter (rather than a clock) orders evictions: lowest = oldest.
const lastUsed = new Map<string, number>();
let useTick = 0;
function touch(dir: string): void {
  lastUsed.set(dir, ++useTick);
}

// The registry index is fetched once per instance and cached. Latest-version
// resolution is therefore pinned for an instance's lifetime, which is fine for
// short-lived serverless instances.
let registryPromise: Promise<Registry> | undefined;

async function pathExists(p: string): Promise<boolean> {
  try {
    await stat(p);
    return true;
  } catch {
    return false;
  }
}

async function fetchRegistry(): Promise<Registry> {
  const res = await fetch(INDEX_URL);
  if (!res.ok) {
    throw new Error(
      `Failed to fetch driver registry ${INDEX_URL}: ${res.status} ${res.statusText}`,
    );
  }
  const registry = parseYaml(await res.text()) as Registry;
  if (!registry || !Array.isArray(registry.drivers)) {
    throw new Error(`Driver registry ${INDEX_URL} is malformed`);
  }
  return registry;
}

function getRegistry(): Promise<Registry> {
  return (registryPromise ??= fetchRegistry());
}

// Parse a registry version like "v1.10.3" or "0.1.0-alpha.2" into numeric core
// components plus an optional prerelease tag.
function parseVersion(raw: string): { nums: number[]; pre: string | null } {
  const v = raw.replace(/^v/, "");
  const dash = v.indexOf("-");
  const core = dash === -1 ? v : v.slice(0, dash);
  const pre = dash === -1 ? null : v.slice(dash + 1);
  const nums = core.split(".").map((n) => Number.parseInt(n, 10) || 0);
  return { nums, pre };
}

// Semver-ish comparison: compare core numerically (so 1.10.0 > 1.9.0), and
// treat a stable release as newer than a prerelease of the same core version.
function compareVersions(a: string, b: string): number {
  const pa = parseVersion(a);
  const pb = parseVersion(b);
  const len = Math.max(pa.nums.length, pb.nums.length);
  for (let i = 0; i < len; i++) {
    const d = (pa.nums[i] ?? 0) - (pb.nums[i] ?? 0);
    if (d !== 0) return d;
  }
  if (pa.pre === pb.pre) return 0;
  if (pa.pre === null) return 1;
  if (pb.pre === null) return -1;
  return pa.pre < pb.pre ? -1 : 1;
}

// Pick the newest release, preferring stable versions and only falling back to
// prereleases for drivers that have no stable release yet (e.g. alpha-only).
function pickLatest(releases: RegistryRelease[]): RegistryRelease {
  const stable = releases.filter((r) => parseVersion(r.version).pre === null);
  const pool = stable.length > 0 ? stable : releases;
  return pool.reduce((best, cur) =>
    compareVersions(cur.version, best.version) > 0 ? cur : best,
  );
}

// Compressed size of a driver tarball via a HEAD request, used to budget the
// cache before downloading. Returns 0 if the server omits Content-Length.
async function headContentLength(url: string): Promise<number> {
  try {
    const res = await fetch(url, { method: "HEAD" });
    return Number(res.headers.get("content-length")) || 0;
  } catch {
    return 0;
  }
}

async function downloadAndExtract(url: string, dest: string): Promise<void> {
  await mkdir(dest, { recursive: true });
  const res = await fetch(url);
  if (!res.ok || !res.body) {
    throw new Error(`Failed to download ${url}: ${res.status} ${res.statusText}`);
  }
  const tarball = join(dest, "driver.tar.gz");
  await pipeline(
    Readable.fromWeb(res.body as never),
    createWriteStream(tarball),
  );
  await tarExtract({ file: tarball, cwd: dest });
  // Drop the tarball once extracted — only the unpacked driver is needed, and
  // keeping both would nearly double this driver's /tmp footprint.
  await rm(tarball, { force: true });
}

// Each driver tarball ships a MANIFEST (TOML) naming the shared library under
// `[Files] driver` and, when non-default, the ADBC entrypoint under
// `[Driver] entrypoint`. Read both rather than hardcoding per-driver names.
async function resolveDriverInfo(dest: string): Promise<DriverInfo> {
  const manifest = await readFile(join(dest, "MANIFEST"), "utf8");
  const driverMatch = /^\s*driver\s*=\s*"(.+?)"\s*$/m.exec(manifest);
  if (!driverMatch) {
    const entries = await readdir(dest);
    throw new Error(
      `MANIFEST in ${dest} has no [Files] driver; found: ${entries.join(", ")}`,
    );
  }
  const sharedLibPath = join(dest, driverMatch[1]);
  if (!(await pathExists(sharedLibPath))) {
    const entries = await readdir(dest);
    throw new Error(
      `Expected ${driverMatch[1]} in ${dest} after extract, found: ${entries.join(", ")}`,
    );
  }
  const entryMatch = /^\s*entrypoint\s*=\s*"(.+?)"\s*$/m.exec(manifest);
  return { sharedLibPath, entrypoint: entryMatch?.[1] };
}

// Total bytes of the regular files directly inside a driver directory. Driver
// tarballs are flat (the .so plus a few small license/manifest files), so a
// shallow scan is sufficient.
async function dirSize(dir: string): Promise<number> {
  let total = 0;
  let entries: string[];
  try {
    entries = await readdir(dir);
  } catch {
    return 0;
  }
  for (const name of entries) {
    try {
      const s = await stat(join(dir, name));
      if (s.isFile()) total += s.size;
    } catch {
      // File vanished (e.g. concurrent eviction) — ignore.
    }
  }
  return total;
}

// Evict least-recently-used cached driver directories until `incoming` bytes
// would fit within the cache budget. Directories currently being downloaded
// (in `building`) are never evicted, so a concurrent install can't be deleted.
async function evictToFit(incoming: number): Promise<void> {
  const dirs = new Map<string, number>(); // dir -> size
  let schemes: string[];
  try {
    schemes = await readdir(installRoot);
  } catch {
    return; // nothing cached yet
  }
  for (const scheme of schemes) {
    const schemeDir = join(installRoot, scheme);
    let versions: string[];
    try {
      versions = await readdir(schemeDir);
    } catch {
      continue;
    }
    for (const version of versions) {
      const dir = join(schemeDir, version);
      if (building.has(dir)) continue;
      dirs.set(dir, await dirSize(dir));
    }
  }

  let used = 0;
  for (const size of dirs.values()) used += size;

  // Evict oldest-first until the incoming driver fits within the budget.
  const ordered = [...dirs.keys()].sort(
    (a, b) => (lastUsed.get(a) ?? 0) - (lastUsed.get(b) ?? 0),
  );
  for (const dir of ordered) {
    if (used + incoming <= CACHE_BUDGET_BYTES) break;
    await rm(dir, { recursive: true, force: true });
    used -= dirs.get(dir) ?? 0;
    lastUsed.delete(dir);
  }
}

/**
 * Ensure the ADBC driver for the given URI scheme is present under /tmp,
 * resolving the latest version from the registry index and downloading the
 * Linux/amd64 build on first use. Concurrent requests for the same driver
 * version share a single download.
 */
export async function ensureDriver(scheme: string): Promise<DriverInfo> {
  const registry = await getRegistry();
  const driver = registry.drivers.find((d) => d.path === scheme);
  if (!driver) {
    const known = registry.drivers
      .map((d) => d.path)
      .sort()
      .join(", ");
    throw new Error(`Unsupported driver '${scheme}'. Known drivers: ${known}`);
  }

  const release = pickLatest(driver.pkginfo);
  const pkg = release.packages.find((p) => p.platform === PLATFORM);
  if (!pkg) {
    throw new Error(
      `Driver '${scheme}' ${release.version} has no ${PLATFORM} build`,
    );
  }

  const dest = join(installRoot, scheme, release.version);
  const url = `${CDN_BASE}/${pkg.url}`;

  // A present MANIFEST means a previous invocation already extracted this
  // version; re-derive the (cheap) driver info from it instead of downloading.
  if (await pathExists(join(dest, "MANIFEST"))) {
    touch(dest);
    return resolveDriverInfo(dest);
  }

  const key = `${scheme}@${release.version}`;
  let task = inflight.get(key);
  if (!task) {
    building.add(dest);
    task = (async () => {
      // Make room before extracting so the cache stays within budget. Estimate
      // the extracted size from the compressed download (~4x; measured ratios
      // are 3.0–3.7x) and evict other drivers' caches LRU-first to fit it.
      const compressed = await headContentLength(url);
      await evictToFit(compressed * 4);
      await downloadAndExtract(url, dest);
      touch(dest);
      return resolveDriverInfo(dest);
    })();
    task.finally(() => {
      inflight.delete(key);
      building.delete(dest);
    });
    inflight.set(key, task);
  }
  return task;
}
