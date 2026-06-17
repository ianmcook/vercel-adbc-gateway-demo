import { mkdir, readdir, readFile, stat } from "node:fs/promises";
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
  // A present MANIFEST means a previous invocation already extracted this
  // version; re-derive the (cheap) driver info from it instead of downloading.
  if (await pathExists(join(dest, "MANIFEST"))) {
    return resolveDriverInfo(dest);
  }

  const key = `${scheme}@${release.version}`;
  let task = inflight.get(key);
  if (!task) {
    task = (async () => {
      await downloadAndExtract(`${CDN_BASE}/${pkg.url}`, dest);
      return resolveDriverInfo(dest);
    })();
    task.finally(() => inflight.delete(key));
    inflight.set(key, task);
  }
  return task;
}
