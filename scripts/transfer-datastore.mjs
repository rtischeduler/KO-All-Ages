import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";

const BASE = "https://apis.roblox.com/datastores/v1/universes";
const WORKFLOW_FILE = "transfer-datastore.yml";
const CHECKPOINT_SIZE = 10000;
const REQUESTS_PER_MINUTE = 280;
const CONCURRENCY = 8;
const ALWAYS_SKIP = ["PlayerLevels_v2"];
const CHECKPOINT_BRANCH = "transfer-progress";
const CHECKPOINT_PATH = "checkpoint.json";
const SAVE_EVERY = 1000;
const MAX_SAVED_FAILURES = 2000;

const readEnv = (name, fallback = "") => (process.env[name] ?? fallback).trim();

const sourceUniverse = readEnv("SOURCE_UNIVERSE_ID");
const destUniverse = readEnv("DEST_UNIVERSE_ID");
const sourceKey = readEnv("ROBLOX_API_KEY_SOURCE");
const destKey = readEnv("ROBLOX_API_KEY_DEST") || sourceKey;
const storeInput = readEnv("DATASTORE_NAME");
const skipStoresInput = readEnv("SKIP_DATASTORES");
const skipStores = new Set([
  ...ALWAYS_SKIP,
  ...skipStoresInput
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean),
]);
const scopeInput = readEnv("SCOPE", "global") || "global";
const keyPrefix = readEnv("KEY_PREFIX");
const dryRun = readEnv("DRY_RUN", "true") === "true";
const maxEntries = parseInt(readEnv("MAX_ENTRIES", "0"), 10) || 0;
let resumeStore = "";
let resumeCursor = "";
let resumeDone = 0;
let resumeStoreDone = 0;
const ignoreCheckpoint = readEnv("IGNORE_CHECKPOINT", "false") === "true";
const ghRepo = readEnv("GITHUB_REPOSITORY");
const ghToken = readEnv("GITHUB_TOKEN");
const signature = [sourceUniverse, destUniverse, storeInput, scopeInput, keyPrefix].join("|");
const deadlineMinutes = parseInt(readEnv("DEADLINE_MINUTES", "330"), 10) || 330;

const startedAt = Date.now();
const deadline = startedAt + deadlineMinutes * 60000;

const missing = [];
if (!sourceUniverse) missing.push("SOURCE_UNIVERSE_ID");
if (!destUniverse) missing.push("DEST_UNIVERSE_ID");
if (!sourceKey) missing.push("ROBLOX_API_KEY_SOURCE");
if (missing.length) {
  console.error(`Missing required values: ${missing.join(", ")}`);
  process.exit(1);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function makeLimiter(perMinute) {
  const interval = 60000 / perMinute;
  let next = 0;
  return async () => {
    const now = Date.now();
    const at = Math.max(now, next);
    next = at + interval;
    if (at > now) await sleep(at - now);
  };
}

const sourceLimiter = makeLimiter(REQUESTS_PER_MINUTE);
const destReadLimiter = makeLimiter(REQUESTS_PER_MINUTE);
const destWriteLimiter = makeLimiter(REQUESTS_PER_MINUTE);

async function api(apiKey, limiter, url, options = {}, attempt = 0) {
  await limiter();
  const res = await fetch(url, {
    ...options,
    headers: { "x-api-key": apiKey, ...(options.headers || {}) },
  });
  if ((res.status === 429 || res.status >= 500) && attempt < 10) {
    const retryAfter = Number(res.headers.get("retry-after"));
    const wait = retryAfter > 0 ? retryAfter * 1000 : Math.min(60000, 1000 * 2 ** attempt);
    await res.arrayBuffer().catch(() => {});
    await sleep(wait);
    return api(apiKey, limiter, url, options, attempt + 1);
  }
  return res;
}

async function failure(label, res) {
  const text = await res.text().catch(() => "");
  return new Error(`${label} failed: HTTP ${res.status} ${text.slice(0, 300)}`);
}

async function listStores() {
  const names = [];
  let cursor = "";
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (cursor) params.set("cursor", cursor);
    const res = await api(
      sourceKey,
      sourceLimiter,
      `${BASE}/${sourceUniverse}/standard-datastores?${params}`
    );
    if (!res.ok) throw await failure("Listing data stores", res);
    const data = await res.json();
    for (const store of data.datastores || []) names.push(store.name);
    cursor = data.nextPageCursor || "";
  } while (cursor);
  return names;
}

class StoreDeletedError extends Error {}
const isDeletedBody = (text) => /DatastoreDeleted|Datastore is deleted/i.test(text);

function fetchKeyPage(store, cursor) {
  const params = new URLSearchParams({ datastoreName: store, limit: "100" });
  if (scopeInput === "all") params.set("allScopes", "true");
  else params.set("scope", scopeInput);
  if (keyPrefix) params.set("prefix", keyPrefix);
  if (cursor) params.set("cursor", cursor);
  return api(
    sourceKey,
    sourceLimiter,
    `${BASE}/${sourceUniverse}/standard-datastores/datastore/entries?${params}`
  );
}

// Used when a saved cursor is no longer accepted: walk the key list only (no reads,
// about 100 keys per request) until roughly skipCount keys have been passed.
async function fastForward(store, skipCount) {
  const target = Math.max(0, skipCount - 100);
  let cursor = "";
  let skipped = 0;
  while (skipped < target) {
    const res = await fetchKeyPage(store, cursor);
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      if (isDeletedBody(body)) throw new StoreDeletedError(store);
      throw new Error(`Fast-forwarding in "${store}" failed: HTTP ${res.status} ${body.slice(0, 300)}`);
    }
    const data = await res.json();
    skipped += (data.keys || []).length;
    cursor = data.nextPageCursor || "";
    if (!cursor) break;
  }
  return { cursor, skipped };
}

async function* listPages(store, startCursor, startStoreDone) {
  let cursor = startCursor;
  let position = startStoreDone;
  let first = true;
  while (true) {
    const res = await fetchKeyPage(store, cursor);
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      if (isDeletedBody(body)) throw new StoreDeletedError(store);
      if (first && cursor) {
        console.log(`  Saved cursor was rejected (HTTP ${res.status}).`);
        if (startStoreDone > 0) {
          console.log(`  Fast-forwarding past about ${startStoreDone} keys by listing only...`);
        } else {
          console.log("  No per-store count saved, so this data store restarts from the beginning.");
        }
        const ff = await fastForward(store, startStoreDone);
        cursor = ff.cursor;
        position = ff.skipped;
        first = false;
        if (startStoreDone > 0 && !cursor) return;
        continue;
      }
      throw new Error(`Listing keys in "${store}" failed: HTTP ${res.status} ${body.slice(0, 300)}`);
    }
    first = false;
    const data = await res.json();
    const next = data.nextPageCursor || "";
    const keys = (data.keys || []).map((entry) => ({
      scope: entry.scope || scopeInput,
      key: entry.key,
    }));
    position += keys.length;
    yield { keys, next, position };
    if (!next) return;
    cursor = next;
  }
}

async function readEntry(universe, apiKey, limiter, store, scope, key) {
  const params = new URLSearchParams({ datastoreName: store, entryKey: key, scope });
  const res = await api(
    apiKey,
    limiter,
    `${BASE}/${universe}/standard-datastores/datastore/entries/entry?${params}`
  );
  if (res.status === 404) {
    await res.arrayBuffer().catch(() => {});
    return null;
  }
  if (!res.ok) throw await failure("Reading entry", res);
  return {
    body: Buffer.from(await res.arrayBuffer()),
    attributes: res.headers.get("roblox-entry-attributes"),
    userIds: res.headers.get("roblox-entry-userids"),
  };
}

function sortedJson(value) {
  if (Array.isArray(value)) return value.map(sortedJson);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((name) => [name, sortedJson(value[name])])
    );
  }
  return value;
}

function normalizeAttributes(raw) {
  if (!raw) return "{}";
  try {
    return JSON.stringify(sortedJson(JSON.parse(raw)));
  } catch {
    return raw;
  }
}

function normalizeUserIds(raw) {
  if (!raw) return "[]";
  try {
    return JSON.stringify([...JSON.parse(raw)].map(Number).sort((a, b) => a - b));
  } catch {
    return raw;
  }
}

function entriesMatch(a, b) {
  return (
    a.body.equals(b.body) &&
    normalizeAttributes(a.attributes) === normalizeAttributes(b.attributes) &&
    normalizeUserIds(a.userIds) === normalizeUserIds(b.userIds)
  );
}

async function writeEntry(store, scope, key, entry) {
  const params = new URLSearchParams({ datastoreName: store, entryKey: key, scope });
  const headers = {
    "content-type": "application/json",
    "content-md5": createHash("md5").update(entry.body).digest("base64"),
  };
  if (entry.attributes) headers["roblox-entry-attributes"] = entry.attributes;
  if (entry.userIds) headers["roblox-entry-userids"] = entry.userIds;
  const res = await api(
    destKey,
    destWriteLimiter,
    `${BASE}/${destUniverse}/standard-datastores/datastore/entries/entry?${params}`,
    { method: "POST", headers, body: entry.body }
  );
  if (!res.ok) throw await failure("Writing entry", res);
  await res.arrayBuffer().catch(() => {});
}

const totals = { seen: 0, written: 0, updated: 0, unchanged: 0, missing: 0, failed: 0 };
const failures = [];
const failedItems = [];

async function transferOne(store, item) {
  try {
    const source = await readEntry(
      sourceUniverse,
      sourceKey,
      sourceLimiter,
      store,
      item.scope,
      item.key
    );
    if (!source) {
      totals.missing++;
      return;
    }
    const existing = await readEntry(
      destUniverse,
      destKey,
      destReadLimiter,
      store,
      item.scope,
      item.key
    );
    if (existing && entriesMatch(source, existing)) {
      totals.unchanged++;
      return;
    }
    await writeEntry(store, item.scope, item.key, source);
    if (existing) totals.updated++;
    else totals.written++;
  } catch (error) {
    totals.failed++;
    failures.push(`${store} / ${item.scope} / ${item.key}: ${error.message}`);
    failedItems.push({ store, scope: item.scope, key: item.key });
  }
}

async function runPool(items, worker) {
  let index = 0;
  const runners = Array.from({ length: Math.min(CONCURRENCY, items.length) }, async () => {
    while (index < items.length) {
      const item = items[index++];
      await worker(item);
    }
  });
  await Promise.all(runners);
}

let progress = null;
let checkpointSha = null;
let branchReady = false;
let saveChain = Promise.resolve();
const canCheckpoint = () => !dryRun && Boolean(ghRepo && ghToken);
const ghHeaders = {
  authorization: `Bearer ${ghToken}`,
  accept: "application/vnd.github+json",
  "x-github-api-version": "2022-11-28",
  "content-type": "application/json",
};
const ghUrl = (path) => `https://api.github.com/repos/${ghRepo}/${path}`;

function buildState(complete = false) {
  return {
    signature,
    complete,
    store: complete ? null : progress?.store ?? null,
    cursor: complete ? "" : progress?.cursor ?? "",
    storeDone: complete ? 0 : progress?.storeDone ?? 0,
    done: resumeDone + totals.seen,
    failed: failedItems.slice(-MAX_SAVED_FAILURES),
    updatedAt: new Date().toISOString(),
  };
}

async function loadCheckpoint() {
  if (!canCheckpoint() || ignoreCheckpoint) return null;
  try {
    const res = await fetch(ghUrl(`contents/${CHECKPOINT_PATH}?ref=${CHECKPOINT_BRANCH}`), {
      headers: ghHeaders,
    });
    if (res.status === 404) return null;
    if (!res.ok) {
      console.log(`Could not read saved progress: HTTP ${res.status}`);
      return null;
    }
    const file = await res.json();
    checkpointSha = file.sha;
    branchReady = true;
    const data = JSON.parse(Buffer.from(file.content, "base64").toString("utf8"));
    if (data.signature !== signature) {
      console.log("Saved progress is for a different transfer setup, ignoring it.");
      return null;
    }
    return data;
  } catch (error) {
    console.log(`Could not read saved progress: ${error.message}`);
    return null;
  }
}

async function ensureBranch() {
  if (branchReady) return;
  const check = await fetch(ghUrl(`git/ref/heads/${CHECKPOINT_BRANCH}`), { headers: ghHeaders });
  if (check.status === 404) {
    const res = await fetch(ghUrl("git/refs"), {
      method: "POST",
      headers: ghHeaders,
      body: JSON.stringify({
        ref: `refs/heads/${CHECKPOINT_BRANCH}`,
        sha: readEnv("GITHUB_SHA"),
      }),
    });
    if (!res.ok) throw await failure("Creating progress branch", res);
  }
  branchReady = true;
}

async function refreshSha() {
  const res = await fetch(ghUrl(`contents/${CHECKPOINT_PATH}?ref=${CHECKPOINT_BRANCH}`), {
    headers: ghHeaders,
  });
  checkpointSha = res.ok ? (await res.json()).sha : null;
}

async function putCheckpoint(state, retry = true) {
  await ensureBranch();
  const body = {
    message: "Update transfer progress",
    content: Buffer.from(JSON.stringify(state, null, 1)).toString("base64"),
    branch: CHECKPOINT_BRANCH,
  };
  if (checkpointSha) body.sha = checkpointSha;
  const res = await fetch(ghUrl(`contents/${CHECKPOINT_PATH}`), {
    method: "PUT",
    headers: ghHeaders,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    if (retry && (res.status === 409 || res.status === 422)) {
      await res.arrayBuffer().catch(() => {});
      await refreshSha();
      return putCheckpoint(state, false);
    }
    throw await failure("Saving progress", res);
  }
  checkpointSha = (await res.json()).content?.sha ?? checkpointSha;
}

function saveCheckpoint(state) {
  if (!canCheckpoint()) return Promise.resolve();
  saveChain = saveChain
    .then(() => putCheckpoint(state))
    .catch((error) => console.log(`Could not save progress: ${error.message}`));
  return saveChain;
}

let shuttingDown = false;
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    if (progress) {
      console.log(`\n${signal} received, saving progress before exit...`);
      await saveCheckpoint(buildState());
    }
    process.exit(130);
  });
}

// Reads the logs of earlier runs (newest first) and returns the last real checkpoint
// from a run with the same universes, scope and (if set) data store.
function parseLogCheckpoint(text) {
  if (/DRY RUN: Transferring/.test(text)) return null;
  const head = text.match(/Transferring from universe (\d+) to universe (\d+)/);
  if (!head || head[1] !== sourceUniverse || head[2] !== destUniverse) return null;
  let found = null;
  let done = null;
  for (const line of text.split("\n")) {
    const scope = line.match(/Data store "[^"]*" \(scope: ([^)]*)\)/);
    if (scope && scope[1] !== scopeInput) return null;
    const d = line.match(/Checkpoint: (\d+) entries done overall/);
    if (d) done = Number(d[1]);
    const r = line.match(/Resume point: store="([^"]*)" cursor="([^"]*)"(?: store_done=(\d+))?\s*$/);
    if (r && (!storeInput || r[1] === storeInput)) {
      found = { store: r[1], cursor: r[2], storeDone: Number(r[3] || 0), done: done ?? 0 };
    }
  }
  return found;
}

async function resumeFromOldLogs() {
  if (!canCheckpoint()) return null;
  const res = await fetch(ghUrl(`actions/workflows/${WORKFLOW_FILE}/runs?per_page=15`), {
    headers: ghHeaders,
  });
  if (!res.ok) {
    console.log(`Could not list earlier runs: HTTP ${res.status}`);
    return null;
  }
  const currentRun = readEnv("GITHUB_RUN_ID");
  const runs = ((await res.json()).workflow_runs || []).filter(
    (run) => String(run.id) !== currentRun && run.status === "completed"
  );
  for (const run of runs) {
    try {
      const text = execFileSync("gh", ["run", "view", String(run.id), "--repo", ghRepo, "--log"], {
        env: { ...process.env, GH_TOKEN: ghToken },
        maxBuffer: 1 << 29,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      });
      const found = parseLogCheckpoint(text);
      if (found) {
        console.log(`Found the last checkpoint in the log of run ${run.id} (${run.created_at})`);
        return found;
      }
    } catch (error) {
      console.log(`Could not read the log of run ${run.id}: ${String(error.message).slice(0, 200)}`);
    }
  }
  return null;
}

async function retryFailed(items) {
  if (!items.length) return;
  console.log(`Retrying ${items.length} keys that failed in an earlier run`);
  await runPool(items, (item) => transferOne(item.store, item));
}

function logCheckpoint(store, cursor) {
  const done = resumeDone + totals.seen;
  console.log(
    `  Checkpoint: ${done} entries done overall, ${progress?.storeDone ?? 0} in this store (new ${totals.written}, updated ${totals.updated}, unchanged ${totals.unchanged}, failed ${totals.failed} this run)`
  );
  console.log(
    `  Resume point: store="${store}" cursor="${cursor}" store_done=${progress?.storeDone ?? 0}`
  );
}

async function processStore(store, startCursor, startStoreDone) {
  console.log(`\nData store "${store}" (scope: ${scopeInput})`);
  progress = { store, cursor: startCursor, storeDone: startStoreDone };
  let sinceCheckpoint = 0;
  let sinceSave = 0;
  let storeCount = 0;

  for await (const page of listPages(store, startCursor, startStoreDone)) {
    let keys = page.keys;
    let capped = false;
    if (maxEntries) {
      const remaining = maxEntries - totals.seen;
      if (keys.length >= remaining) {
        keys = keys.slice(0, remaining);
        capped = true;
      }
    }

    totals.seen += keys.length;
    storeCount += keys.length;
    sinceCheckpoint += keys.length;
    sinceSave += keys.length;
    if (!dryRun) await runPool(keys, (item) => transferOne(store, item));

    if (capped) {
      console.log(`  ${storeCount} entries ${dryRun ? "found" : "handled"} in "${store}"`);
      return { capped: true };
    }

    if (page.next) progress = { store, cursor: page.next, storeDone: page.position };

    if (page.next && sinceSave >= SAVE_EVERY) {
      await saveCheckpoint(buildState());
      sinceSave = 0;
    }

    if (page.next && sinceCheckpoint >= CHECKPOINT_SIZE) {
      logCheckpoint(store, page.next);
      sinceCheckpoint = 0;
    }

    if (page.next && Date.now() >= deadline) {
      console.log(`  ${storeCount} entries ${dryRun ? "found" : "handled"} in "${store}" this run`);
      return { stop: true, cursor: page.next, storeDone: page.position };
    }
  }

  console.log(`  ${storeCount} entries ${dryRun ? "found" : "handled"} in "${store}"`);
  return {};
}

async function dispatchNext(store, cursor, storeDone) {
  const repo = readEnv("GITHUB_REPOSITORY");
  const token = readEnv("GITHUB_TOKEN");
  const ref = readEnv("GITHUB_REF_NAME", "main");
  if (!repo || !token) return false;

  const inputs = {
    source_universe_id: sourceUniverse,
    destination_universe_id: destUniverse,
    scope: scopeInput,
    max_entries: "0",
    dry_run: "false",
  };
  if (storeInput) inputs.datastore_name = storeInput;
  if (skipStoresInput) inputs.skip_datastores = skipStoresInput;
  if (keyPrefix) inputs.key_prefix = keyPrefix;

  const res = await fetch(
    `https://api.github.com/repos/${repo}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "x-github-api-version": "2022-11-28",
        "content-type": "application/json",
      },
      body: JSON.stringify({ ref, inputs }),
    }
  );
  if (!res.ok) {
    console.log(`Could not start the next run: HTTP ${res.status} ${(await res.text()).slice(0, 300)}`);
  }
  return res.ok;
}

function printSummary() {
  console.log("\nSummary for this run");
  console.log(`  entries listed: ${totals.seen}`);
  console.log(`  entries done overall: ${resumeDone + totals.seen}`);
  if (dryRun) {
    console.log("  dry run enabled, nothing was written");
    return;
  }
  console.log(`  new in destination: ${totals.written}`);
  console.log(`  overwritten (values differed): ${totals.updated}`);
  console.log(`  unchanged (already matched): ${totals.unchanged}`);
  console.log(`  vanished before read: ${totals.missing}`);
  console.log(`  failed: ${totals.failed}`);
  if (failures.length) {
    console.log("\nFailures");
    for (const line of failures.slice(0, 100)) console.log(`  ${line}`);
    if (failures.length > 100) console.log(`  ...and ${failures.length - 100} more`);
  }
}

async function main() {
  console.log(
    `${dryRun ? "DRY RUN: " : ""}Transferring from universe ${sourceUniverse} to universe ${destUniverse}`
  );

  const saved = await loadCheckpoint();
  let retryItems = [];
  if (saved) {
    retryItems = saved.failed || [];
    if (saved.complete) {
      console.log(
        "Saved progress says this transfer already finished (set ignore_checkpoint to run it all again)."
      );
      await retryFailed(retryItems);
      await saveCheckpoint(buildState(true));
      printSummary();
      if (failedItems.length) process.exit(1);
      return;
    }
    if (!resumeStore && saved.store) {
      resumeStore = saved.store;
      resumeCursor = saved.cursor || "";
      resumeDone = saved.done || 0;
      resumeStoreDone = saved.storeDone || 0;
      console.log(`Continuing from saved progress (${resumeDone} entries done overall)`);
    }
  }
  if (!saved && !ignoreCheckpoint) {
    const fromLogs = await resumeFromOldLogs();
    if (fromLogs) {
      resumeStore = fromLogs.store;
      resumeCursor = fromLogs.cursor;
      resumeDone = fromLogs.done;
      resumeStoreDone = fromLogs.storeDone;
    } else {
      console.log("No saved progress or earlier checkpoint found, starting from the beginning.");
    }
  }
  if (resumeStore) {
    console.log(`Resuming in "${resumeStore}" after ${resumeDone} entries`);
  }

  const allStores = storeInput ? [storeInput] : await listStores();
  let stores = allStores;
  if (!storeInput && skipStores.size) {
    const skipped = stores.filter((name) => skipStores.has(name));
    stores = stores.filter((name) => !skipStores.has(name));
    if (skipped.length) console.log(`Skipping data stores: ${skipped.join(", ")}`);
  }
  if (!stores.length) {
    console.log("No data stores to process.");
    return;
  }
  console.log(`Data stores to process: ${stores.length}`);

  await retryFailed(retryItems);

  let startIndex = 0;
  if (resumeStore) {
    startIndex = stores.indexOf(resumeStore);
    if (startIndex < 0) {
      const position = allStores.indexOf(resumeStore);
      if (position >= 0) {
        startIndex = stores.findIndex((name) => allStores.indexOf(name) > position);
        if (startIndex < 0) startIndex = stores.length;
      } else if (skipStores.has(resumeStore)) {
        // The resume store no longer exists. The API lists stores in name order, so
        // everything before it is already done: continue with the first store after it.
        const plain = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
        const folded = (a, b) => plain(a.toLowerCase(), b.toLowerCase());
        const order = [plain, folded].find((cmp) =>
          allStores.every((name, i) => i === 0 || cmp(allStores[i - 1], name) <= 0)
        );
        if (order) {
          startIndex = stores.findIndex((name) => order(name, resumeStore) > 0);
          if (startIndex < 0) startIndex = stores.length;
          console.log(`Data store "${resumeStore}" no longer exists; continuing with the stores listed after it.`);
        } else {
          startIndex = 0;
          console.log("Store list is not in name order, so starting from the first data store.");
        }
      } else {
        throw new Error(`Resume data store "${resumeStore}" was not found`);
      }
      console.log(`Resume data store "${resumeStore}" is skipped, moving on to the next one.`);
      resumeCursor = "";
      resumeStoreDone = 0;
    }
  }

  let handoff = null;
  let capped = false;
  for (let i = startIndex; i < stores.length; i++) {
    const first = i === startIndex;
    let result;
    try {
      result = await processStore(
        stores[i],
        first ? resumeCursor : "",
        first ? resumeStoreDone : 0
      );
    } catch (error) {
      if (!(error instanceof StoreDeletedError)) throw error;
      console.log(`  Data store "${stores[i]}" is deleted, skipping it.`);
      result = {};
    }
    if (result.capped) {
      capped = true;
      break;
    }
    if (result.stop) {
      handoff = { store: stores[i], cursor: result.cursor, storeDone: result.storeDone };
      break;
    }
    progress = i + 1 < stores.length ? { store: stores[i + 1], cursor: "", storeDone: 0 } : null;
  }

  if (!dryRun && !capped) await saveCheckpoint(buildState(!handoff));

  printSummary();

  if (handoff) {
    const canChain = !dryRun && !maxEntries;
    console.log(`  Checkpoint: ${resumeDone + totals.seen} entries done overall`);
    console.log(
      `  Resume point: store="${handoff.store}" cursor="${handoff.cursor}" store_done=${handoff.storeDone}`
    );
    if (canChain && (await dispatchNext(handoff.store, handoff.cursor, handoff.storeDone))) {
      console.log("\nTime limit reached, started the next run to continue automatically.");
    } else {
      console.log("\nTime limit reached. Progress is saved, run the workflow again to continue.");
      if (canChain) process.exit(1);
    }
  }

  if (failures.length) process.exit(1);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
