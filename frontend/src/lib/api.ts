// Thin typed client for the Catchment HTTP API. In dev, next.config rewrites /api/* to the FastAPI
// server (DUCKSTRING_CATCHMENT_URL / :8000); in the static-export build FastAPI serves this app —
// possibly under a path prefix (a reverse proxy, Posit Connect's /content/{guid}/), so the API base
// is derived from where the page is mounted rather than hard-coded to the origin root.

import type { FreqUnit, ViewPayload } from './types';

function apiBase(): string {
  if (typeof window === 'undefined') return '/api';
  const p = window.location.pathname;
  // The app is a single root-level page: its directory is the Catchment mount point.
  const dir = p.endsWith('/') ? p : p.slice(0, p.lastIndexOf('/') + 1);
  return `${dir}api`;
}

// ─── API key (for a Catchment started with --key) ────────────────────────────
// Kept in localStorage; attached as a Bearer header on every request. A 401 raises
// UnauthorizedError so the store can surface the key prompt.

const KEY_STORAGE = 'duckstring.apiKey';

export class UnauthorizedError extends Error {
  constructor() {
    super('The Catchment requires an API key');
    this.name = 'UnauthorizedError';
  }
}

export function getApiKey(): string | null {
  try {
    return window.localStorage.getItem(KEY_STORAGE);
  } catch {
    return null;
  }
}

export function setApiKey(key: string | null): void {
  try {
    if (key) window.localStorage.setItem(KEY_STORAGE, key);
    else window.localStorage.removeItem(KEY_STORAGE);
  } catch {
    /* storage unavailable — the key just won't persist */
  }
}

// Key handoff: a console can deep-link the UI as {url}/#key={apiKey} — the key is moved into
// localStorage on load and stripped from the URL (history.replaceState, so it never lands in the
// address bar, browser history, or a copy-paste). Runs once at module init, before the first fetch.
function adoptHashKey(): void {
  try {
    const m = window.location.hash.match(/^#key=([^&]+)/);
    if (!m) return;
    setApiKey(decodeURIComponent(m[1]));
    window.history.replaceState(null, '', window.location.pathname + window.location.search);
  } catch {
    /* no window (SSR/export prerender) or storage unavailable — the hash is simply ignored */
  }
}
if (typeof window !== 'undefined') adoptHashKey();

function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const key = getApiKey();
  return key ? { ...extra, authorization: `Bearer ${key}` } : extra;
}

// ─── Raw payload shapes (snake_case, as the backend emits) ───────────────────

export interface RawRipple {
  name: string;
  status: 'running' | 'queued' | 'idle';
  gen: number;
  runs_completed: number;
  has_pull: boolean;
  target_f: string | null;
  start_f: string | null;
  end_f: string | null;
}

export interface RawPond {
  id: string; // the pond key "name@major" — the runtime identity of one major line
  name: string;
  major: number;
  kind: string;
  is_draw: boolean; // a Pond Draw — fed by a duct from an upstream Catchment, not run by a Duck
  is_spout: boolean; // a Spout — egresses its source's output to an external system (run by the egress worker)
  spout: { destination: string; table: string | null; mode: string; armed: boolean } | null;
  version: string;
  has_tables: boolean; // this major line has published at least one table — the data viewer is offered
  has_objects: boolean; // this major line has published at least one non-tabular Object — the Objects tab is offered
  status: 'running' | 'queued' | 'idle' | 'failed' | 'killed' | 'blocked' | 'repairing';
  gen: number;
  runs_completed: number;
  has_pull: boolean;
  target_f: string | null;
  start_f: string | null;
  end_f: string | null;
  d_ms: number;
  trigger: { kind: 'wave' | 'tide'; bound_ms: number | null } | null;
  is_failed: boolean;
  is_blocked: boolean;
  blocked_reason: string | null; // e.g. waiting for a Source asset a Ripple couldn't read (Mechanism 2)
  is_killed: boolean;
  refresh_pending: boolean; // next run is a cold wipe-and-rebuild (control refresh)
  repairing: boolean; // in an active repair plan — blocked from normal demand
  failed_f: string | null;
  failures: number;
  missing_sources: string[]; // declared Sources absent from the Catchment (pond keys "name@major")
  blocked_by: string[]; // required Sources that are down (failed/killed/blocked) — the upstream block
  error: string | null; // failure message of the freshest failed Run, when failed
  failure_kind?: 'contract' | 'error' | null; // the failed sub-reason (contract = the schema gate refused the publish)
  immediate_retries: number;
  source_retries: number;
  // The effective compute config (Duck target/size + Flock posture) with declared/override provenance;
  // null for Draws/Spouts (no Duck runs them). See plans/cloud-config.md.
  duck: DuckConfig | null;
  ripples: RawRipple[];
  ripple_edges: [string, string][]; // [sourceName, sinkName] within the Pond
}

// A named Duck Pool — Catchment-level remote compute (plans/cloud-config.md). On DSC the S/M/L/XL
// presets are just pools; the picker renders whatever /catchment/duck-pools returns.
export interface DuckPool {
  name: string;
  provider: string; // 'fargate' (default) | 'ec2'
  instance_type: string | null; // EC2 pools
  cpu: number | null; // Fargate task cpu units
  memory: number | null; // Fargate task memory (MiB)
  min_instances: number;
  max_instances: number;
  idle_timeout: number | null;
  keep_warm: number;
  region: string | null;
  managed?: boolean; // a built-in preset (S/M/L/XL) — not editable/removable
}

// A Pond's effective compute config = override ?? declared ?? Catchment default (coalesce). Sizing is
// concrete (a pool / dedicated instance type) — there is no abstract size field.
export interface DuckConfig {
  duck_target: string; // 'catchment' | a pool name | 'dedicated'
  remote: boolean;
  pool: DuckPool | null;
  dedicated_instance_type: string | null;
  dedicated_auto_stop: boolean | null;
  flock_mode: string; // 'off' | 'upgrade' | 'always'
  flock_engine: string | null;
  oom_policy: string; // 'fail_up' | 'fail'
  declared: {
    duck_target?: string | null;
    flock_mode?: string | null;
    flock_engine?: string | null;
    oom_policy?: string | null;
  };
  override: {
    duck_target: string | null;
    dedicated_instance_type: string | null;
    dedicated_auto_stop: boolean | null;
    flock_mode: string | null;
    flock_engine: string | null;
    oom_policy: string | null;
  };
  defaults: { duck_target: string; flock_mode: string; flock_engine: string | null; oom_policy: string };
}

// The cloud-enable gate — remote data root + AWS creds (plans/cloud-config.md). Surfaced on /api/status
// so the UI greys out remote-compute options until both hold.
export interface CloudGate {
  data_root: string | null;
  data_root_remote: boolean;
  aws_configured: boolean;
  cloud_enabled: boolean;
}

// The caller's access level — a total order read ⊂ demand ⊂ full. The UI gates its controls on it.
export type AccessLevel = 'read' | 'demand' | 'full';

export interface StatusPayload {
  catchment: { id: string | null; name: string | null } | null; // this Catchment's stable identity
  version: number; // change token for the /api/status long-poll (pass back as ?since=)
  access_level: AccessLevel; // the caller's level (always 'full' when the Catchment is open/unauthed)
  cloud?: CloudGate; // the cloud-enable gate + reasons (remote data root + AWS creds)
  ponds: RawPond[];
  edges: [string, string][]; // [sourceId, sinkId] — pond keys ("name@major")
}

export interface RawRippleRun {
  ripple: string;
  started_at: string | null;
  finished_at: string | null;
  status: string;
  retry: number;
  error: string | null;
  traceback: string | null;
}

export interface RawPondRun {
  pond: string;
  id: string; // pond key "name@major"
  major: number;
  version: string;
  f: string;
  started_at: string | null;
  finished_at: string | null;
  status: string;
  error: string | null;
  traceback: string | null;
  ripples?: RawRippleRun[];
}

export interface RawWindow {
  name: string;
  start_anchor: string;
  duration_seconds: number;
  freq_unit: FreqUnit;
  freq_interval: number;
  valid_days: string | null;
  until_time: string | null;
}

// ─── Requests ────────────────────────────────────────────────────────────────

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${apiBase()}${path}`, { cache: 'no-store', headers: authHeaders() });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error(`GET ${path} → ${res.status}`);
  return res.json() as Promise<T>;
}

async function postJSON(path: string, body: unknown = {}): Promise<void> {
  const res = await fetch(`${apiBase()}${path}`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify(body),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) {
    let detail = '';
    try {
      detail = (await res.json())?.detail ?? '';
    } catch {
      /* no body */
    }
    throw new Error(detail || `POST ${path} → ${res.status}`);
  }
}

export function fetchStatus(since?: number): Promise<StatusPayload> {
  // `since` long-polls: the request holds until the state moves past that version (or a heartbeat).
  return getJSON<StatusPayload>(since === undefined ? '/status' : `/status?since=${since}`);
}

export function fetchView(): Promise<ViewPayload> {
  return getJSON<ViewPayload>('/view');
}

// A pond id ("name@major") → the route path + query addressing that major line. All pond-targeting
// routes are keyed by name with a `major` query param.
function pondPath(id: string, rest: string): string {
  const at = id.lastIndexOf('@');
  const name = at === -1 ? id : id.slice(0, at);
  const major = at === -1 ? null : id.slice(at + 1);
  const suffix = major === null ? '' : `${rest.includes('?') ? '&' : '?'}major=${major}`;
  return `/ponds/${encodeURIComponent(name)}/${rest}${suffix}`;
}

export interface RunsQuery {
  pond?: string | null; // a pond id ("name@major")
  lineage?: boolean;
  ripples?: boolean;
  limit?: number;
  after?: string; // UTC ISO — runs started at/after this bound
  before?: string; // UTC ISO — runs started at/before this bound
}

export async function fetchRuns(q: RunsQuery = {}): Promise<RawPondRun[]> {
  const params = new URLSearchParams();
  if (q.pond) {
    const at = q.pond.lastIndexOf('@');
    params.set('pond', at === -1 ? q.pond : q.pond.slice(0, at));
    if (at !== -1) params.set('major', q.pond.slice(at + 1));
  }
  if (q.lineage !== undefined) params.set('lineage', String(q.lineage));
  if (q.ripples !== undefined) params.set('ripples', String(q.ripples));
  if (q.limit !== undefined) params.set('limit', String(q.limit));
  if (q.after) params.set('after', q.after);
  if (q.before) params.set('before', q.before);
  const qs = params.toString();
  const data = await getJSON<{ runs: RawPondRun[] }>(`/runs${qs ? `?${qs}` : ''}`);
  return data.runs;
}

// Trigger / demand actions (the CLI `trigger` surface). `pond` is a pond id ("name@major");
// `endpoint` is the route segment under /api/ponds/{name}/ — tap | pulse | wave | tide | wake |
// sleep | force | kill | untrigger.
export function postTrigger(pond: string, endpoint: string, body: unknown = {}): Promise<void> {
  return postJSON(pondPath(pond, endpoint), body);
}

// Refresh: flag a Pond so its next run is a cold wipe-and-rebuild (or `clear` to un-flag).
export function refreshPond(pond: string, clear = false): Promise<void> {
  return postJSON(pondPath(pond, clear ? 'refresh?clear=true' : 'refresh'));
}

// Repair: force-rebuild a connected set of Ponds now (ids "name@major"). Throws the server's detail
// (e.g. a disconnected set) on a 4xx so the caller can surface it.
export async function repairPonds(
  ids: string[],
  downstream: boolean,
): Promise<{ scope: string[] }> {
  const ponds = ids.map((id) => {
    const at = id.lastIndexOf('@');
    return { name: at === -1 ? id : id.slice(0, at), major: at === -1 ? null : Number(id.slice(at + 1)) };
  });
  const res = await fetch(`${apiBase()}/repair`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify({ ponds, downstream }),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `repair failed (${res.status})`);
  return res.json();
}

// The bulk-operation vocabulary (see plans/selector_ui.md), in application-precedence order.
export const BATCH_OPS = ['kill', 'sleep', 'reset', 'wipe', 'remove', 'clear', 'repair', 'refresh'] as const;
export type BatchOp = (typeof BATCH_OPS)[number];

export interface BatchResult {
  operations: BatchOp[];
  ponds: string[];
  removed: string[];
  errors: { pond: string | null; op: string; error: string }[];
}

// Apply a set of operations to a set of Ponds (ids "name@major"), in precedence order. `confirm` is the
// catchment name, required by the server when any of reset/wipe/remove is present. Throws the server's
// detail on a 4xx (bad op-set, missing/wrong confirm) so the caller can surface it.
export async function batchPonds(
  ids: string[],
  operations: BatchOp[],
  confirm: string | null,
): Promise<BatchResult> {
  const ponds = ids.map((id) => {
    const at = id.lastIndexOf('@');
    return { name: at === -1 ? id : id.slice(0, at), major: at === -1 ? null : Number(id.slice(at + 1)) };
  });
  const res = await fetch(`${apiBase()}/ponds/batch`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify({ ponds, operations, confirm }),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `batch failed (${res.status})`);
  return res.json();
}

// Reset the whole Catchment to a fresh-deploy state (scrub data + state; keep deploys/config/secrets).
export async function resetCatchment(clearHistory = false): Promise<{ ponds: number }> {
  const res = await fetch(`${apiBase()}/catchment/reset`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify({ clear_history: clearHistory }),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `reset failed (${res.status})`);
  return res.json();
}

// Remove (retire) a Pond major line — deletes its data, config, Spouts + alerts; keeps history. Full only.
// With `wipe`, also purges the deployment record + run history + artifacts (as if never deployed).
export async function removePond(pond: string, wipe = false): Promise<{ removed: string; spouts_removed: string[]; now_blocked: string[]; wiped: boolean }> {
  const { name, major } = splitPond(pond);
  const params = new URLSearchParams();
  if (major !== undefined) params.set('major', String(major));
  if (wipe) params.set('wipe', 'true');
  const qs = params.toString() ? `?${params}` : '';
  const res = await fetch(`${apiBase()}/ponds/${encodeURIComponent(name)}${qs}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `remove failed (${res.status})`);
  return res.json();
}

// Failure management.
export function clearFailure(pond: string): Promise<void> {
  return postJSON(pondPath(pond, 'clear'));
}

export function setBudget(pond: string, immediateRetries: number, sourceRetries: number): Promise<void> {
  return postJSON(pondPath(pond, 'budget'), {
    immediate_retries: immediateRetries,
    source_retries: sourceRetries,
  });
}

// Compute override (Duck target/size + Flock posture). Only the fields passed change; clear=true drops
// the whole override (reverts to the pond.toml-declared config, else the Catchment default).
export interface DuckOverrideBody {
  duck_target?: string | null;
  dedicated_instance_type?: string | null;
  dedicated_auto_stop?: boolean | null;
  flock_mode?: string | null;
  flock_engine?: string | null;
  oom_policy?: string | null;
  clear?: boolean;
}

export function setDuck(pond: string, body: DuckOverrideBody): Promise<void> {
  return postJSON(pondPath(pond, 'duck'), body);
}

// ─── Cloud config (Catchment-level: the data-plane target + Duck Pools) ───────

export interface CloudSettings extends CloudGate {
  has_data: boolean; // once true the data root is set-once (switching would strand data)
}

export function fetchCloudSettings(): Promise<CloudSettings> {
  return getJSON<CloudSettings>('/catchment/settings');
}

// Attach the data-plane target (set-once in practice). Returns the updated gate, or throws the 409/422
// detail (already has data / already configured / unusable root).
export async function setDataRoot(dataRoot: string): Promise<CloudSettings> {
  const res = await fetch(`${apiBase()}/catchment/settings`, {
    method: 'PUT',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify({ data_root: dataRoot }),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `set data root failed (${res.status})`);
  return res.json();
}

export function fetchDuckPools(): Promise<DuckPool[]> {
  return getJSON<{ pools: DuckPool[] }>('/catchment/duck-pools').then((d) => d.pools);
}

export function addDuckPool(body: {
  name: string;
  provider?: string | null;
  cpu?: number | null;
  memory?: number | null;
  instance_type?: string | null;
  min_instances?: number | null;
  max_instances?: number | null;
  keep_warm?: number | null;
  idle_timeout?: number | null;
  region?: string | null;
}): Promise<void> {
  return postJSON('/catchment/duck-pools', body);
}

export async function removeDuckPool(name: string): Promise<void> {
  const res = await fetch(`${apiBase()}/catchment/duck-pools/${encodeURIComponent(name)}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error(`remove pool failed (${res.status})`);
}

// ─── Spouts (egress) ─────────────────────────────────────────────────────────

export interface RawSpout {
  name: string;
  table: string | null;
  destination: string;
  mode: string;
  is_failed: boolean;
  is_killed: boolean;
  standing_wake: boolean;
  error: string | null;
}

// A Spout's node id is "{source}#{spout}@{major}" — split into the source pond id + the spout name.
export function spoutParts(spoutId: string): { source: string; name: string } {
  const at = spoutId.lastIndexOf('@');
  const major = at === -1 ? '' : spoutId.slice(at + 1);
  const body = at === -1 ? spoutId : spoutId.slice(0, at);
  const hash = body.indexOf('#');
  const sourceName = hash === -1 ? body : body.slice(0, hash);
  const name = hash === -1 ? '' : body.slice(hash + 1);
  return { source: major ? `${sourceName}@${major}` : sourceName, name };
}

export function fetchSpouts(sourceId: string): Promise<RawSpout[]> {
  return getJSON<{ spouts: RawSpout[] }>(pondPath(sourceId, 'spouts')).then((d) => d.spouts);
}

// Control a Spout's standing Wake (wake | force | sleep | kill | clear | resync). `spoutId` is the node id.
export function controlSpout(spoutId: string, action: string): Promise<void> {
  const { source, name } = spoutParts(spoutId);
  return postJSON(pondPath(source, `spouts/${encodeURIComponent(name)}/${action}`));
}

export function removeSpout(spoutId: string): Promise<void> {
  const { source, name } = spoutParts(spoutId);
  return postJSON(pondPath(source, `spouts/${encodeURIComponent(name)}/remove`));
}

// Probe a destination's connection/credentials before binding a Spout (the add form's "Test" button).
// Writes no data. Returns {ok} or {ok: false, error} — a connection problem is data, not an exception.
export async function testSpout(sourceId: string, destination: string): Promise<{ ok: boolean; error?: string }> {
  const res = await fetch(`${apiBase()}${pondPath(sourceId, 'spouts/test')}`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify({ destination }),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `test failed (${res.status})`);
  return res.json();
}

// Add a Spout on a source Pond. Surfaces the server's 422 detail (bad destination / PK gate) on error.
export async function addSpout(
  sourceId: string,
  body: { destination: string; name?: string | null; table?: string | null; mode?: string },
): Promise<{ name: string }> {
  const res = await fetch(`${apiBase()}${pondPath(sourceId, 'spouts')}`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify(body),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `add spout failed (${res.status})`);
  return res.json();
}

// ─── Secrets (the catchment-wide write-only store) ───────────────────────────

export interface SecretName {
  name: string;
  set_at: string | null;
}

export function fetchSecrets(): Promise<SecretName[]> {
  return getJSON<{ secrets: SecretName[] }>('/secrets').then((d) => d.secrets);
}

// Set/overwrite a secret. The value travels in the request (use an HTTPS Catchment); never read back.
// Surfaces the server's 422 detail (bad name) on error.
export function setSecret(name: string, value: string): Promise<void> {
  return postJSON('/secrets', { name, value });
}

export async function removeSecret(name: string): Promise<void> {
  const res = await fetch(`${apiBase()}/secrets/${encodeURIComponent(name)}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error(`remove secret failed (${res.status})`);
}

// ─── Data serving (the Catalog — plans/data-serving.md) ─────────────────────

export interface CatalogTable {
  table: string;
  exposed: boolean;
  source: string; // 'declared' | 'override' | 'hidden'
  major: number;
}

export interface CatalogPond {
  name: string;
  served_major: number;
  majors: number[];
  tables: CatalogTable[];
}

export interface Catalog {
  catchment: string | null;
  ponds: CatalogPond[];
  connect: Record<string, string>; // {pg: "host:port", flight: "host:port"} for configured wires
}

export function fetchCatalog(): Promise<Catalog> {
  return getJSON<Catalog>('/serve');
}

// Cross-pond hand-written SQL now runs through the paginated /query/page + /query/count surface (the data
// viewer's query mode) — the standalone /serve/query is the CLI/wire JSON path, not a frontend client.

export function promoteServe(pond: string, major: number): Promise<void> {
  return postJSON(`/ponds/${encodeURIComponent(pond)}/serve/promote`, { major });
}

export function exposeTable(pond: string, table: string, exposed: boolean | null, major?: number): Promise<void> {
  return postJSON(pondPath(pond, 'serve/expose') + (major != null ? `?major=${major}` : ''), { table, exposed });
}

// ─── Alerts (failure & freshness notification channels — full-gated) ─────────

export interface RawAlertChannel {
  name: string;
  destination: string;
  scope: string | null; // a pond name, or null for catchment-wide
  events: string; // CSV of kinds, or 'all'
  stale_ms: number | null;
  renotify_ms?: number | null; // re-notify interval while an episode persists; null = once per episode
  enabled: boolean;
  created_at: string | null;
}

export interface RawAlertDelivery {
  channel: string;
  kind: string;
  pond: string | null;
  severity: string;
  status: string; // pending | sent | failed
  attempts: number;
  error: string | null;
  created_at: string | null;
  sent_at: string | null;
}

// ─── Observed table-level lineage (plans/lineage.md Phase 1) ─────────────────

export interface RawLineageRipple {
  ripple: string;
  f: string; // the latest recorded run
  reads: { source: string | null; table: string }[]; // source null = an own table
  writes: string[];
}

export interface RawLineagePond {
  id: string;
  name: string;
  major: number;
  version: string;
  ripples: RawLineageRipple[];
}

export function fetchLineage(pond?: string, major?: number): Promise<RawLineagePond[]> {
  const params = new URLSearchParams();
  if (pond !== undefined) params.set('pond', pond);
  if (major !== undefined) params.set('major', String(major));
  const q = params.toString();
  return getJSON<{ ponds: RawLineagePond[] }>(`/lineage${q ? `?${q}` : ''}`).then((d) => d.ponds);
}

export function fetchAlerts(): Promise<RawAlertChannel[]> {
  return getJSON<{ channels: RawAlertChannel[] }>('/alerts').then((d) => d.channels);
}

// Create a channel. Surfaces the server's 422 detail (bad destination / event kind) on error.
export function addAlert(body: {
  name: string;
  destination: string;
  scope?: string | null;
  events?: string;
  stale_ms?: number | null;
  renotify_ms?: number | null;
}): Promise<void> {
  return postJSON('/alerts', body);
}

export async function removeAlert(name: string): Promise<void> {
  const res = await fetch(`${apiBase()}/alerts/${encodeURIComponent(name)}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error(`remove alert failed (${res.status})`);
}

// Send a test notification (validates connectivity/credentials). A connection problem is 200
// {ok: false, error} (a sanitised message), not an exception — mirrors testSpout.
export async function testAlert(name: string): Promise<{ ok: boolean; error?: string }> {
  const res = await fetch(`${apiBase()}/alerts/${encodeURIComponent(name)}/test`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: '{}',
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `test failed (${res.status})`);
  return res.json();
}

export function fetchDeliveries(limit = 50): Promise<RawAlertDelivery[]> {
  return getJSON<{ deliveries: RawAlertDelivery[] }>(`/alerts/deliveries?limit=${limit}`).then((d) => d.deliveries);
}

// ─── Data viewer (windowed read of a Pond's exported tables) ─────────────────

export interface PageResult {
  columns: string[];
  rows: unknown[][];
  has_more: boolean;
}

export type TrickleMode = 'append' | 'merge';

// A published table, with its Trickle mode (if any) and primary key.
export interface TableInfo {
  name: string;
  trickle: TrickleMode | null;
  pk: string[];
}

// A query against a Pond's exported data: a named `table` (browse), a custom `sql`, or — for a Trickle
// — a server-built windowed/consolidated view (`trickle` + `pk` + the freshness window `fLo`..`fHi`).
export interface DataQuery {
  pond: string; // pond id ("name@major")
  table?: string;
  sql?: string;
  trickle?: TrickleMode;
  pk?: string[];
  fLo?: string | null; // inclusive lower freshness bound (ISO), null = unbounded
  fHi?: string | null; // inclusive upper freshness bound (ISO), null = unbounded
  orderBy?: string | null; // opt-in sort column (null = base order); only affects /page
  orderDesc?: boolean;
}

// Split a pond id ("name@major") into the name + major the data routes expect.
function splitPond(pond: string): { name: string; major: number | undefined } {
  const at = pond.lastIndexOf('@');
  return { name: at === -1 ? pond : pond.slice(0, at), major: at === -1 ? undefined : Number(pond.slice(at + 1)) };
}

async function postData<T>(path: string, body: object): Promise<T> {
  const res = await fetch(`${apiBase()}${path}`, {
    method: 'POST',
    headers: authHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify(body),
  });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `${path} → ${res.status}`);
  return res.json() as Promise<T>;
}

// The query body shared by /query/count and /query/page: maps the client DataQuery onto the backend's
// snake_case fields, adding the pond name + major.
function queryBody(q: DataQuery): object {
  const { name, major } = splitPond(q.pond);
  return { pond: name, major, table: q.table, sql: q.sql, trickle: q.trickle, pk: q.pk, f_lo: q.fLo, f_hi: q.fHi };
}

// The tables this Pond's major line has published — the viewer's table picker (with Trickle mode + pk).
export async function fetchTables(pond: string): Promise<TableInfo[]> {
  const { name, major } = splitPond(pond);
  const qs = major === undefined ? '' : `?major=${major}`;
  return getJSON<{ tables: TableInfo[] }>(`/ponds/${encodeURIComponent(name)}/tables${qs}`).then((d) => d.tables);
}

// A published non-tabular Object (an ML model, a serialised blob) — see plans/objects.md.
export interface ObjectInfo {
  name: string;
  size: number | null; // total byte size (payload sum for a directory Object)
  f: string | null; // the run freshness that produced it (ISO)
  is_dir: boolean; // a directory Object (published/downloaded as a unit) vs a single file
  ext: string; // the single-file Object's extension (e.g. ".pkl"); "" for a dir Object or an extension-less blob
}

// The non-tabular Objects this Pond's major line has published — the viewer's Objects list.
export async function fetchObjects(pond: string): Promise<ObjectInfo[]> {
  const { name, major } = splitPond(pond);
  const qs = major === undefined ? '' : `?major=${major}`;
  return getJSON<{ objects: ObjectInfo[] }>(`/ponds/${encodeURIComponent(name)}/objects${qs}`).then((d) => d.objects);
}

// Download one Object (a single file, or a directory Object as a zip). Fetched with auth then handed to the
// browser as a blob (a plain link wouldn't carry the auth header the Catchment may require).
export async function downloadObject(pond: string, obj: ObjectInfo): Promise<void> {
  const { name, major } = splitPond(pond);
  const qs = major === undefined ? '' : `?major=${major}`;
  const url = `${apiBase()}/ponds/${encodeURIComponent(name)}/objects/${encodeURIComponent(obj.name)}${qs}`;
  const res = await fetch(url, { headers: authHeaders() });
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error(`download failed: ${res.status}`);
  const blob = await res.blob();
  const href = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = href;
  a.download = obj.is_dir ? `${obj.name}.zip` : `${obj.name}${obj.ext}`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(href);
}

// Delete a table (its data + registry state); the Catchment forces a rebuild run. Full access only.
export async function deleteTable(pond: string, table: string): Promise<void> {
  const { name, major } = splitPond(pond);
  const qs = major === undefined ? '' : `?major=${major}`;
  const res = await fetch(
    `${apiBase()}/ponds/${encodeURIComponent(name)}/tables/${encodeURIComponent(table)}${qs}`,
    { method: 'DELETE', headers: authHeaders() },
  );
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `delete failed: ${res.status}`);
}

// Delete a published Object. Requires the Pond to be idle (409 otherwise). Full access only.
export async function deleteObject(pond: string, name: string): Promise<void> {
  const { name: pn, major } = splitPond(pond);
  const qs = major === undefined ? '' : `?major=${major}`;
  const res = await fetch(
    `${apiBase()}/ponds/${encodeURIComponent(pn)}/objects/${encodeURIComponent(name)}${qs}`,
    { method: 'DELETE', headers: authHeaders() },
  );
  if (res.status === 401) throw new UnauthorizedError();
  if (!res.ok) throw new Error((await res.json().catch(() => null))?.detail ?? `delete failed: ${res.status}`);
}

// The distinct run freshnesses (newest-first) of a Trickle table — the window selector's options.
export async function fetchFreshness(pond: string, table: string): Promise<{ freshness: string[]; floor: string | null }> {
  const { name, major } = splitPond(pond);
  const params = new URLSearchParams({ table });
  if (major !== undefined) params.set('major', String(major));
  return getJSON(`/ponds/${encodeURIComponent(name)}/freshness?${params}`);
}

// The full changelog history of one record (merge Trickle), for the per-row history view.
export async function fetchHistory(pond: string, table: string, pk: Record<string, unknown>): Promise<PageResult> {
  const { name, major } = splitPond(pond);
  return postData<PageResult>('/query/history', { pond: name, major, table, pk });
}

// Total rows of a query — sizes the viewer's virtual scroll.
export async function fetchCount(q: DataQuery): Promise<number> {
  return postData<{ count: number }>('/query/count', queryBody(q)).then((d) => d.count);
}

// A windowed read [offset, offset+limit) for the virtual grid. The server wraps the query in a subquery
// with LIMIT/OFFSET; a static Parquet scan is deterministic, so windows are stable.
export async function fetchPage(q: DataQuery & { limit: number; offset: number }): Promise<PageResult> {
  return postData<PageResult>('/query/page', {
    ...queryBody(q), order_by: q.orderBy, order_desc: q.orderDesc, limit: q.limit, offset: q.offset,
  });
}

export function fetchWindows(pond: string): Promise<RawWindow[]> {
  return getJSON<{ windows: RawWindow[] }>(pondPath(pond, 'windows')).then((d) => d.windows);
}

export interface AddWindowBody {
  name: string;
  start_anchor: string;
  duration_seconds: number;
  freq_unit: FreqUnit;
  freq_interval: number;
  valid_days: string | null;
  until_time: string | null;
}

export function addWindow(pond: string, body: AddWindowBody): Promise<void> {
  return postJSON(pondPath(pond, 'windows'), body);
}

export function removeWindow(pond: string, name: string): Promise<void> {
  return postJSON(pondPath(pond, `windows/${encodeURIComponent(name)}/remove`));
}
