import { create } from 'zustand';
import type {
  PondId,
  RippleId,
  Pond,
  Ripple,
  NodeView,
  TriggerView,
  PondInfo,
  ViewPayload,
  PondRun,
  WindowRow,
} from './types';
import {
  fetchStatus,
  fetchView,
  fetchRuns,
  fetchWindows,
  postTrigger,
  refreshPond,
  batchPonds,
  BATCH_OPS,
  type BatchOp,
  clearFailure,
  setBudget,
  setDuck,
  addWindow,
  removeWindow,
  controlSpout,
  addSpout as apiAddSpout,
  removeSpout as apiRemoveSpout,
  setApiKey,
  UnauthorizedError,
  type AccessLevel,
  type CloudGate,
  type DuckOverrideBody,
  type StatusPayload,
  type RawPond,
  type RawRipple,
  type RawPondRun,
  type RawWindow,
  type AddWindowBody,
} from './api';

// ─── Payload → view-model transforms ─────────────────────────────────────────

// Parse a backend timestamp to ms. Most are tz-aware ISO, but SQLite `datetime('now')` defaults are
// naive UTC ("YYYY-MM-DD HH:MM:SS"); a bare Date.parse would read those as *local* time. Normalise:
// space→T and append 'Z' when no offset is present, so everything is interpreted as UTC.
export function parseTs(ts: string | null): number {
  if (!ts) return 0;
  const s = ts.includes('T') ? ts : ts.replace(' ', 'T');
  const hasTz = /([zZ]|[+-]\d{2}:?\d{2})$/.test(s);
  return Date.parse(hasTz ? s : `${s}Z`);
}

const isoToMs = (iso: string | null): number => parseTs(iso);
const isoToMsOrNull = (iso: string | null): number | null => (iso ? parseTs(iso) : null);

function nodeView(n: RawPond | RawRipple, dMs: number): NodeView {
  return {
    status: n.status,
    startF: isoToMs(n.start_f),
    endF: isoToMs(n.end_f),
    targetF: isoToMsOrNull(n.target_f),
    hasPull: n.has_pull,
    runsStarted: n.gen,
    runsCompleted: n.runs_completed,
    dMs,
  };
}

function mapRun(r: RawPondRun): PondRun {
  return {
    pond: r.pond,
    id: r.id,
    major: r.major,
    version: r.version,
    f: r.f,
    startedAt: r.started_at,
    finishedAt: r.finished_at,
    status: r.status,
    error: r.error,
    traceback: r.traceback,
    ripples: r.ripples?.map((rr) => ({
      ripple: rr.ripple,
      startedAt: rr.started_at,
      finishedAt: rr.finished_at,
      status: rr.status,
      retry: rr.retry,
      error: rr.error,
      traceback: rr.traceback,
    })),
  };
}

function mapWindow(w: RawWindow): WindowRow {
  return {
    name: w.name,
    startAnchor: w.start_anchor,
    durationSeconds: w.duration_seconds,
    freqUnit: w.freq_unit,
    freqInterval: w.freq_interval,
    validDays: w.valid_days,
    untilTime: w.until_time,
  };
}

const LEVEL_RANK: Record<AccessLevel, number> = { read: 1, demand: 2, full: 3 };

/** Whether `level` meets `required` on the read ⊂ demand ⊂ full ladder. */
export function atLeast(level: AccessLevel, required: AccessLevel): boolean {
  return LEVEL_RANK[level] >= LEVEL_RANK[required];
}

interface StatusSlice {
  catchment: { id: string | null; name: string | null } | null;
  accessLevel: AccessLevel;
  cloud: CloudGate | null; // the cloud-enable gate (remote data root + AWS creds); null until first /status
  ponds: Record<PondId, Pond>;
  ripples: Record<RippleId, Ripple>;
  pondViews: Record<PondId, NodeView>;
  rippleViews: Record<RippleId, NodeView>;
  pondInfo: Record<PondId, PondInfo>;
  triggers: Record<PondId, TriggerView>;
}

function transformStatus(payload: StatusPayload): StatusSlice {
  const ponds: Record<PondId, Pond> = {};
  const ripples: Record<RippleId, Ripple> = {};
  const pondViews: Record<PondId, NodeView> = {};
  const rippleViews: Record<RippleId, NodeView> = {};
  const pondInfo: Record<PondId, PondInfo> = {};
  const triggers: Record<PondId, TriggerView> = {};

  // A Pond Draw only earns a node if a local Pond actually sources from it — an unconsumed Draw (e.g.
  // from `duct create --sync` drawing more than this Catchment uses) is noise, so hide it.
  const consumed = new Set<PondId>(payload.edges.map(([src]) => src));

  for (const p of payload.ponds) {
    if (p.is_draw && !consumed.has(p.id)) continue;
    ponds[p.id] = {
      id: p.id, name: p.name, kind: p.kind,
      isDraw: p.is_draw ?? false, isSpout: p.is_spout ?? false, sources: [],
    };
    pondInfo[p.id] = {
      version: p.version,
      major: p.major,
      kind: p.kind,
      hasTables: p.has_tables ?? false,
      hasObjects: p.has_objects ?? false,
      isFailed: p.is_failed,
      isBlocked: p.is_blocked,
      blockedReason: p.blocked_reason ?? null,
      isKilled: p.is_killed,
      refreshPending: p.refresh_pending ?? false,
      repairing: p.repairing ?? false,
      failedF: p.failed_f,
      failures: p.failures,
      missingSources: p.missing_sources ?? [],
      blockedBy: p.blocked_by ?? [],
      error: p.error,
      failureKind: p.failure_kind ?? null,
      immediateRetries: p.immediate_retries,
      sourceRetries: p.source_retries,
      duck: p.duck ?? null,
      spout: p.spout ?? null,
    };
    pondViews[p.id] = nodeView(p, p.d_ms);
    if (p.trigger) triggers[p.id] = { kind: p.trigger.kind, boundMs: p.trigger.bound_ms };

    // A Draw's "draw" ripple and a Spout's "egress" ripple are internal mechanisms — not worth rendering.
    // Skip them; the node shows bare (its running/idle/failed state is pond-level).
    if (!p.is_draw && !p.is_spout) {
      for (const r of p.ripples) {
        const eid = `${p.id}.${r.name}`;
        ripples[eid] = { id: eid, pondId: p.id, name: r.name, parents: [] };
        rippleViews[eid] = nodeView(r, 0);
      }
      // ripple_edges are [sourceName, sinkName] within the Pond → sink.parents includes source.
      for (const [src, snk] of p.ripple_edges) {
        ripples[`${p.id}.${snk}`]?.parents.push(`${p.id}.${src}`);
      }
    }
  }
  // Pond sources from inter-Pond edges [sourceId, sinkId] (pond keys).
  for (const [src, snk] of payload.edges) {
    ponds[snk]?.sources.push(src);
  }

  return {
    catchment: payload.catchment ?? null,
    // Default to 'full' when absent — keeps an open/unauthed Catchment (and any transitional backend)
    // showing every control, matching pre-ladder behaviour.
    accessLevel: payload.access_level ?? 'full',
    cloud: payload.cloud ?? null,
    ponds, ripples, pondViews, rippleViews, pondInfo, triggers,
  };
}

// ─── Store ───────────────────────────────────────────────────────────────────

export interface RunFilters {
  lineage: boolean; // include the selected Pond's upstream sources (default on)
  ripples: boolean; // nest Ripple Runs under each Pond Run (default off)
}

export interface LiveState extends StatusSlice {
  now: number;
  connected: boolean;
  error: string | null;
  lineage: ViewPayload | null; // upstream Catchments + duct edges, for the lineage overlay
  statusVersion: number | null; // last seen engine-state version; drives the /api/status long-poll
  needsKey: boolean; // the Catchment answered 401 — show the API-key prompt

  selectedPondId: PondId | null;
  selectedRippleId: RippleId | null;
  selectedTriggerId: PondId | null;

  // Collapsed Ponds hide their Ripples/Trickles in the canvas (header-only). Purely client-side view
  // state — keyed by pond id, `true` = collapsed.
  collapsedPonds: Record<PondId, boolean>;
  toggleCollapse(id: PondId): void;
  setAllCollapsed(collapsed: boolean): void;

  // The Pond whose exported tables are open in the full-screen data viewer (null = closed). The modal
  // owns its own table-selection / SQL / windowing state; the store just tracks the target.
  dataViewerPondId: PondId | null;
  openDataViewer(id: PondId): void;
  closeDataViewer(): void;

  runs: PondRun[]; // run-history feed (filtered by selection + filters)
  selectedRun: PondRun | null; // the run open in the detail pane (enriched with ripples on select)
  runFilters: RunFilters;
  runLimit: number; // size of the live window the feed fetches; grows as the user scrolls
  runsAtEnd: boolean; // the window reached the oldest available run (fewer rows than asked)
  loadingMore: boolean; // a scroll-triggered window growth is in flight
  selectedPondRuns: PondRun[]; // the selected Pond's own runs (with ripples), for the trace charts
  windowsByPond: Record<PondId, WindowRow[]>;

  refresh(): Promise<void>;
  submitApiKey(key: string): Promise<void>;
  refreshWindows(pond: PondId): Promise<void>;
  refreshRunDetail(): Promise<void>;
  setRunFilter(key: keyof RunFilters, value: boolean): void;
  loadMoreRuns(): void;
  selectRun(run: PondRun | null): void;

  selectPond(id: PondId | null): void;
  selectRipple(id: RippleId | null): void;
  selectTrigger(id: PondId | null): void;
  clearSelection(): void;

  tap(pond: PondId): Promise<void>;
  pulse(pond: PondId): Promise<void>;
  wave(pond: PondId): Promise<void>;
  tide(pond: PondId, boundSeconds: number): Promise<void>;
  removeTrigger(pond: PondId): Promise<void>;

  wake(pond: PondId): Promise<void>;
  sleep(pond: PondId, upstream?: boolean): Promise<void>;
  force(pond: PondId): Promise<void>;
  kill(pond: PondId): Promise<void>;
  refreshPond(pond: PondId, clear?: boolean): Promise<void>;
  resetPond(pond: PondId): Promise<void>;

  // Selector: a canvas selection mode that applies a set of operations to a set of Ponds (Pond Actions).
  // Phase 'select' picks the Ponds on the canvas; phase 'ops' picks the operations to apply.
  selectorMode: boolean;
  selectorPhase: 'select' | 'ops';
  selectorScope: PondId[];
  selectorOps: BatchOp[];
  selectorError: string | null;
  enterSelector(): void;
  exitSelector(): void;
  toggleSelect(id: PondId): void;
  selectAll(): void;
  selectTree(): void;
  selectBetween(): void;
  addDownstream(): void;
  clearSelectorScope(): void;
  setSelectorPhase(phase: 'select' | 'ops'): void;
  toggleOp(op: BatchOp): void;
  submitBatch(confirm: string | null): Promise<void>;

  clearFailure(pond: PondId): Promise<void>;
  setBudget(pond: PondId, immediateRetries: number, sourceRetries: number): Promise<void>;
  setDuck(pond: PondId, body: DuckOverrideBody): Promise<void>;

  addWindow(pond: PondId, body: AddWindowBody): Promise<void>;
  removeWindow(pond: PondId, name: string): Promise<void>;

  spoutControl(spoutId: PondId, action: string): Promise<void>;
  removeSpout(spoutId: PondId): Promise<void>;
  addSpout(
    sourceId: PondId,
    body: { destination: string; name?: string | null; table?: string | null; mode?: string },
  ): Promise<{ name: string }>;
}

// Run-history feed window: starts at one page, grows by a page per scroll-to-bottom, hard-capped
// (matches the backend /api/runs clamp). The poll always fetches the current window, so payloads
// stay small until the user scrolls deep.
const RUN_PAGE = 100;
const RUN_MAX = 1000;

// The Pond a run-history / chart query should focus on: an explicitly selected Pond, or the Pond
// owning a selected Ripple.
function focusPond(s: LiveState): PondId | null {
  if (s.selectedPondId) return s.selectedPondId;
  if (s.selectedRippleId) return s.ripples[s.selectedRippleId]?.pondId ?? null;
  return null;
}

// Identity of a Pond Run (newest history can re-fetch the same run to keep the detail pane live).
export function runKey(r: PondRun): string {
  return `${r.id}::${r.version}::${r.f}`;
}

// The pond topology as adjacency lists, for the Selector's expansion helpers (children = ponds that
// list me as a source; parents = my sources).
function graphOf(ponds: Record<PondId, Pond>): { children: Record<string, string[]>; parents: Record<string, string[]> } {
  const children: Record<string, string[]> = {};
  const parents: Record<string, string[]> = {};
  for (const p of Object.values(ponds)) {
    parents[p.id] = [...p.sources];
    for (const src of p.sources) (children[src] ??= []).push(p.id);
  }
  return { children, parents };
}

// The reachable-set closure from `seeds` over adjacency `adj`, including the seeds themselves.
function closure(seeds: PondId[], adj: Record<string, string[]>): Set<string> {
  const out = new Set<string>(seeds);
  const stack = [...seeds];
  while (stack.length) {
    for (const nxt of adj[stack.pop()!] ?? []) if (!out.has(nxt)) { out.add(nxt); stack.push(nxt); }
  }
  return out;
}

export const useLiveStore = create<LiveState>((set, get) => ({
  catchment: null,
  accessLevel: 'full', // until the first /api/status; open Catchments stay 'full'
  cloud: null,
  ponds: {},
  ripples: {},
  pondViews: {},
  rippleViews: {},
  pondInfo: {},
  triggers: {},
  now: Date.now(),
  connected: false,
  error: null,
  needsKey: false,

  selectedPondId: null,
  selectedRippleId: null,
  selectedTriggerId: null,
  collapsedPonds: {},
  dataViewerPondId: null,

  runs: [],
  selectedRun: null,
  runFilters: { lineage: true, ripples: false },
  runLimit: RUN_PAGE,
  runsAtEnd: false,
  loadingMore: false,
  selectedPondRuns: [],
  windowsByPond: {},
  lineage: null,
  statusVersion: null,

  async refresh() {
    let payload;
    try {
      // Long-poll: holds until the engine state moves past the version we last saw (or a heartbeat),
      // so this resolves the instant anything changes rather than on a fixed timer.
      payload = await fetchStatus(get().statusVersion ?? undefined);
    } catch (e) {
      if (e instanceof UnauthorizedError) {
        set({ connected: false, needsKey: true, error: null });
      } else {
        set({ connected: false, error: e instanceof Error ? e.message : String(e) });
      }
      return; // if the Catchment is unreachable, skip the dependent fetches this tick
    }

    // The state just changed → fetch the lineage fresh (after the gate, not concurrently, so it
    // reflects the post-change state). Non-critical: keep the last good one on failure.
    let lineage = get().lineage;
    try {
      lineage = await fetchView();
    } catch {
      /* leave the last lineage in place */
    }
    const next = transformStatus(payload);
    // Drop a selection whose Pond has vanished (e.g. it was just removed) so the Sidebar doesn't hold a
    // ghost and the canvas re-frames to what's left (see the fit-all fallback in DagCanvas).
    const sel = get().selectedPondId;
    const cleared = sel && !next.ponds[sel]
      ? { selectedPondId: null, selectedRippleId: null, selectedTriggerId: null }
      : {};
    set({
      ...next, ...cleared, lineage, statusVersion: payload.version,
      now: Date.now(), connected: true, error: null, needsKey: false,
    });

    const s = get();
    const pond = focusPond(s);
    const limit = s.runLimit;
    try {
      const [feed, ownRuns] = await Promise.all([
        fetchRuns({ pond, lineage: s.runFilters.lineage, ripples: s.runFilters.ripples, limit }),
        pond ? fetchRuns({ pond, lineage: false, ripples: true, limit: 60 }) : Promise.resolve([]),
      ]);
      // Fewer rows than the window means we've reached the oldest run (also true once at the cap).
      set({
        runs: feed.map(mapRun),
        selectedPondRuns: ownRuns.map(mapRun),
        runsAtEnd: feed.length < limit || limit >= RUN_MAX,
      });
    } catch {
      /* history is non-critical; leave the last good feed in place */
    }
    if (get().selectedRun) void get().refreshRunDetail(); // keep the open detail live as it runs/retries
  },

  async submitApiKey(key: string) {
    setApiKey(key.trim() || null);
    await get().refresh(); // a wrong key just re-raises 401 → the prompt stays up
  },

  async refreshRunDetail() {
    const sel = get().selectedRun;
    if (!sel) return;
    try {
      // The feed may not carry ripples; fetch this run's own Pond history (with ripples) and match it.
      const rows = await fetchRuns({ pond: sel.id, lineage: false, ripples: true, limit: 200 });
      const found = rows.map(mapRun).find((r) => runKey(r) === runKey(sel));
      const still = get().selectedRun;
      if (found && still && runKey(still) === runKey(found)) set({ selectedRun: found });
    } catch {
      /* detail is non-critical */
    }
  },

  async refreshWindows(pond) {
    try {
      const windows = await fetchWindows(pond);
      set((s) => ({ windowsByPond: { ...s.windowsByPond, [pond]: windows.map(mapWindow) } }));
    } catch {
      /* ignore */
    }
  },

  setRunFilter(key, value) {
    // Changing the feed resets the scroll window to the first page.
    set((s) => ({ runFilters: { ...s.runFilters, [key]: value }, runLimit: RUN_PAGE, runsAtEnd: false }));
    get().refresh();
  },

  loadMoreRuns() {
    const s = get();
    if (s.loadingMore || s.runsAtEnd || s.runLimit >= RUN_MAX) return;
    set({ loadingMore: true, runLimit: Math.min(s.runLimit + RUN_PAGE, RUN_MAX) });
    get().refresh().finally(() => set({ loadingMore: false }));
  },

  selectRun(run) {
    set({ selectedRun: run });
    if (run) void get().refreshRunDetail();
  },

  selectPond(id) {
    set({ selectedPondId: id, selectedRippleId: null, selectedTriggerId: null, selectedPondRuns: [], runLimit: RUN_PAGE, runsAtEnd: false });
    // Windows live on Inlets (batch availability) and on Spouts (delivery throttle).
    if (id && (get().ponds[id]?.sources.length === 0 || get().ponds[id]?.isSpout)) get().refreshWindows(id);
    get().refresh();
  },
  selectRipple(id) {
    const pondId = id ? get().ripples[id]?.pondId ?? null : null;
    set({ selectedRippleId: id, selectedPondId: pondId, selectedTriggerId: null, selectedPondRuns: [], runLimit: RUN_PAGE, runsAtEnd: false });
    get().refresh();
  },
  selectTrigger(id) {
    set({ selectedTriggerId: id, selectedPondId: null, selectedRippleId: null });
  },
  clearSelection() {
    set({ selectedPondId: null, selectedRippleId: null, selectedTriggerId: null, selectedPondRuns: [], runLimit: RUN_PAGE, runsAtEnd: false });
  },

  toggleCollapse(id) {
    set((s) => ({ collapsedPonds: { ...s.collapsedPonds, [id]: !s.collapsedPonds[id] } }));
  },
  openDataViewer(id) {
    set({ dataViewerPondId: id });
  },
  closeDataViewer() {
    set({ dataViewerPondId: null });
  },
  setAllCollapsed(collapsed) {
    // Only Ponds that actually own Ripples can collapse — a Draw (ripple-less) has nothing to hide.
    const next: Record<PondId, boolean> = {};
    if (collapsed) {
      for (const r of Object.values(get().ripples)) next[r.pondId] = true;
    }
    set({ collapsedPonds: next });
  },

  tap: (pond) => act(get, set, () => postTrigger(pond, 'tap')),
  pulse: (pond) => act(get, set, () => postTrigger(pond, 'pulse')),
  wave: (pond) => act(get, set, () => postTrigger(pond, 'wave')),
  tide: (pond, boundSeconds) => act(get, set, () => postTrigger(pond, 'tide', { bound_seconds: boundSeconds })),
  removeTrigger: (pond) => act(get, set, () => postTrigger(pond, 'untrigger')),

  wake: (pond) => act(get, set, () => postTrigger(pond, 'wake')),
  sleep: (pond, upstream = false) => act(get, set, () => postTrigger(pond, 'sleep', { upstream })),
  force: (pond) => act(get, set, () => postTrigger(pond, 'force')),
  kill: (pond) => act(get, set, () => postTrigger(pond, 'kill')),
  refreshPond: (pond, clear = false) => act(get, set, () => refreshPond(pond, clear)),
  resetPond: (pond) => act(get, set, () => postTrigger(pond, 'reset')),

  selectorMode: false,
  selectorPhase: 'select',
  selectorScope: [],
  selectorOps: [],
  selectorError: null,
  enterSelector: () => set({ selectorMode: true, selectorPhase: 'select', selectorScope: [], selectorOps: [], selectorError: null }),
  exitSelector: () => set({ selectorMode: false, selectorPhase: 'select', selectorScope: [], selectorOps: [], selectorError: null }),
  toggleSelect: (id) =>
    set((s) => ({
      selectorError: null,
      selectorScope: s.selectorScope.includes(id)
        ? s.selectorScope.filter((x) => x !== id)
        : [...s.selectorScope, id],
    })),
  // All targetable Ponds (Spouts/Draws have no local output — excluded from bulk ops).
  selectAll: () =>
    set((s) => ({
      selectorScope: Object.values(s.ponds).filter((p) => !p.isSpout && !p.isDraw).map((p) => p.id),
      selectorError: null,
    })),
  addDownstream: () =>
    set((s) => {
      const { children } = graphOf(s.ponds);
      return { selectorScope: [...closure(s.selectorScope, children)], selectorError: null };
    }),
  // Weakly-connected component(s) of the selection: undirected reachability over source edges.
  selectTree: () =>
    set((s) => {
      const { children, parents } = graphOf(s.ponds);
      const undirected: Record<string, string[]> = {};
      for (const id of Object.keys(s.ponds)) undirected[id] = [...(children[id] ?? []), ...(parents[id] ?? [])];
      return { selectorScope: [...closure(s.selectorScope, undirected)], selectorError: null };
    }),
  // Ponds on a directed path between two selected Ponds: descendants(S) ∩ ancestors(S) (S included).
  selectBetween: () =>
    set((s) => {
      const { children, parents } = graphOf(s.ponds);
      const desc = closure(s.selectorScope, children);
      const anc = closure(s.selectorScope, parents);
      return { selectorScope: [...desc].filter((id) => anc.has(id)), selectorError: null };
    }),
  clearSelectorScope: () => set({ selectorScope: [], selectorError: null }),
  setSelectorPhase: (phase) => set({ selectorPhase: phase, selectorError: null }),
  toggleOp: (op) =>
    set((s) => {
      let ops = s.selectorOps.includes(op) ? s.selectorOps.filter((o) => o !== op) : [...s.selectorOps, op];
      // Constraints: Repair is exclusive with Remove/Reset; Remove implies (and locks on) Reset.
      if (ops.includes('repair')) ops = ops.filter((o) => o !== 'remove' && o !== 'reset');
      if (ops.includes('remove')) {
        ops = ops.filter((o) => o !== 'repair');
        if (!ops.includes('reset')) ops.push('reset');
      }
      return { selectorOps: ops, selectorError: null };
    }),
  submitBatch: async (confirm) => {
    const { selectorScope, selectorOps } = get();
    if (selectorScope.length === 0 || selectorOps.length === 0) return;
    const ordered = BATCH_OPS.filter((o) => selectorOps.includes(o));
    try {
      await batchPonds(selectorScope, ordered, confirm);
      set({ selectorMode: false, selectorPhase: 'select', selectorScope: [], selectorOps: [], selectorError: null });
      await get().refresh();
    } catch (e) {
      set({ selectorError: e instanceof Error ? e.message : 'operation failed' });
    }
  },

  clearFailure: (pond) => act(get, set, () => clearFailure(pond)),
  setBudget: (pond, immediateRetries, sourceRetries) =>
    act(get, set, () => setBudget(pond, immediateRetries, sourceRetries)),
  setDuck: (pond, body) => act(get, set, () => setDuck(pond, body)),

  // Spout control (the standing Wake) — wake/force/sleep/kill/clear/resync on a Spout node id.
  spoutControl: (spoutId, action) => act(get, set, () => controlSpout(spoutId, action)),
  removeSpout: (spoutId) => act(get, set, () => apiRemoveSpout(spoutId)),
  // Add returns the created name (or throws the server detail) so the form can show inline errors;
  // re-polls on success.
  addSpout: async (sourceId, body) => {
    const res = await apiAddSpout(sourceId, body);
    set({ error: null });
    await get().refresh();
    return res;
  },

  addWindow: (pond, body) =>
    act(get, set, async () => {
      await addWindow(pond, body);
      await get().refreshWindows(pond);
    }),
  removeWindow: (pond, name) =>
    act(get, set, async () => {
      await removeWindow(pond, name);
      await get().refreshWindows(pond);
    }),
}));

// Run a control action, surface any error, and immediately re-poll so the UI reflects the result
// without waiting for the next tick.
async function act(
  get: () => LiveState,
  set: (partial: Partial<LiveState>) => void,
  fn: () => Promise<void>,
): Promise<void> {
  try {
    await fn();
    set({ error: null });
    await get().refresh();
  } catch (e) {
    if (e instanceof UnauthorizedError) set({ needsKey: true });
    else set({ error: e instanceof Error ? e.message : String(e) });
  }
}

// ─── Visual helpers (shared by the node components) ──────────────────────────

// Age of a freshness timestamp relative to now, unit-scaled. F=0 (NEVER) → '—'. Two-digit numeric
// part so widths don't jitter as a value crosses 9↔10.
export function formatAge(F: number, now: number): string {
  if (!F) return '—';
  const pad = (n: number) => String(Math.floor(n)).padStart(2, '0');
  const secs = Math.max(0, (now - F) / 1000);
  if (secs < 60) return `${pad(secs)}s`;
  const mins = secs / 60;
  if (mins < 60) return `${pad(mins)}m`;
  const hrs = mins / 60;
  if (hrs < 24) return `${pad(hrs)}h`;
  const days = hrs / 24;
  if (days < 30) return `${pad(days)}d`;
  const months = days / 30;
  if (months < 12) return `${pad(months)}mo`;
  return '>1y';
}

// A staleness bound (ms) as a compact duration in its largest whole unit: 86_400_000 → "1d",
// 5_400_000 → "1.5h", 2_000 → "2s".
export function formatDuration(ms: number): string {
  const s = ms / 1000;
  for (const [label, unit] of [['w', 604800], ['d', 86400], ['h', 3600], ['m', 60], ['s', 1]] as const) {
    if (s >= unit) {
      const v = s / unit;
      return Number.isInteger(v) ? `${v}${label}` : `${v.toFixed(1)}${label}`;
    }
  }
  return `${s}s`;
}

// ─── Semantic palette ────────────────────────────────────────────────────────
// Named by role, not hue, so the values can move without the names lying. pull / push / running form
// a roughly-even triad (they are routinely co-visible and must mutually contrast); success/danger are
// the fixed green/red conventions. THEME_BRAND (the Duckstring colour) IS the running colour and also
// the generic accent.
export const THEME_BRAND = '#06c4e6'; // a pond on a bright day — full-saturation water cyan
export const THEME_RUNNING = THEME_BRAND; // active execution + brand accent
export const THEME_PULL = '#ee9333'; // pull (Tap/Wave) · queued · run interval — warm amber-orange
export const THEME_PUSH = '#a3e635'; // push (Pulse/Tide) — green-yellow
export const THEME_SUCCESS = '#22c55e'; // success · connected · start · clear (green)
export const THEME_DANGER = '#ef4444'; // stop · failed (red)
export const THEME_BLOCKED = '#991b1b'; // blocked by an upstream failure — a darker, muted red
export const THEME_WAKE = '#15803d'; // Wake — a muted/dark green (the soft "go", vs Force's bright green)

// Node/Ripple demand state → border colour. Failed/killed/blocked (Ponds) take precedence in the
// backend's status string, so they map straight through here.
export const STATE_COLORS: Record<string, string> = {
  running: THEME_RUNNING,
  queued: THEME_PULL,
  idle: '#71717a',
  failed: THEME_DANGER,
  killed: THEME_DANGER, // killed reads as red, like failed
  blocked: THEME_BLOCKED,
  repairing: THEME_WAKE, // mid-rebuild — a deliberate operator action, the soft "go" green
};

// Border colour for a node. Running is always the brand cyan and idle is grey; a *queued* node is
// coloured by its demand — push if it holds any push target, else pull. (Push is the more specific
// demand, so it wins the colour when a node holds both.)
export function stateColor(view: NodeView): string {
  if (view.status === 'queued') {
    if (view.targetF !== null) return THEME_PUSH;
    if (view.hasPull) return THEME_PULL;
  }
  return STATE_COLORS[view.status] ?? STATE_COLORS.idle;
}

// pull amber; push green-yellow; idle grey.
export const EDGE_COLORS: Record<string, string> = {
  pull: THEME_PULL,
  push: THEME_PUSH,
  idle: '#3f3f46',
};

// Edge colour = whether the child can consume the parent. Coloured when the parent's output is
// fresher than the child's last run start (parent.endF > child.startF); push if consuming it would
// also meet a push target the child holds (parent.endF >= child.targetF), else pull. Freshnesses are
// ms-epoch (0 = never); childTargetF is null when there is no push target.
export function consumeEdgeColor(parentEndF: number, childStartF: number, childTargetF: number | null): string {
  if (parentEndF <= childStartF) return EDGE_COLORS.idle;
  if (childTargetF !== null && parentEndF >= childTargetF) return EDGE_COLORS.push;
  return EDGE_COLORS.pull;
}

// Internal fill of a node/pill: its rim colour washed in at low alpha over the dark canvas, so the
// interior is a dark shade of the rim. Shared by Ponds, Ripples, and the trigger pills so they match.
const FILL_ALPHA = '17'; // ~9% — tune to taste
export function nodeFill(rim: string): string {
  return `${rim}${FILL_ALPHA}`;
}
