// Live Catchment UI types. The DAG topology mirrors the playground's shape (Ponds keyed by id,
// Ripples keyed by `${pondId}.${ripple}`) so the layout engine (lib/layout.ts) is reused unchanged.
// Per-node live state is carried in a NodeView, fed from the enriched /api/status payload.

export type PondId = string; // the pond key "name@major" — one live major line
export type RippleId = string; // `${pondId}.${ripple}`

export type DemandStatus = 'running' | 'queued' | 'idle' | 'failed' | 'killed' | 'blocked' | 'repairing';
export type FreqUnit = 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY' | 'WEEK';
export type Weekday = 'MON' | 'TUE' | 'WED' | 'THU' | 'FRI' | 'SAT' | 'SUN';
export type TriggerKind = 'wave' | 'tide';

// ─── Topology (drives layout) ────────────────────────────────────────────────

export interface Pond {
  id: PondId;
  name: string;
  kind: string; // inlet | pond | outlet
  isDraw: boolean; // a Pond Draw — fed by a duct from an upstream Catchment
  isSpout: boolean; // a Spout — egresses its source's output to an external system (terminal)
  sources: PondId[];
}

export interface Ripple {
  id: RippleId;
  pondId: PondId;
  name: string;
  parents: RippleId[];
}

// ─── Live per-node state ─────────────────────────────────────────────────────

// Freshness is a ms-epoch (0 = NEVER, so formatAge renders "—"); targetF is null when there is no
// outstanding push target (vs. 0, which would be a real timestamp).
export interface NodeView {
  status: DemandStatus;
  startF: number;
  endF: number;
  targetF: number | null;
  hasPull: boolean;
  runsStarted: number;
  runsCompleted: number;
  dMs: number; // window delay carried by the current freshness (Pond only; 0 for Ripples)
}

export interface TriggerView {
  kind: TriggerKind;
  boundMs: number | null; // Tide only: the staleness bound
}

export interface PondInfo {
  version: string;
  major: number;
  kind: string;
  hasTables: boolean; // this major line has published tables — the data viewer is offered
  hasObjects: boolean; // this major line has published non-tabular Objects — the viewer's Objects tab is offered
  // Fault tolerance + control (Ponds only).
  isFailed: boolean;
  isBlocked: boolean;
  blockedReason: string | null;
  isKilled: boolean;
  refreshPending: boolean; // next run is a cold wipe-and-rebuild (control refresh)
  repairing: boolean; // in an active repair plan
  failedF: string | null; // freshness the failed Run was reaching
  failures: number; // failed Runs this episode (vs sourceRetries)
  missingSources: string[]; // declared Sources absent from the Catchment (pond keys "name@major")
  blockedBy: string[]; // required Sources that are down — the reason for an upstream block
  error: string | null; // failure message of the freshest failed Run, when failed
  immediateRetries: number; // live budget: Ripple retries within a Run
  sourceRetries: number; // live budget: Runs retried on a Source change
  // Spout nodes only: its egress config + standing-Wake armed state.
  spout?: { destination: string; table: string | null; mode: string; armed: boolean } | null;
}

// ─── Run history ─────────────────────────────────────────────────────────────

export interface RippleRun {
  ripple: string;
  startedAt: string | null;
  finishedAt: string | null;
  status: string;
  retry: number; // attempt index (0 = first try); a Ripple's failed attempts + final outcome form a trace
  error: string | null; // failure message for this attempt, if it errored
  traceback: string | null; // full traceback for this attempt, if it errored
}

export interface PondRun {
  pond: string;
  id: string; // pond key "name@major"
  major: number;
  version: string;
  f: string;
  startedAt: string | null;
  finishedAt: string | null;
  status: string;
  error: string | null; // Pond-level failure message (dead/silent Duck, ledger error), if any
  traceback: string | null; // full traceback for a Pond-level failure, if any
  ripples?: RippleRun[];
}

// ─── Cross-Catchment lineage view (/api/view) ─────────────────────────────────

// A read-only Pond in an upstream Catchment's container (subset of the status pond shape).
export interface ViewPond {
  id: PondId;
  name: string;
  status: DemandStatus;
  is_draw: boolean;
  end_f: string | null;
  start_f: string | null;
}

export interface ViewCatchment {
  id: string | null;
  name: string | null;
  reachable: boolean;
  ponds: ViewPond[];
  edges: [PondId, PondId][]; // [source, sink] within this Catchment
}

export interface DuctEdge {
  from: { catchment: string | null; pond: PondId }; // upstream source Pond
  to: { catchment: string | null; pond: PondId }; // the consumer's Draw node
}

export interface ViewPayload {
  catchments: ViewCatchment[];
  duct_edges: DuctEdge[];
}

// ─── Windows ─────────────────────────────────────────────────────────────────

// Matches the backend list_windows shape (ISO start/until, seconds duration). Operational config
// managed via the API, not declared in pond.toml.
export interface WindowRow {
  name: string;
  startAnchor: string;
  durationSeconds: number;
  freqUnit: FreqUnit;
  freqInterval: number;
  validDays: string | null;
  untilTime: string | null;
}
