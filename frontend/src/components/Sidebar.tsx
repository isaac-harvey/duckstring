'use client';

import { useEffect, useState } from 'react';
import { fetchAlerts, fetchLineage, fetchRuns, fetchSecrets, testSpout, type RawAlertChannel, type RawLineageRipple } from '@/lib/api';
import { useLiveStore, atLeast, formatAge, formatDuration, mapRun, parseTs, runKey, THEME_PULL, THEME_PUSH, THEME_SUCCESS, THEME_DANGER, THEME_BLOCKED, THEME_WAKE, THEME_BRAND } from '@/lib/store';
import type { FreqUnit, Pond, PondInfo, PondRun } from '@/lib/types';
import { AlertChannelForm, ChannelRow } from './AlertsMenu';
import { DuckSection } from './DuckSection';
import { TraceChart } from './TraceChart';
import { WindowEditor } from './WindowEditor';

function Btn({
  onClick,
  children,
  color = THEME_PUSH,
  small = false,
  disabled = false,
  block = false,
}: {
  onClick: () => void;
  children: React.ReactNode;
  color?: string;
  small?: boolean;
  disabled?: boolean;
  block?: boolean;
}) {
  return (
    <button
      onClick={disabled ? undefined : onClick}
      disabled={disabled}
      style={{
        background: 'transparent',
        border: `1px solid ${color}`,
        color,
        borderRadius: 5,
        padding: small ? '2px 8px' : '5px 12px',
        fontSize: small ? 11 : 12,
        cursor: disabled ? 'not-allowed' : 'pointer',
        fontWeight: 600,
        letterSpacing: '0.04em',
        opacity: disabled ? 0.35 : 1,
        width: block ? '100%' : undefined,
      }}
    >
      {children}
    </button>
  );
}

// A 4-column row of equal-width buttons — so the Trigger and Control rows line up.
const quadRow: React.CSSProperties = { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 6 };


function Label({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 10, fontWeight: 700, color: '#52525b', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 6 }}>
      {children}
    </div>
  );
}

function Section({ children }: { children: React.ReactNode }) {
  return <div style={{ borderTop: '1px solid #27272a', paddingTop: 14, marginTop: 14 }}>{children}</div>;
}

// A bordered, tinted callout for a Pond's failed/blocked state — shown in the Triggers section in
// place of the trigger buttons (triggers have no effect while halted), so its presence is the signal.
function StatusCard({ color, title, children }: { color: string; title: string; children?: React.ReactNode }) {
  return (
    <div style={{ border: `1px solid ${color}`, background: `${color}14`, borderRadius: 6, padding: '10px 12px' }}>
      <div style={{ fontSize: 11, fontWeight: 700, color, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: children ? 7 : 0 }}>
        {title}
      </div>
      {children && (
        <div style={{ fontSize: 12, color: '#d4d4d8', lineHeight: 1.6, wordBreak: 'break-word' }}>{children}</div>
      )}
    </div>
  );
}

// The reason a Pond is failed/killed/blocked (missing Sources / a failed upstream), with details.
// Null when the Pond is healthy.
function StatusBox({ info, ponds, canControl }: { info: PondInfo; ponds: Record<string, Pond>; canControl: boolean }) {
  const bullets = (items: string[], render: (s: string) => string) => (
    <ul style={{ margin: 0, paddingLeft: 18, color: '#fafafa', listStyleType: 'disc', listStylePosition: 'outside' }}>
      {items.map((s) => <li key={s} style={{ marginTop: 2 }}>{render(s)}</li>)}
    </ul>
  );

  if (info.isFailed) {
    // A contract failure gets its own sub-reason: the Duck refused to publish (last-good data is
    // intact) because the output broke the major line's additive schema contract — the remedy is a
    // major bump (or restoring the columns), not a retry.
    const contract = info.failureKind === 'contract';
    return (
      <StatusCard color={THEME_DANGER} title={contract ? 'Failed · Contract violation' : 'Failed'}>
        {contract && (
          <div style={{ color: '#a1a1aa', marginBottom: 6 }}>
            The publish was refused — last-good data is intact. Breaking schema changes need a major bump.
          </div>
        )}
        {info.error ?? 'The most recent Pond Run failed.'}
      </StatusCard>
    );
  }
  if (info.isKilled) {
    return (
      <StatusCard color={THEME_DANGER} title="Killed">
        Stopped by an operator.{canControl ? ' Wake, Force, or clear to resume.' : ''}
      </StatusCard>
    );
  }
  if (info.isBlocked) {
    if (info.blockedReason) {
      return <StatusCard color={THEME_BLOCKED} title="Blocked · Waiting for a Source asset">{info.blockedReason}</StatusCard>;
    }
    if (info.missingSources.length > 0) {
      return <StatusCard color={THEME_BLOCKED} title="Blocked · Missing Sources">{bullets(info.missingSources, (s) => s)}</StatusCard>;
    }
    if (info.blockedBy.length > 0) {
      return <StatusCard color={THEME_BLOCKED} title="Blocked · Upstream unavailable">{bullets(info.blockedBy, (s) => ponds[s]?.name ?? s)}</StatusCard>;
    }
    return <StatusCard color={THEME_BLOCKED} title="Blocked" />;
  }
  return null;
}

// A Spout node (egress) — its destination + state, and the Control set on its standing Wake.
function SpoutPanel({ pond, canControl }: { pond: Pond; canControl: boolean }) {
  const info = useLiveStore((s) => s.pondInfo[pond.id]);
  const view = useLiveStore((s) => s.pondViews[pond.id]);
  const now = useLiveStore((s) => s.now);
  const spoutControl = useLiveStore((s) => s.spoutControl);
  const removeSpout = useLiveStore((s) => s.removeSpout);
  const cfg = info?.spout;
  if (!cfg) return null;
  const state = info?.isKilled ? 'killed' : info?.isFailed ? 'failed' : cfg.armed ? 'armed' : 'asleep';
  return (
    <>
      <Section>
        <Label>Spout · egress</Label>
        <div style={{ fontSize: 11, color: '#a1a1aa', wordBreak: 'break-all', marginBottom: 6 }}>→ {cfg.destination}</div>
        <div style={{ fontSize: 11, color: '#71717a', lineHeight: 1.7 }}>
          <div>table <span style={{ color: '#a1a1aa' }}>{cfg.table ?? 'all'}</span> · mode <span style={{ color: '#a1a1aa' }}>{cfg.mode}</span></div>
          <div>delivered <span style={{ color: '#a1a1aa' }}>{formatAge(view?.endF ?? 0, now)}</span> old · {state}</div>
        </div>
        {info?.isFailed && info.error && (
          <div style={{ marginTop: 8, border: `1px solid ${THEME_DANGER}`, background: `${THEME_DANGER}14`, borderRadius: 6, padding: '8px 10px', fontSize: 12, color: '#d4d4d8', wordBreak: 'break-word' }}>
            {info.error}
          </div>
        )}
      </Section>
      {canControl && (
        <Section>
          <Label>Control</Label>
          <div style={quadRow}>
            <Btn block onClick={() => spoutControl(pond.id, 'force')} color={THEME_SUCCESS}>Force</Btn>
            <Btn block onClick={() => spoutControl(pond.id, cfg.armed ? 'sleep' : 'wake')} color={cfg.armed ? THEME_BLOCKED : THEME_WAKE}>
              {cfg.armed ? 'Sleep' : 'Wake'}
            </Btn>
            <Btn block onClick={() => spoutControl(pond.id, 'kill')} color={THEME_DANGER}>Kill</Btn>
            <Btn block onClick={() => spoutControl(pond.id, 'clear')} color={THEME_PULL}>Clear</Btn>
          </div>
          <div style={{ marginTop: 6 }}>
            <Btn onClick={() => removeSpout(pond.id)} color={THEME_DANGER}>Remove Spout</Btn>
          </div>
        </Section>
      )}
      <Section>
        <Label>Windows · throttle</Label>
        <WindowEditor pond={pond} readOnly={!canControl} caption="Throttle delivery to a window cadence. None ⇒ deliver on every source advance." />
      </Section>
    </>
  );
}

// The egress Spouts on a source Pond — list (click to inspect) + an add form whose credentials are
// *names* (assembled as ${env:NAME} or ${secret:NAME}), never values; a value never crosses the wire.
function SpoutEditor({ sourceId, canControl }: { sourceId: string; canControl: boolean }) {
  const ponds = useLiveStore((s) => s.ponds);
  const pondInfo = useLiveStore((s) => s.pondInfo);
  const addSpout = useLiveStore((s) => s.addSpout);
  const selectPond = useLiveStore((s) => s.selectPond);
  const spouts = Object.values(ponds).filter((p) => p.isSpout && p.sources.includes(sourceId));

  const [open, setOpen] = useState(false);
  const [scheme, setScheme] = useState('s3');
  const [path, setPath] = useState('');
  const [table, setTable] = useState('');
  const [keyVar, setKeyVar] = useState('');
  const [secretVar, setSecretVar] = useState('');
  // Where the credential names resolve from: the Catchment's process env (${env:NAME}), or the
  // catchment-wide write-only secret store (${secret:NAME}, set under the brand box).
  const [credKind, setCredKind] = useState<'env' | 'secret'>('env');
  const [storedNames, setStoredNames] = useState<string[]>([]);
  const [mode, setMode] = useState('auto');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  // The connection-test outcome (the "Test" button), tagged with the destination it ran against so it
  // only shows while that destination is still in the form (no effect needed to clear it on edits).
  const [testResult, setTestResult] = useState<{ destination: string; ok: boolean; error?: string } | null>(null);
  const [testing, setTesting] = useState(false);

  // Offer the stored secret names as a datalist once the form opens (full access only — the names
  // route is full-gated; a 403 just leaves the list empty).
  useEffect(() => {
    if (open && canControl) fetchSecrets().then((s) => setStoredNames(s.map((n) => n.name))).catch(() => setStoredNames([]));
  }, [open, canControl]);

  if (!canControl && spouts.length === 0) return null;
  const fld: React.CSSProperties = { ...numInput, width: '100%', boxSizing: 'border-box' };

  const buildDestination = () => {
    const ref = (name: string) => `\${${credKind}:${name.trim()}}`;
    if (scheme === 'file') return `file://${path}`;
    if (scheme === 'postgres') return `postgres://${path}`;
    const q: string[] = [];
    if (keyVar.trim()) q.push(`key_id=${ref(keyVar)}`);
    if (secretVar.trim()) q.push(`secret=${ref(secretVar)}`);
    return `${scheme}://${path}${q.length ? '?' + q.join('&') : ''}`;
  };

  const test = async () => {
    setErr(null);
    setTestResult(null);
    setTesting(true);
    const destination = buildDestination();
    try {
      setTestResult({ destination, ...(await testSpout(sourceId, destination)) });
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'test failed');
    } finally {
      setTesting(false);
    }
  };

  const submit = async () => {
    setErr(null);
    setBusy(true);
    try {
      await addSpout(sourceId, { destination: buildDestination(), table: table.trim() || null, mode });
      setOpen(false);
      setPath('');
      setTable('');
      setKeyVar('');
      setSecretVar('');
      setTestResult(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'failed to add');
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section>
      <Label>Spouts · egress</Label>
      {spouts.length === 0 && !open && <div style={{ fontSize: 12, color: '#52525b', marginBottom: 6 }}>None.</div>}
      {spouts.map((sp) => (
        <div
          key={sp.id}
          role="button"
          onClick={() => selectPond(sp.id)}
          style={{ fontSize: 12, marginBottom: 4, cursor: 'pointer', display: 'flex', justifyContent: 'space-between', gap: 8 }}
        >
          <span style={{ color: pondInfo[sp.id]?.isFailed ? THEME_DANGER : '#a1a1aa', flexShrink: 0 }}>
            {sp.name.split('#').slice(1).join('#')}
          </span>
          <span style={{ color: '#52525b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {pondInfo[sp.id]?.spout?.destination ?? ''} ›
          </span>
        </div>
      ))}
      {canControl && (open ? (
        <div style={{ marginTop: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ display: 'flex', gap: 6 }}>
            <select value={scheme} onChange={(e) => setScheme(e.target.value)} style={selectInput}>
              <option value="s3">s3://</option>
              <option value="gs">gs://</option>
              <option value="file">file://</option>
              <option value="postgres">postgres://</option>
            </select>
            <input
              value={path}
              onChange={(e) => setPath(e.target.value)}
              placeholder={scheme === 'file' ? '/abs/path' : scheme === 'postgres' ? 'user:${env:PW}@host/db' : 'bucket/prefix'}
              style={{ ...fld, flex: 1 }}
            />
          </div>
          {(scheme === 's3' || scheme === 'gs') && (
            <>
              <select value={credKind} onChange={(e) => setCredKind(e.target.value as 'env' | 'secret')} style={selectInput}>
                <option value="env">env var</option>
                <option value="secret">stored secret</option>
              </select>
              <input
                value={keyVar}
                onChange={(e) => setKeyVar(e.target.value)}
                list={credKind === 'secret' ? 'spout-secret-names' : undefined}
                placeholder={`key id — ${credKind === 'secret' ? 'secret name' : 'env var name'} (e.g. AWS_KEY)`}
                style={fld}
              />
              <input
                value={secretVar}
                onChange={(e) => setSecretVar(e.target.value)}
                list={credKind === 'secret' ? 'spout-secret-names' : undefined}
                placeholder={`secret — ${credKind === 'secret' ? 'secret name' : 'env var name'} (e.g. AWS_SECRET)`}
                style={fld}
              />
              {credKind === 'secret' && (
                <datalist id="spout-secret-names">
                  {storedNames.map((n) => <option key={n} value={n} />)}
                </datalist>
              )}
            </>
          )}
          <div style={{ display: 'flex', gap: 6 }}>
            <input value={table} onChange={(e) => setTable(e.target.value)} placeholder="table (blank = all)" style={{ ...fld, flex: 1 }} />
            <select value={mode} onChange={(e) => setMode(e.target.value)} style={selectInput}>
              <option value="auto">auto</option>
              <option value="full">full</option>
              <option value="append">append</option>
            </select>
          </div>
          <div style={{ fontSize: 10, color: '#52525b', lineHeight: 1.5 }}>
            Credentials are <b>names</b>, not values — {credKind === 'secret'
              ? 'resolved from the Catchment’s secret store'
              : 'read from the Catchment’s environment'} at delivery time.
          </div>
          {err && <div style={{ fontSize: 11, color: THEME_DANGER, wordBreak: 'break-word' }}>{err}</div>}
          {testResult && testResult.destination === buildDestination() && (
            <div style={{ fontSize: 11, color: testResult.ok ? THEME_SUCCESS : THEME_DANGER, wordBreak: 'break-word' }}>
              {testResult.ok ? '✓ connection ok' : `✕ ${testResult.error ?? 'connection failed'}`}
            </div>
          )}
          <div style={{ display: 'flex', gap: 6 }}>
            <Btn small onClick={test} color={THEME_PULL} disabled={testing || busy || !path.trim()}>
              {testing ? 'Testing…' : 'Test'}
            </Btn>
            <Btn small onClick={submit} color={THEME_SUCCESS} disabled={busy || testing || !path.trim()}>Add</Btn>
            <Btn small onClick={() => { setOpen(false); setErr(null); setTestResult(null); }} color={THEME_BLOCKED}>Cancel</Btn>
          </div>
        </div>
      ) : (
        <div style={{ marginTop: 6 }}>
          <Btn block onClick={() => setOpen(true)} color={THEME_BRAND}>+ Add Spout</Btn>
        </div>
      ))}
    </Section>
  );
}

// The notification channels scoped to this Pond (full access only) — list (with per-channel Test +
// remove) + an add form pinned to this Pond's name. Catchment-wide channels are managed under the
// "Alerts" button by Collapse-all; this section is the per-Pond view of the same alert_channel store.
// The observed table-level lineage of the selected Pond (plans/lineage.md Phase 1): what each Ripple
// actually read (per source) and wrote on its latest recorded run — recorded at the call, never inferred.
function LineageSection({ pond }: { pond: Pond }) {
  const major = useLiveStore((s) => s.pondInfo[pond.id]?.major);
  const [ripples, setRipples] = useState<RawLineageRipple[] | null>(null);

  // Load once on mount — keyed by pond id in the parent, so a pond switch remounts fresh.
  useEffect(() => {
    fetchLineage(pond.name, major)
      .then((ps) => setRipples(ps.find((p) => p.id === pond.id)?.ripples ?? []))
      .catch(() => setRipples([]));
  }, [pond.id, pond.name, major]);

  if (!ripples || ripples.length === 0) return null; // nothing recorded yet — say nothing, not "unknown"
  return (
    <Section>
      <Label>Lineage · observed</Label>
      {ripples.map((r) => (
        <div key={r.ripple} style={{ fontSize: 11, marginBottom: 6, lineHeight: 1.6 }}>
          <div style={{ color: '#e4e4e7' }}>{r.ripple}</div>
          {r.reads.map((rd) => (
            <div key={`${rd.source ?? ''}.${rd.table}`} style={{ color: '#71717a', paddingLeft: 10 }}>
              ← {rd.source ? `${rd.source}.${rd.table}` : <span>{rd.table} <span style={{ color: '#52525b' }}>(own)</span></span>}
            </div>
          ))}
          {r.writes.map((w) => (
            <div key={w} style={{ color: THEME_SUCCESS, paddingLeft: 10 }}>→ {w}</div>
          ))}
        </div>
      ))}
    </Section>
  );
}

function AlertEditor({ pond, canControl }: { pond: Pond; canControl: boolean }) {
  const [channels, setChannels] = useState<RawAlertChannel[]>([]);
  const [open, setOpen] = useState(false);

  const load = () =>
    fetchAlerts().then((cs) => setChannels(cs.filter((c) => c.scope === pond.id))).catch(() => setChannels([]));
  // Load once on mount — the parent keys this component by pond id, so a pond switch remounts it fresh.
  useEffect(() => { if (canControl) void load(); }, [canControl]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!canControl) return null;
  return (
    <Section>
      <Label>Alerts</Label>
      {channels.length === 0 && !open && <div style={{ fontSize: 12, color: '#52525b', marginBottom: 6 }}>None.</div>}
      {channels.map((c) => <ChannelRow key={c.name} channel={c} onChanged={load} />)}
      {open ? (
        <div style={{ marginTop: 8 }}>
          <AlertChannelForm fixedScope={pond.id} onAdded={() => { setOpen(false); void load(); }} />
          <div style={{ marginTop: 6 }}>
            <Btn small onClick={() => setOpen(false)} color={THEME_BLOCKED}>Cancel</Btn>
          </div>
        </div>
      ) : (
        <div style={{ marginTop: 6 }}>
          <Btn block onClick={() => setOpen(true)} color={THEME_BRAND}>+ Add Alert</Btn>
        </div>
      )}
    </Section>
  );
}

const numInput: React.CSSProperties = {
  width: 64,
  background: '#1a1a1f',
  border: '1px solid #3f3f46',
  borderRadius: 4,
  color: '#e4e4e7',
  padding: '3px 6px',
  fontSize: 12,
};

const selectInput: React.CSSProperties = { ...numInput, width: 'auto' };

const TIDE_UNITS: { value: FreqUnit; label: string; secs: number }[] = [
  { value: 'SECOND', label: 'sec', secs: 1 },
  { value: 'MINUTE', label: 'min', secs: 60 },
  { value: 'HOUR', label: 'hr', secs: 3600 },
  { value: 'DAY', label: 'day', secs: 86400 },
  { value: 'WEEK', label: 'wk', secs: 604800 },
];

const ms = (iso: string | null): number => parseTs(iso);

// The plot shows at most this many (most-recent) runs; older windows are reached via the date range.
const RUN_CAP = 50;

// A per-run point on the timing trace — one per run that has a duration (both timestamps). `run` is the
// Pond Run it maps to, so a click selects it (opening Run Detail) and a selected run highlights back here.
type TracePoint = { key: string; time: number; duration: number; run: PondRun };

// Per-run points for a set of Pond Runs — each run's completion time + its duration. Sorted ascending by
// time so the plot renders oldest→newest regardless of the source order (the feed is newest-first, but a
// "from"-anchored range fetch comes back ascending).
function pondTrace(runs: PondRun[]): TracePoint[] {
  const pts: TracePoint[] = [];
  for (const r of runs) {
    if (r.startedAt && r.finishedAt) {
      pts.push({ key: runKey(r), time: ms(r.finishedAt), duration: ms(r.finishedAt) - ms(r.startedAt), run: r });
    }
  }
  return pts.sort((a, b) => a.time - b.time);
}

// Per-run points for one Ripple across the Pond's run history — plots the *ripple's* timing, but still
// maps to the containing Pond Run (Run Detail is per Pond Run). Sorted ascending by time.
function rippleTrace(runs: PondRun[], rippleName: string): TracePoint[] {
  const pts: TracePoint[] = [];
  for (const r of runs) {
    const rr = r.ripples?.find((x) => x.ripple === rippleName);
    if (rr?.startedAt && rr?.finishedAt) {
      pts.push({ key: runKey(r), time: ms(rr.finishedAt), duration: ms(rr.finishedAt) - ms(rr.startedAt), run: r });
    }
  }
  return pts.sort((a, b) => a.time - b.time);
}

// The interactive timing chart wired to run selection: clicking a point opens that run in Run Detail;
// the selected run (from anywhere — e.g. a Run History click) enlarges its point here.
function RunTrace({ points }: { points: TracePoint[] }) {
  const selectRun = useLiveStore((s) => s.selectRun);
  const selectedRun = useLiveStore((s) => s.selectedRun);
  const selectedKey = selectedRun ? runKey(selectedRun) : null;
  return (
    <TraceChart
      points={points}
      selectedKey={selectedKey}
      onSelect={(key) => { const p = points.find((x) => x.key === key); if (p) selectRun(p.run); }}
    />
  );
}

// ─── Options modal: the rarely-needed per-Pond config, off the always-visible sidebar ───────────────

const modalBackdrop: React.CSSProperties = {
  position: 'fixed', inset: 0, zIndex: 1100, display: 'flex', alignItems: 'center', justifyContent: 'center',
  background: 'rgba(9,9,11,0.78)', backdropFilter: 'blur(2px)', fontFamily: 'ui-monospace, SFMono-Regular, monospace',
};

function PondOptionsModal({ pond, canControl, onClose }: { pond: Pond; canControl: boolean; onClose: () => void }) {
  const pondInfo = useLiveStore((s) => s.pondInfo);
  const setBudget = useLiveStore((s) => s.setBudget);
  const info = pondInfo[pond.id];
  // Retry-budget inputs, seeded from the Pond's live config (the modal remounts per pond, so a plain
  // initializer is enough — no re-seed-on-poll dance needed).
  const [immRetries, setImmRetries] = useState(info ? String(info.immediateRetries) : '0');
  const [srcRetries, setSrcRetries] = useState(info ? String(info.sourceRetries) : '0');

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div onClick={onClose} style={modalBackdrop}>
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: '#15151a', border: '1px solid #27272a', borderRadius: 10, width: 'min(460px, 92vw)',
          maxHeight: '82vh', display: 'flex', flexDirection: 'column', overflow: 'hidden', color: '#e4e4e7',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '12px 16px', borderBottom: '1px solid #27272a', flexShrink: 0 }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: '#e4e4e7' }}>{pond.name}</span>
          <span style={{ fontSize: 11, color: '#71717a' }}>Options</span>
          <button
            onClick={onClose}
            title="Close (Esc)"
            style={{ marginLeft: 'auto', background: 'transparent', border: '1px solid #3f3f46', borderRadius: 5,
                     color: '#a1a1aa', fontSize: 13, lineHeight: 1, padding: '4px 9px', cursor: 'pointer', fontFamily: 'inherit' }}
          >
            ✕
          </button>
        </div>
        <div style={{ overflowY: 'auto', padding: '0 16px 16px', minHeight: 0 }}>
          {/* Retry budgets (full only). */}
          {canControl && (
            <Section>
              <Label>Failures · retry budgets</Label>
              {([
                ['Immediate Retries', immRetries, setImmRetries],
                ['On Change Retries', srcRetries, setSrcRetries],
              ] as const).map(([label, value, setValue]) => (
                <div key={label} style={{ display: 'flex', gap: 6, alignItems: 'center', marginBottom: 6 }}>
                  <span style={{ flex: 1, fontSize: 11, color: '#a1a1aa' }}>{label}</span>
                  <input type="number" min="0" step="1" value={value} onChange={(e) => setValue(e.target.value)} style={{ ...numInput, width: 48 }} />
                  <Btn
                    small
                    color={THEME_WAKE}
                    onClick={() => setBudget(pond.id, Math.max(0, parseInt(immRetries) || 0), Math.max(0, parseInt(srcRetries) || 0))}
                  >
                    Set
                  </Btn>
                </div>
              ))}
            </Section>
          )}
          {/* Compute config (full only; Draws/Spouts carry none). */}
          {canControl && info?.duck && (
            <Section>
              <Label>Compute</Label>
              <DuckSection pondId={pond.id} duck={info.duck} />
            </Section>
          )}
          {/* Egress Spouts — a Draw can't be a source. */}
          {!pond.isDraw && <SpoutEditor sourceId={pond.id} canControl={canControl} />}
          {/* Observed lineage (read-visible) + alert channels scoped to this Pond. */}
          {!pond.isDraw && <LineageSection key={`lin-${pond.id}`} pond={pond} />}
          <AlertEditor key={pond.id} pond={pond} canControl={canControl} />
        </div>
      </div>
    </div>
  );
}

// ─── Run-timing panel: the bottom-right quadrant (Pond sidebar column × the run-history bottom bar) ──

const dateInput: React.CSSProperties = {
  flex: 1, minWidth: 0, background: '#18181b', border: '1px solid #3f3f46', borderRadius: 5, color: '#a1a1aa',
  padding: '2px 4px', fontSize: 10, fontFamily: 'inherit', outline: 'none', colorScheme: 'dark',
};

// A per-bound clear (✕) — fixed-width so both bound rows' inputs stay equal; greyed/disabled when empty.
function ClearX({ active, onClick, title }: { active: boolean; onClick: () => void; title: string }) {
  return (
    <button
      onClick={onClick}
      disabled={!active}
      title={title}
      style={{ width: 14, flexShrink: 0, textAlign: 'center', background: 'transparent', border: 'none',
               color: active ? '#a1a1aa' : '#3f3f46', cursor: active ? 'pointer' : 'default', fontSize: 12,
               padding: 0, fontFamily: 'inherit' }}
    >
      ✕
    </button>
  );
}

export function TracePanel() {
  const selectedPondId = useLiveStore((s) => s.selectedPondId);
  const selectedRippleId = useLiveStore((s) => s.selectedRippleId);
  const ripples = useLiveStore((s) => s.ripples);
  const ponds = useLiveStore((s) => s.ponds);
  const selectedPondRuns = useLiveStore((s) => s.selectedPondRuns);
  const selectedRipple = selectedRippleId ? ripples[selectedRippleId] : null;
  const pond = selectedPondId ? ponds[selectedPondId] : null;
  const active = selectedRipple || pond;

  // Date-range navigation (local day bounds → UTC ISO). Empty = the last RUN_CAP runs (selectedPondRuns);
  // a range fetches that window (capped) from the backend so a long history is navigable.
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [rangeRuns, setRangeRuns] = useState<PondRun[] | null>(null);
  const rangeActive = !!(from || to);

  // A selection change abandons a stale range (it would show another node's dates).
  const [prevSel, setPrevSel] = useState(selectedPondId);
  if (selectedPondId !== prevSel) { setPrevSel(selectedPondId); setFrom(''); setTo(''); setRangeRuns(null); }

  useEffect(() => {
    if (!rangeActive || !selectedPondId) return;
    let cancelled = false;
    // datetime-local values are local wall-clock — toISOString converts to the UTC the backend stores.
    const iso = (v: string) => { const d = new Date(v); return v && !isNaN(d.getTime()) ? d.toISOString() : undefined; };
    fetchRuns({ pond: selectedPondId, lineage: false, ripples: true, limit: RUN_CAP, after: iso(from), before: iso(to) })
      .then((rows) => { if (!cancelled) setRangeRuns(rows.map(mapRun)); })
      .catch(() => { if (!cancelled) setRangeRuns([]); });
    return () => { cancelled = true; };
  }, [rangeActive, from, to, selectedPondId]);

  const source = (rangeActive ? rangeRuns ?? [] : selectedPondRuns).slice(0, RUN_CAP); // newest-first
  let title: string | null = null;
  let points: TracePoint[] | null = null;
  if (selectedRipple) { title = `${selectedRipple.name} · run timing`; points = rippleTrace(source, selectedRipple.name); }
  else if (pond) { title = `${pond.name} · run timing`; points = pondTrace(source); }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', padding: 14, boxSizing: 'border-box', overflow: 'hidden', background: '#15151a' }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: '#52525b', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 8, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
        {title ?? 'Run timing'}
      </div>
      {active && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginBottom: 8 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 26, fontSize: 10, color: '#52525b' }}>from</span>
            <input type="datetime-local" step="1" value={from} max={to || undefined}
                   onChange={(e) => setFrom(e.target.value)} style={dateInput} />
            <ClearX active={!!from} onClick={() => setFrom('')} title="Clear the from bound" />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 26, fontSize: 10, color: '#52525b' }}>to</span>
            <input type="datetime-local" step="1" value={to} min={from || undefined}
                   onChange={(e) => setTo(e.target.value)} style={dateInput} />
            <ClearX active={!!to} onClick={() => setTo('')} title="Clear the to bound" />
          </div>
        </div>
      )}
      {points ? (
        <>
          <RunTrace points={points} />
          <div style={{ fontSize: 10, color: '#3f3f46', marginTop: 4 }}>
            {rangeActive ? `${points.length} in range` : `last ${points.length}`}
            {!rangeActive && selectedPondRuns.length > RUN_CAP ? ' · set a time range for older' : ''}
          </div>
        </>
      ) : (
        <div style={{ fontSize: 12, color: '#52525b', margin: 'auto', textAlign: 'center' }}>Select a Pond or Ripple</div>
      )}
    </div>
  );
}

export function Sidebar({ mobile = false }: { mobile?: boolean }) {
  const ponds = useLiveStore((s) => s.ponds);
  const ripples = useLiveStore((s) => s.ripples);
  const pondViews = useLiveStore((s) => s.pondViews);
  const rippleViews = useLiveStore((s) => s.rippleViews);
  const pondInfo = useLiveStore((s) => s.pondInfo);
  const triggers = useLiveStore((s) => s.triggers);
  const now = useLiveStore((s) => s.now);
  const selectedPondRuns = useLiveStore((s) => s.selectedPondRuns);

  const selectedPondId = useLiveStore((s) => s.selectedPondId);
  const selectedRippleId = useLiveStore((s) => s.selectedRippleId);
  const selectedTriggerId = useLiveStore((s) => s.selectedTriggerId);

  const tap = useLiveStore((s) => s.tap);
  const pulse = useLiveStore((s) => s.pulse);
  const wave = useLiveStore((s) => s.wave);
  const tide = useLiveStore((s) => s.tide);
  const wake = useLiveStore((s) => s.wake);
  const sleep = useLiveStore((s) => s.sleep);
  const force = useLiveStore((s) => s.force);
  const kill = useLiveStore((s) => s.kill);
  const removeTrigger = useLiveStore((s) => s.removeTrigger);
  const clearFailure = useLiveStore((s) => s.clearFailure);

  // Access level gates the action surface (the backend enforces it too — this just avoids dead buttons).
  // read: status/history/data only · demand: + the Triggers menu · full: + Control/Windows/Failures.
  const accessLevel = useLiveStore((s) => s.accessLevel);
  const canDemand = atLeast(accessLevel, 'demand');
  const canControl = atLeast(accessLevel, 'full');

  const [tideBound, setTideBound] = useState('2');
  const [tideUnit, setTideUnit] = useState<FreqUnit>('SECOND');
  const [showTideInput, setShowTideInput] = useState(false);
  // The Options modal (retry budgets, Compute, Spouts, Alerts, Lineage) — opened by the Options bar
  // beneath the always-visible sections.
  const [optionsOpen, setOptionsOpen] = useState(false);

  const selectedPond = selectedPondId ? ponds[selectedPondId] : null;
  const selectedRipple = selectedRippleId ? ripples[selectedRippleId] : null;
  const trigger = selectedPondId ? triggers[selectedPondId] : undefined;
  const isInlet = selectedPond ? selectedPond.sources.length === 0 : false;
  const selectedInfo = selectedPondId ? pondInfo[selectedPondId] : undefined;
  // Failed/killed/blocked all no-op triggers (only Control/Clear recover them), so the Triggers
  // section shows the reason in place of the buttons.
  const halted = !!selectedInfo && (selectedInfo.isFailed || selectedInfo.isKilled || selectedInfo.isBlocked);

  // Mobile: the sidebar is a collapsible bottom sheet; selecting a node opens it
  // (state adjusted during render, like the budget inputs above).
  const [collapsed, setCollapsed] = useState(true);
  const selectionKey = selectedRippleId ?? selectedTriggerId ?? selectedPondId;
  const [prevSelectionKey, setPrevSelectionKey] = useState(selectionKey);
  if (selectionKey !== prevSelectionKey) {
    setPrevSelectionKey(selectionKey);
    setOptionsOpen(false); // a new selection closes a stale Options modal
    if (mobile && selectionKey) setCollapsed(false);
  }

  const headerContext = selectedTriggerId
    ? `${ponds[selectedTriggerId]?.name ?? selectedTriggerId} trigger`
    : selectedRipple
      ? `${ponds[selectedRipple.pondId]?.name ?? ''} / ${selectedRipple.name}`
      : selectedPond?.name ?? null;


  const content = (
    <>
      {!selectedPond && !selectedRipple && !selectedTriggerId && (
        <>
          <div style={{ fontSize: 12, color: '#71717a', lineHeight: 1.6 }}>
            Select a Pond or Ripple
          </div>
          <div style={{ fontSize: 12, color: '#71717a', lineHeight: 1.6 }}>
            (deployed from CLI)
          </div>
        </>
      )}

      {/* Trigger node selected */}
      {selectedTriggerId && triggers[selectedTriggerId] && (
        <Section>
          <Label>Trigger · {selectedTriggerId}</Label>
          {triggers[selectedTriggerId].kind === 'wave' ? (
            <div style={{ fontSize: 12, color: '#a1a1aa', marginBottom: 10 }}>Wave — standing pull.</div>
          ) : (
            <div style={{ fontSize: 12, color: '#a1a1aa', marginBottom: 10 }}>
              Tide — max staleness ≤ {formatDuration(triggers[selectedTriggerId].boundMs ?? 1000)}.
            </div>
          )}
          {canDemand && (
            <Btn onClick={() => removeTrigger(selectedTriggerId)} color={THEME_DANGER}>Remove Trigger</Btn>
          )}
        </Section>
      )}

      {/* Pond selected */}
      {selectedPond && !selectedRippleId && (
        <>
          <Section>
            <Label>
              {selectedPond.isSpout ? 'Spout' : 'Pond'}: {selectedPond.isSpout
                ? selectedPond.name.split('#').slice(1).join('#') || selectedPond.name
                : selectedPond.name}
            </Label>
            <div style={{ fontSize: 11, color: '#71717a', marginBottom: 8, lineHeight: 1.7 }}>
              <div>
                {selectedPond.isSpout ? 'egress' : (pondInfo[selectedPond.id]?.kind ?? 'pond')}
                {!selectedPond.isSpout && (
                  <>
                    <span style={{ color: '#52525b' }}> · </span>
                    v<span style={{ color: '#a1a1aa' }}>{pondInfo[selectedPond.id]?.version ?? '—'}</span>
                  </>
                )}
              </div>
              <div>
                Runs: <span style={{ color: '#a1a1aa' }}>{pondViews[selectedPond.id]?.runsCompleted ?? 0}</span>
                <span style={{ color: '#52525b' }}> · </span>
                fresh <span style={{ color: '#a1a1aa' }}>{formatAge(pondViews[selectedPond.id]?.endF ?? 0, now)}</span> old
              </div>
            </div>

            {selectedPond.sources.length > 0 && (
              <div style={{ marginTop: 8 }}>
                <Label>Sources</Label>
                {selectedPond.sources.map((sid) => (
                  <div key={sid} style={{ fontSize: 12, color: '#a1a1aa', marginBottom: 3 }}>{ponds[sid]?.name ?? sid}</div>
                ))}
              </div>
            )}
          </Section>

          {/* A Spout is a real node but its surface is its own (egress destination + the standing-Wake
              control), so it replaces the Pond's Triggers/Control/Windows sections. */}
          {selectedPond.isSpout ? (
            <SpoutPanel pond={selectedPond} canControl={canControl} />
          ) : (
          <>

          {/* Windows (Inlet ponds only) */}
          {isInlet && (
            <Section>
              <Label>Windows (batch source)</Label>
              <WindowEditor pond={selectedPond} readOnly={!canControl} />
            </Section>
          )}

          {/* Triggers — or, while halted, the reason (triggers have no effect). The failure reason is
              read-level (shown to all); the trigger buttons are demand+. A read user on a healthy Pond
              sees no Triggers section at all. */}
          {(halted || canDemand) && (
          <Section>
            <Label>Triggers</Label>
            {halted ? (
              <StatusBox info={selectedInfo!} ponds={ponds} canControl={canControl} />
            ) : (
              <>
                <div style={quadRow}>
                  <Btn block onClick={() => tap(selectedPond.id)} color={THEME_PULL}>Tap</Btn>
                  <Btn block onClick={() => wave(selectedPond.id)} color={THEME_PULL}>Wave</Btn>
                  <Btn block onClick={() => pulse(selectedPond.id)} color={THEME_PUSH}>Pulse</Btn>
                  <Btn block onClick={() => setShowTideInput((v) => !v)} color={THEME_PUSH}>Tide</Btn>
                </div>
                {showTideInput && (
                  <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginTop: 8 }}>
                    <span style={{ fontSize: 11, color: '#71717a' }} title="keep no more stale than">≤</span>
                    <input type="number" min="1" step="1" value={tideBound} onChange={(e) => setTideBound(e.target.value)} style={{ ...numInput, width: 56 }} />
                    <select value={tideUnit} onChange={(e) => setTideUnit(e.target.value as FreqUnit)} style={selectInput}>
                      {TIDE_UNITS.map((u) => (
                        <option key={u.value} value={u.value}>{u.label}</option>
                      ))}
                    </select>
                    <Btn
                      small
                      onClick={() => {
                        const secs = TIDE_UNITS.find((u) => u.value === tideUnit)!.secs;
                        tide(selectedPond.id, Math.max(0.1, parseFloat(tideBound) * secs));
                        setShowTideInput(false);
                      }}
                      color={THEME_PUSH}
                    >
                      Set
                    </Btn>
                  </div>
                )}
                {trigger && (
                  <div style={{ marginTop: 8 }}>
                    <Btn small onClick={() => removeTrigger(selectedPond.id)} color={THEME_DANGER}>
                      Remove {trigger.kind === 'wave' ? 'Wave' : 'Tide'} Trigger
                    </Btn>
                  </div>
                )}
              </>
            )}
          </Section>
          )}

          {/* Control: Force/Wake (go) and Sleep/Kill (stop) lifecycle on the Duck. Full only. */}
          {canControl && (
          <Section>
            <Label>Control</Label>
            <div style={quadRow}>
              <Btn block onClick={() => force(selectedPond.id)} color={THEME_SUCCESS}>Force</Btn>
              <Btn block onClick={() => wake(selectedPond.id)} color={THEME_WAKE}>Wake</Btn>
              <Btn block onClick={() => sleep(selectedPond.id)} color={THEME_BLOCKED}>Sleep</Btn>
              <Btn block onClick={() => kill(selectedPond.id)} color={THEME_DANGER}>Kill</Btn>
            </div>
            {/* Refresh / Reset / Remove are collection-oriented — do them via Options → Pond Actions. */}
          </Section>
          )}

          {/* Failure clearing stays visible (the remediation). The retry *budgets* moved to Options; the
              failure reason is shown above (Triggers StatusBox) and in Run Detail for every level. */}
          {canControl && selectedInfo?.isFailed && (
          <Section>
            <Label>Failure</Label>
            <Btn onClick={() => clearFailure(selectedPond.id)} color={THEME_SUCCESS}>Clear Failure</Btn>
          </Section>
          )}

          {/* Rarely-needed config lives behind Options: retry budgets, Compute, Spouts, Alerts, Lineage. */}
          <div style={{ borderTop: '1px solid #27272a', marginTop: 14, paddingTop: 14 }}>
            <button
              onClick={() => setOptionsOpen(true)}
              style={{
                width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
                background: '#1a1a1f', border: '1px solid #3f3f46', borderRadius: 6, padding: '8px 12px',
                color: '#a1a1aa', fontSize: 12, fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit',
              }}
            >
              ⚙ Options
            </button>
          </div>

          {/* Desktop shows run timing in the bottom-right quadrant (TracePanel); mobile keeps it here. */}
          {mobile && (
            <Section>
              <RunTrace points={pondTrace(selectedPondRuns.slice(0, RUN_CAP))} />
            </Section>
          )}

          </>
          )}
        </>
      )}

      {/* Ripple selected */}
      {selectedRipple && (
        <>
          <Section>
            <Label>Ripple: {selectedRipple.name}</Label>
            <div style={{ fontSize: 11, color: '#71717a', marginBottom: 8, lineHeight: 1.7 }}>
              <div>in <span style={{ color: '#a1a1aa' }}>{ponds[selectedRipple.pondId]?.name ?? selectedRipple.pondId}</span></div>
              <div>
                Runs: <span style={{ color: '#a1a1aa' }}>{rippleViews[selectedRipple.id]?.runsCompleted ?? 0}</span>
                <span style={{ color: '#52525b' }}> · </span>
                fresh <span style={{ color: '#a1a1aa' }}>{formatAge(rippleViews[selectedRipple.id]?.endF ?? 0, now)}</span> old
              </div>
            </div>

            {selectedRipple.parents.length > 0 && (
              <div style={{ marginBottom: 4 }}>
                <Label>Parents</Label>
                {selectedRipple.parents.map((pid) => (
                  <div key={pid} style={{ fontSize: 12, color: '#a1a1aa', marginBottom: 3 }}>{ripples[pid]?.name ?? pid}</div>
                ))}
              </div>
            )}
          </Section>

          {mobile && (
            <Section>
              <RunTrace points={rippleTrace(selectedPondRuns.slice(0, RUN_CAP), selectedRipple.name)} />
            </Section>
          )}
        </>
      )}

      {/* Options modal — the moved config sections (regular Ponds only; a Spout has its own lean panel). */}
      {selectedPond && !selectedPond.isSpout && !selectedRippleId && optionsOpen && (
        <PondOptionsModal pond={selectedPond} canControl={canControl} onClose={() => setOptionsOpen(false)} />
      )}
    </>
  );

  if (mobile) {
    return (
      <div
        className="ds-sidebar"
        style={{
          background: '#15151a',
          borderTop: '1px solid #27272a',
          flexShrink: 0,
          display: 'flex',
          flexDirection: 'column',
          maxHeight: '46dvh',
          color: '#e4e4e7',
          fontFamily: 'inherit',
        }}
      >
        <div
          onClick={() => setCollapsed((v) => !v)}
          style={{ display: 'flex', alignItems: 'center', padding: '8px 14px', cursor: 'pointer', userSelect: 'none', flexShrink: 0 }}
        >
          <span style={{ fontSize: 11, fontWeight: 700, color: '#a1a1aa', letterSpacing: '0.08em' }}>
            {collapsed ? '▸' : '▾'} CATCHMENT
            {headerContext && <span style={{ color: '#52525b', fontWeight: 400, marginLeft: 8 }}>{headerContext}</span>}
          </span>
        </div>
        {!collapsed && <div style={{ overflowY: 'auto', padding: '0 14px 14px' }}>{content}</div>}
      </div>
    );
  }

  return (
    <div
      className="ds-sidebar"
      style={{
        width: 320,
        minWidth: 320,
        background: '#15151a',
        borderLeft: '1px solid #27272a',
        padding: 18,
        display: 'flex',
        flexDirection: 'column',
        overflowY: 'auto',
        color: '#e4e4e7',
        fontFamily: 'inherit',
      }}
    >
      {content}
    </div>
  );
}
