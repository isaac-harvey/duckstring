'use client';

import { useLiveStore, parseTs, THEME_PULL, THEME_DANGER } from '@/lib/store';
import type { PondRun, RippleRun } from '@/lib/types';
import { clock, durationOf, STATUS_COLOR } from './RunHistory';

interface RunError {
  source: string;
  message: string;
  traceback: string | null;
}

// One failure, attributed to its source (a Ripple by name, or a Pond-level cause), with its traceback.
function ErrorBox({ source, message, traceback }: RunError) {
  return (
    <div
      style={{
        marginBottom: 6,
        padding: '6px 8px',
        background: `${THEME_DANGER}12`,
        border: `1px solid ${THEME_DANGER}40`,
        borderRadius: 4,
        fontSize: 11,
        lineHeight: 1.5,
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
      }}
    >
      <div>
        <span style={{ color: THEME_DANGER, fontWeight: 700 }}>{source}</span>
        <span style={{ color: '#52525b' }}> · </span>
        <span style={{ color: '#fca5a5' }}>{message}</span>
      </div>
      {traceback && (
        <pre
          style={{
            margin: '6px 0 0',
            padding: '6px 8px',
            background: '#000',
            borderRadius: 3,
            color: '#9ca3af',
            fontSize: 10,
            lineHeight: 1.45,
            overflowX: 'auto',
            whiteSpace: 'pre',
          }}
        >
          {traceback}
        </pre>
      )}
    </div>
  );
}

// Errors for a run, attributed to their source. Ripple errors win (one per failed attempt, named);
// only a genuine Pond-level failure (no Ripple errored) shows the Pond Run's own error.
function runErrors(run: PondRun): RunError[] {
  const rippleErrs = (run.ripples ?? [])
    .filter((r) => r.error)
    .map((r) => ({
      source: r.retry > 0 ? `${r.ripple} ↻${r.retry}` : r.ripple,
      message: r.error as string,
      traceback: r.traceback,
    }));
  if (rippleErrs.length > 0) return rippleErrs;
  return run.error ? [{ source: 'Pond', message: run.error, traceback: run.traceback }] : [];
}

function StatusPill({ status }: { status: string }) {
  const c = STATUS_COLOR[status] ?? '#71717a';
  return (
    <span style={{ color: c, border: `1px solid ${c}`, background: `${c}1a`, borderRadius: 4, padding: '1px 8px', fontSize: 11, fontWeight: 700, letterSpacing: '0.04em' }}>
      {status}
    </span>
  );
}

function PersistField({ p }: { p: NonNullable<import('../lib/types').PondRun['persist']> }) {
  const ok = p.status === 'success';
  const dur = durationOf(p.startedAt, p.finishedAt);
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <span style={{ fontSize: 9, fontWeight: 700, color: '#52525b', letterSpacing: '0.1em', textTransform: 'uppercase' }}>
        Persisted
      </span>
      <span style={{ fontSize: 12, color: ok ? '#4ade80' : '#f87171' }}>
        {ok ? `✓ ${dur || 'done'}` : `✗ ${p.error || 'failed'}`}
      </span>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <span style={{ fontSize: 9, fontWeight: 700, color: '#52525b', letterSpacing: '0.1em', textTransform: 'uppercase' }}>{label}</span>
      <span style={{ fontSize: 12, color: '#d4d4d8' }}>{value}</span>
    </div>
  );
}

// Freshness F as a date+time (it's the Run's identity; may be in the future for windowed Inlets).
function freshness(iso: string): string {
  const d = new Date(parseTs(iso));
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function RippleLine({ r, isRetry }: { r: RippleRun; isRetry: boolean }) {
  const c = STATUS_COLOR[r.status] ?? '#71717a';
  const dur = durationOf(r.startedAt, r.finishedAt);
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '3px 0', paddingLeft: isRetry ? 16 : 0, borderBottom: '1px solid #161619' }}>
      <span style={{ width: 7, height: 7, borderRadius: '50%', background: c, flexShrink: 0 }} />
      <span style={{ flex: 1, minWidth: 0, color: isRetry ? '#a1a1aa' : '#d4d4d8', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {isRetry ? <span style={{ color: THEME_PULL }}>{`↻${r.retry} `}</span> : null}{r.ripple}
      </span>
      <span style={{ color: c, fontSize: 11 }}>{r.status}</span>
      <span style={{ color: '#52525b', fontSize: 11, width: 48, textAlign: 'right' }}>{dur || '—'}</span>
      <span style={{ color: '#3f3f46', fontSize: 11, width: 64, textAlign: 'right' }}>{clock(r.finishedAt ?? r.startedAt)}</span>
    </div>
  );
}

function Empty() {
  return (
    <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 20 }}>
      <span style={{ color: '#52525b', fontSize: 12, textAlign: 'center', lineHeight: 1.6 }}>
        Select a run on the left to inspect its Ripples and outcome.
      </span>
    </div>
  );
}

export function RunDetail() {
  const run: PondRun | null = useLiveStore((s) => s.selectedRun);

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: '#0c0c10', fontFamily: 'ui-monospace, SFMono-Regular, monospace', minWidth: 0 }}>
      <div style={{ display: 'flex', alignItems: 'center', height: 30, boxSizing: 'border-box', padding: '0 12px', borderBottom: '1px solid #18181d', fontSize: 11, fontWeight: 700, color: '#a1a1aa', letterSpacing: '0.08em' }}>
        RUN DETAIL
      </div>

      {!run ? (
        <Empty />
      ) : (
        <div style={{ flex: 1, overflowY: 'auto', padding: '12px 14px', fontSize: 12, color: '#e4e4e7' }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 12 }}>
            <span style={{ fontSize: 14, fontWeight: 700, color: '#e4e4e7' }}>{run.pond}</span>
            <span style={{ fontSize: 11, color: '#52525b' }}>v{run.version}</span>
            <span style={{ flex: 1 }} />
            <StatusPill status={run.status} />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px 16px', marginBottom: 14 }}>
            <Field label="Freshness" value={freshness(run.f)} />
            <Field label="Duration" value={durationOf(run.startedAt, run.finishedAt) || '—'} />
            <Field label="Started" value={clock(run.startedAt)} />
            <Field label="Finished" value={clock(run.finishedAt)} />
            {/* The Persist — the async mirror to the durable plane, its own log item landing after the
                run (which closes at local publish). Absent field = no cloud / direct-to-plane Duck. */}
            {run.persist !== undefined && run.persist !== null && (
              <PersistField p={run.persist} />
            )}
          </div>

          <div style={{ fontSize: 9, fontWeight: 700, color: '#52525b', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 4 }}>
            Ripples
          </div>
          {run.ripples === undefined ? (
            <div style={{ color: '#52525b', fontSize: 11 }}>Loading…</div>
          ) : run.ripples.length === 0 ? (
            <div style={{ color: '#52525b', fontSize: 11 }}>No Ripple detail recorded for this Run.</div>
          ) : (
            run.ripples.map((r) => (
              <RippleLine key={`${r.ripple}-${r.retry}`} r={r} isRetry={r.retry > 0} />
            ))
          )}

          {(() => {
            const errors = runErrors(run);
            return errors.length > 0 ? (
              <div style={{ marginTop: 14 }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#52525b', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 4 }}>
                  {errors.length > 1 ? 'Errors' : 'Error'}
                </div>
                {errors.map((e, i) => (
                  <ErrorBox key={i} source={e.source} message={e.message} traceback={e.traceback} />
                ))}
              </div>
            ) : null;
          })()}
        </div>
      )}
    </div>
  );
}
