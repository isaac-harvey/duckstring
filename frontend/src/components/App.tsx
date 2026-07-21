'use client';

import { useEffect, useState } from 'react';
import { ReactFlowProvider } from '@xyflow/react';
import { useLiveStore } from '@/lib/store';
import { BATCH_OPS, type BatchOp } from '@/lib/api';
import { useIsMobile } from '@/lib/useIsMobile';
import { DagCanvas } from './DagCanvas';
import { Sidebar } from './Sidebar';
import { RunHistory } from './RunHistory';
import { RunDetail } from './RunDetail';
import { DataViewerModal } from './DataViewerModal';
import { ConfirmDialog } from './ConfirmDialog';
import { TopBar } from './OptionsMenu';

const POLL_MS = 1000;

// Shown when the Catchment answers 401 (it was started with --key / --generate-key). The key is
// kept in localStorage and sent as a Bearer header on every request; a wrong key re-raises 401 so
// the prompt simply stays up.
function KeyPrompt() {
  const submitApiKey = useLiveStore((s) => s.submitApiKey);
  const [value, setValue] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!value.trim() || busy) return;
    setBusy(true);
    try {
      await submitApiKey(value);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 1000, display: 'flex', alignItems: 'center',
      justifyContent: 'center', background: 'rgba(9, 9, 11, 0.85)', backdropFilter: 'blur(2px)',
    }}>
      <div style={{
        background: '#101014', border: '1px solid #27272a', borderRadius: 8, padding: '22px 26px',
        width: 360, maxWidth: '90vw', display: 'flex', flexDirection: 'column', gap: 12,
      }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#e4e4e7' }}>API key required</div>
        <div style={{ fontSize: 12, color: '#a1a1aa', lineHeight: 1.5 }}>
          This Catchment was started with an API key. Enter it to connect; it is stored in this
          browser only.
        </div>
        <input
          type="password"
          autoFocus
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') void submit(); }}
          placeholder="API key"
          style={{
            background: '#18181b', border: '1px solid #3f3f46', borderRadius: 5, padding: '8px 10px',
            color: '#e4e4e7', fontSize: 13, fontFamily: 'inherit', outline: 'none',
          }}
        />
        <button
          onClick={() => void submit()}
          disabled={!value.trim() || busy}
          style={{
            background: '#06c4e6', border: 'none', borderRadius: 5, padding: '8px 10px',
            color: '#09090b', fontSize: 13, fontWeight: 700, cursor: 'pointer',
            opacity: !value.trim() || busy ? 0.5 : 1, fontFamily: 'inherit',
          }}
        >
          {busy ? 'Connecting…' : 'Connect'}
        </button>
      </div>
    </div>
  );
}

// Mobile: the run panels collapse into a bottom sheet (RunHistory stacked over RunDetail)
// behind a header bar, so the canvas keeps the screen until runs are wanted.
function MobileRunsPanel() {
  const [open, setOpen] = useState(false);
  const selectedRun = useLiveStore((s) => s.selectedRun);
  return (
    <div style={{ borderTop: '1px solid #27272a', background: '#0c0c10', flexShrink: 0, display: 'flex', flexDirection: 'column' }}>
      <div
        onClick={() => setOpen((v) => !v)}
        style={{ display: 'flex', alignItems: 'center', padding: '8px 14px', cursor: 'pointer', userSelect: 'none', flexShrink: 0 }}
      >
        <span style={{ fontSize: 11, fontWeight: 700, color: '#a1a1aa', letterSpacing: '0.08em' }}>
          {open ? '▾' : '▸'} RUNS
        </span>
      </div>
      {open && (
        // Fixed height so both sheets open can't push past a short phone's screen;
        // history and detail split it evenly (each scrolls internally).
        <div style={{ display: 'flex', flexDirection: 'column', height: '40dvh', minHeight: 0 }}>
          <div style={{ flex: 1, minHeight: 0, borderTop: '1px solid #27272a' }}>
            <RunHistory />
          </div>
          {selectedRun && (
            <div style={{ flex: 1, minHeight: 0, borderTop: '1px solid #27272a' }}>
              <RunDetail />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Cyan is the default accent for the Selector (the Duckstring brand colour, used wherever there is no
// semantic reason for another). Destructive/irreversible ops keep red — that reason exists.
const BRAND = '#06c4e6';

// Operation metadata: label + a short blurb (shown on the button's second line so an unfamiliar operator
// knows what each does), `warn` (destructive → red accent) and `irreversible` (needs the typed confirm).
const OP_META: Record<BatchOp, { label: string; blurb: string; warn?: boolean; irreversible?: boolean }> = {
  kill: { label: 'Kill', blurb: 'Stop The Duck', warn: true },
  sleep: { label: 'Sleep', blurb: 'Cancel Demand' },
  reset: { label: 'Reset', blurb: 'Erase Data & State', warn: true, irreversible: true },
  wipe: { label: 'Wipe', blurb: 'Clear Run History', warn: true, irreversible: true },
  remove: { label: 'Remove', blurb: 'Retire The Pond', warn: true, irreversible: true },
  clear: { label: 'Clear', blurb: 'Clear Failures' },
  repair: { label: 'Repair', blurb: 'Rebuild Now' },
  refresh: { label: 'Refresh', blurb: 'Rebuild Next Run' },
};

const bannerBtn = (bg: string, disabled = false): React.CSSProperties => ({
  background: bg, border: 'none', borderRadius: 5, padding: '6px 12px', color: '#0a0a0a',
  fontSize: 12, fontWeight: 700, cursor: disabled ? 'default' : 'pointer', fontFamily: 'inherit',
  opacity: disabled ? 0.5 : 1,
});

// The Selector toolbar — a two-phase top banner (Pond Actions). Phase 1: pick a set of Ponds on the
// canvas (All / Tree / Between / Downstream / Clear). Phase 2: pick the operations to apply; irreversible
// ones (reset/wipe/remove) gate behind typing the catchment name. The server applies them in precedence
// order and returns per-Pond errors (surfaced here).
function SelectorBanner() {
  const mode = useLiveStore((s) => s.selectorMode);
  const phase = useLiveStore((s) => s.selectorPhase);
  const scope = useLiveStore((s) => s.selectorScope);
  const ops = useLiveStore((s) => s.selectorOps);
  const error = useLiveStore((s) => s.selectorError);
  const ponds = useLiveStore((s) => s.ponds);
  const pondInfo = useLiveStore((s) => s.pondInfo);
  const catchmentName = useLiveStore((s) => s.catchment?.name ?? null);
  const selectAll = useLiveStore((s) => s.selectAll);
  const selectTree = useLiveStore((s) => s.selectTree);
  const selectBetween = useLiveStore((s) => s.selectBetween);
  const addDownstream = useLiveStore((s) => s.addDownstream);
  const clearScope = useLiveStore((s) => s.clearSelectorScope);
  const setPhase = useLiveStore((s) => s.setSelectorPhase);
  const toggleOp = useLiveStore((s) => s.toggleOp);
  const submitBatch = useLiveStore((s) => s.submitBatch);
  const cancel = useLiveStore((s) => s.exitSelector);
  const [confirming, setConfirming] = useState(false);
  const isMobile = useIsMobile();

  if (!mode) return null;

  const ordered = BATCH_OPS.filter((o) => ops.includes(o));
  const needsConfirm = ordered.some((o) => OP_META[o].irreversible);

  const apply = () => {
    if (ordered.length === 0) return;
    if (needsConfirm) setConfirming(true);
    else void submitBatch(null);
  };

  const opDisabled = (op: BatchOp): boolean => {
    if (op === 'repair') return ops.includes('remove') || ops.includes('reset');
    if (op === 'reset') return ops.includes('remove'); // locked on by Remove
    return false;
  };

  return (
    <div style={{
      display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 10, padding: '8px 14px',
      background: '#06141a', borderBottom: `1px solid ${BRAND}`, color: '#7dd3e8', fontSize: 12,
    }}>
      <span style={{ fontWeight: 700 }}>Pond Actions</span>

      {phase === 'select' ? (
        <>
          <span style={{ color: '#a1a1aa' }}>
            Click Ponds, or use a helper · <b style={{ color: '#e4e4e7' }}>{scope.length}</b> selected
          </span>
          <div style={{ display: 'flex', gap: 6 }}>
            <button style={bannerBtn('#52525b')} onClick={() => selectAll()}>All</button>
            <button style={bannerBtn('#52525b', scope.length === 0)} disabled={scope.length === 0} onClick={() => selectTree()}>Tree</button>
            <button style={bannerBtn('#52525b', scope.length === 0)} disabled={scope.length === 0} onClick={() => selectBetween()}>Between</button>
            <button style={bannerBtn('#52525b', scope.length === 0)} disabled={scope.length === 0} onClick={() => addDownstream()}>Downstream</button>
            <button style={bannerBtn('#52525b', scope.length === 0)} disabled={scope.length === 0} onClick={() => clearScope()}>Clear</button>
          </div>
          {error && <span style={{ color: '#ef4444' }}>{error}</span>}
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
            <button style={bannerBtn(BRAND, scope.length === 0)} disabled={scope.length === 0} onClick={() => setPhase('ops')}>
              Choose actions →
            </button>
            <button style={bannerBtn('#71717a')} onClick={() => cancel()}>Cancel</button>
          </div>
        </>
      ) : (
        <>
          <span style={{ color: '#a1a1aa' }}>
            Apply to <b style={{ color: '#e4e4e7' }}>{scope.length}</b> Pond{scope.length === 1 ? '' : 's'} ·
          </span>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, width: isMobile ? '100%' : undefined, flexDirection: isMobile ? 'column' : 'row' }}>
            {BATCH_OPS.map((op) => {
              const on = ops.includes(op);
              const disabled = opDisabled(op);
              const m = OP_META[op];
              const accent = on ? (m.warn ? '#ef4444' : BRAND) : '#3f3f46';
              const muted = disabled && !on; // precluded by another selection — clearly greyed out
              const lockedOn = disabled && on; // auto-selected (Reset under Remove)
              return (
                <button
                  key={op}
                  onClick={() => !disabled && toggleOp(op)}
                  disabled={disabled}
                  title={lockedOn ? 'Auto-selected — implied by another action' : disabled ? 'Not available with the current selection' : undefined}
                  style={{
                    // Mobile: one wide row per action, single line "Label: Blurb". Desktop: compact
                    // two-line chips that wrap.
                    display: 'flex', flexDirection: isMobile ? 'row' : 'column',
                    alignItems: isMobile ? 'baseline' : 'flex-start', gap: isMobile ? 6 : 1,
                    width: isMobile ? '100%' : undefined, boxSizing: 'border-box',
                    padding: isMobile ? '7px 11px' : '3px 9px', borderRadius: 5, fontFamily: 'inherit', textAlign: 'left',
                    cursor: disabled ? (lockedOn ? 'default' : 'not-allowed') : 'pointer',
                    border: `1px solid ${muted ? '#27272a' : accent}`,
                    color: on ? accent : muted ? '#52525b' : '#a1a1aa',
                    background: on ? (m.warn ? '#ef444414' : '#06c4e614') : muted ? '#18181b55' : 'transparent',
                    opacity: muted ? 0.5 : 1,
                  }}
                >
                  <span style={{ fontSize: isMobile ? 13 : 11, fontWeight: 600 }}>{m.label}{lockedOn ? ' 🔒' : ''}{isMobile ? ':' : ''}</span>
                  <span style={{ fontSize: isMobile ? 12 : 9, color: on ? accent : muted ? '#3f3f46' : '#71717a', opacity: 0.9 }}>{m.blurb}</span>
                </button>
              );
            })}
          </div>
          {error && <span style={{ color: '#ef4444' }}>{error}</span>}
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
            <button style={bannerBtn('#52525b')} onClick={() => setPhase('select')}>← Back</button>
            <button style={bannerBtn(BRAND, ordered.length === 0)} disabled={ordered.length === 0} onClick={apply}>
              Apply {ordered.length > 0 ? `(${ordered.map((o) => OP_META[o].label).join(', ')})` : ''}
            </button>
            <button style={bannerBtn('#71717a')} onClick={() => cancel()}>Cancel</button>
          </div>
          {confirming && (
            <ConfirmDialog
              opts={{
                title: 'Confirm irreversible actions',
                body:
                  `Apply ${ordered.map((o) => OP_META[o].label).join(', ')} to these ${scope.length} ` +
                  `Pond${scope.length === 1 ? '' : 's'} — includes irreversible actions that cannot be undone:`,
                details: (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    {[...scope].sort().map((id) => {
                      const p = ponds[id];
                      const version = pondInfo[id]?.version ?? id.split('@')[1];
                      return (
                        <div key={id} style={{ fontSize: 12, color: '#d4d4d8', whiteSpace: 'nowrap' }}>
                          {p ? `${p.name}@${version}` : id}
                        </div>
                      );
                    })}
                  </div>
                ),
                confirmLabel: 'Apply',
                requireTyped: catchmentName || 'confirm',
                action: async () => { await submitBatch(catchmentName || 'confirm'); },
              }}
              onClose={() => setConfirming(false)}
            />
          )}
        </>
      )}
    </div>
  );
}

export function App() {
  const refresh = useLiveStore((s) => s.refresh);
  const needsKey = useLiveStore((s) => s.needsKey);
  const isMobile = useIsMobile();

  useEffect(() => {
    // Long-poll loop: refresh() holds (via /api/status?since=) until the engine state changes, then
    // resolves at once, so the UI tracks state changes instantly rather than on a fixed timer. A short
    // gap between iterations (longer while disconnected) avoids a hot loop on instant returns/errors.
    let alive = true;
    (async () => {
      while (alive) {
        await refresh();
        if (!alive) break;
        await new Promise((r) => setTimeout(r, useLiveStore.getState().connected ? 100 : POLL_MS));
      }
    })();
    // A separate lightweight clock so freshness "age" keeps counting up between state changes.
    const clock = setInterval(() => useLiveStore.setState({ now: Date.now() }), POLL_MS);
    return () => {
      alive = false;
      clearInterval(clock);
    };
  }, [refresh]);

  return (
    <div className="ds-app" style={{ display: 'flex', flexDirection: 'column', width: '100%', overflow: 'hidden', fontFamily: 'ui-monospace, SFMono-Regular, monospace' }}>
      {needsKey && <KeyPrompt />}
      {/* One modal serves both the Catalog (Options → Catalog) and the per-Pond Data Viewer (table icon). */}
      <DataViewerModal />
      {/* Mobile: the TopBar is a full-width page header pinned above everything (desktop floats it on the canvas). */}
      {isMobile && <TopBar />}
      <SelectorBanner />
      {/* On mobile the sidebar drops below the canvas as a collapsible bottom sheet. */}
      <div style={{ display: 'flex', flexDirection: isMobile ? 'column' : 'row', flex: 1, minHeight: 0 }}>
        <ReactFlowProvider>
          <div style={{ flex: 1, minWidth: 0, minHeight: 0 }}>
            <DagCanvas />
          </div>
        </ReactFlowProvider>
        <Sidebar mobile={isMobile} />
      </div>
      {isMobile ? (
        <MobileRunsPanel />
      ) : (
        <div style={{ display: 'flex', height: 260, minHeight: 0, borderTop: '1px solid #27272a' }}>
          <div style={{ flex: 1, minWidth: 0, borderRight: '1px solid #27272a' }}>
            <RunHistory />
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <RunDetail />
          </div>
        </div>
      )}
    </div>
  );
}
