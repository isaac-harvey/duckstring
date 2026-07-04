'use client';

import { Fragment, useEffect, useMemo, useRef, useState } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  Panel,
  BaseEdge,
  getStraightPath,
  useReactFlow,
  useUpdateNodeInternals,
  type NodeTypes,
  type EdgeTypes,
  type EdgeProps,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { useLiveStore, consumeEdgeColor, formatAge, THEME_PULL, THEME_PUSH, THEME_SUCCESS, THEME_DANGER } from '@/lib/store';
import { resetCatchment, type AccessLevel } from '@/lib/api';
import { computeLayout, statsLineWidth, type ContentFloors } from '@/lib/layout';
import { useIsMobile } from '@/lib/useIsMobile';
import { PondNode } from './PondNode';
import { RippleNode } from './RippleNode';
import { TriggerNode } from './TriggerNode';
import { CatchmentGroupNode } from './CatchmentGroupNode';
import { RemotePondNode } from './RemotePondNode';
import { AlertsMenu } from './AlertsMenu';
import { SecretsMenu } from './SecretsMenu';
import { ConfirmDialog, type ConfirmOpts } from './ConfirmDialog';

// ─── Custom edges (read-only; colour reflects the sink's demand) ─────────────

function RippleEdge({ id, sourceX, sourceY, targetX, targetY, data }: EdgeProps) {
  const { sourceRippleId, sinkRippleId } = data as { sourceRippleId: string; sinkRippleId: string };
  const parentEndF = useLiveStore((s) => s.rippleViews[sourceRippleId]?.endF ?? 0);
  const childStartF = useLiveStore((s) => s.rippleViews[sinkRippleId]?.startF ?? 0);
  const childTargetF = useLiveStore((s) => s.rippleViews[sinkRippleId]?.targetF ?? null);
  const color = consumeEdgeColor(parentEndF, childStartF, childTargetF);
  const [edgePath] = getStraightPath({ sourceX, sourceY, targetX, targetY });
  return <BaseEdge id={id} path={edgePath} interactionWidth={0} style={{ stroke: color, strokeWidth: 2 }} />;
}

function PondEdge({ id, sourceX, sourceY, targetX, targetY, data }: EdgeProps) {
  const { sourcePondId, sinkPondId } = data as { sourcePondId: string; sinkPondId: string };
  const parentEndF = useLiveStore((s) => s.pondViews[sourcePondId]?.endF ?? 0);
  const childStartF = useLiveStore((s) => s.pondViews[sinkPondId]?.startF ?? 0);
  const childTargetF = useLiveStore((s) => s.pondViews[sinkPondId]?.targetF ?? null);
  const color = consumeEdgeColor(parentEndF, childStartF, childTargetF);
  const [edgePath] = getStraightPath({ sourceX, sourceY, targetX, targetY });
  return <BaseEdge id={id} path={edgePath} interactionWidth={0} style={{ stroke: color, strokeWidth: 2 }} />;
}

function TriggerEdge({ id, sourceX, sourceY, targetX, targetY, data }: EdgeProps) {
  const pondId = (data as { pondId: string }).pondId;
  const trigger = useLiveStore((s) => s.triggers[pondId]);
  const color = trigger?.kind === 'wave' ? THEME_PULL : THEME_PUSH;
  const [edgePath] = getStraightPath({ sourceX, sourceY, targetX, targetY });
  return (
    <BaseEdge id={id} path={edgePath} interactionWidth={0} style={{ stroke: color, strokeWidth: 2, strokeDasharray: '6 3' }} />
  );
}

const nodeTypes: NodeTypes = {
  pond: PondNode as NodeTypes[string],
  ripple: RippleNode as NodeTypes[string],
  trigger: TriggerNode as NodeTypes[string],
  catchmentGroup: CatchmentGroupNode as NodeTypes[string],
  remotePond: RemotePondNode as NodeTypes[string],
};

const edgeTypes: EdgeTypes = {
  rippleEdge: RippleEdge as EdgeTypes[string],
  pondEdge: PondEdge as EdgeTypes[string],
  triggerEdge: TriggerEdge as EdgeTypes[string],
};

// ─── Status / legend panel ───────────────────────────────────────────────────

// The caller's API access level as three capabilities — Manage | Demand | Read — each prefixed with a
// green ✓ when the key grants it, a grey – when not. The labels stay white (the ✓/– carries the state);
// reads as "what your key can do", not a broken UI. Read is the floor (always on); Demand adds
// tap/wave/pulse/tide; Manage adds deploy/control/secrets.
const ACCESS_CHIPS: { label: string; active: (l: AccessLevel) => boolean; hint: string }[] = [
  { label: 'Manage', active: (l) => l === 'full', hint: 'Deploy, control, and manage secrets.' },
  { label: 'Demand', active: (l) => l === 'full' || l === 'demand', hint: 'Create demand: tap, wave, pulse, tide.' },
  { label: 'Read', active: () => true, hint: 'Read status and query data.' },
];

function AccessBadge() {
  const level = useLiveStore((s) => s.accessLevel);
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, letterSpacing: '0.02em' }}>
      {ACCESS_CHIPS.map(({ label, active, hint }, i) => {
        const on = active(level);
        return (
          <Fragment key={label}>
            {i > 0 && <span style={{ color: '#3f3f46' }}>|</span>}
            <span title={hint} style={{ display: 'inline-flex', alignItems: 'center', gap: 3, color: on ? '#e4e4e7' : '#3f3f46' }}>
              <span style={{ color: on ? THEME_SUCCESS : '#3f3f46' }}>{on ? '✓' : '–'}</span>
              {label}
            </span>
          </Fragment>
        );
      })}
    </div>
  );
}

// Top-left panel: the Duckstring brand mark (and natural home for catchment navigation later),
// over the live connection status.
function StatusPanel() {
  const connected = useLiveStore((s) => s.connected);
  const error = useLiveStore((s) => s.error);
  const count = useLiveStore((s) => Object.keys(s.ponds).length);
  const catchment = useLiveStore((s) => s.catchment);
  // This Catchment's display name, or a short slice of its stable id, or a plain label.
  const label = catchment?.name || (catchment?.id ? catchment.id.slice(0, 8) : 'Catchment');
  const isMobile = useIsMobile();

  // Mobile: one compact row — the full card would shade a third of a phone canvas.
  if (isMobile) {
    return (
      <div
        style={{
          background: '#15151a',
          border: '1px solid #27272a',
          borderRadius: 8,
          padding: '5px 10px',
          fontFamily: 'ui-monospace, SFMono-Regular, monospace',
          fontSize: 11,
          color: '#a1a1aa',
          display: 'flex',
          alignItems: 'center',
          gap: 8,
        }}
      >
        <span
          aria-label="Duckstring"
          style={{
            width: 22,
            height: 22,
            flexShrink: 0,
            backgroundImage: 'url(/logo-mark.svg)',
            backgroundSize: 'contain',
            backgroundRepeat: 'no-repeat',
            backgroundPosition: 'center',
          }}
        />
        <span style={{ fontSize: 13, fontWeight: 700, color: '#f4f4f5' }}>Duckstring</span>
        <span
          title={connected ? 'connected' : error ? 'unreachable' : 'connecting'}
          style={{ width: 8, height: 8, flexShrink: 0, borderRadius: '50%', background: connected ? THEME_SUCCESS : THEME_DANGER }}
        />
        <span style={{ color: '#71717a' }}>
          {connected ? `${count} pond${count === 1 ? '' : 's'}` : error ? 'unreachable' : 'connecting…'}
        </span>
        <span style={{ color: '#3f3f46' }}>·</span>
        <AccessBadge />
      </div>
    );
  }

  return (
    <div
      style={{
        background: '#15151a',
        border: '1px solid #27272a',
        borderRadius: 8,
        padding: '9px 12px',
        fontFamily: 'ui-monospace, SFMono-Regular, monospace',
        fontSize: 11,
        color: '#a1a1aa',
        display: 'flex',
        flexDirection: 'column',
        gap: 7,
        minWidth: 168,
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 11 }}>
        <span
          aria-label="Duckstring"
          style={{
            width: 39,
            height: 39,
            flexShrink: 0,
            backgroundImage: 'url(/logo-mark.svg)',
            backgroundSize: 'contain',
            backgroundRepeat: 'no-repeat',
            backgroundPosition: 'center',
          }}
        />
        <span style={{ fontSize: 17, fontWeight: 700, lineHeight: 1, color: '#f4f4f5', letterSpacing: '0.01em' }}>
          Duckstring
        </span>
      </div>
      <div style={{ height: 1, background: '#27272a' }} />
      <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
        <span
          title={connected ? 'connected' : error ? 'unreachable' : 'connecting'}
          style={{ width: 8, height: 8, flexShrink: 0, borderRadius: '50%', background: connected ? THEME_SUCCESS : THEME_DANGER }}
        />
        <span style={{ color: '#71717a' }}>
          <span title={catchment?.id ?? undefined} style={{ color: '#a1a1aa' }}>{label}</span>
          <span style={{ color: '#3f3f46' }}> · </span>
          {connected ? `${count} pond${count === 1 ? '' : 's'}` : error ? 'unreachable' : 'connecting…'}
        </span>
      </div>
      <AccessBadge />
    </div>
  );
}

// The shared look of a top-right panel button (collapse-all, Secrets) — same shape, colour, width.
const panelButton: React.CSSProperties = {
  background: '#15151a',
  border: '1px solid #27272a',
  borderRadius: 8,
  padding: '7px 12px',
  fontFamily: 'ui-monospace, SFMono-Regular, monospace',
  fontSize: 12,
  color: '#a1a1aa',
  cursor: 'pointer',
  display: 'flex',
  alignItems: 'center',
  gap: 7,
};

// Top-right controls, stacked: collapse every Pond's Ripples to a header-only box (label flips to
// "Expand all" once every collapsible Pond is collapsed), and — full access only — the catchment-wide
// secret store (SecretsMenu). The two buttons share a width; the menu drops below them.
function ControlsPanel() {
  const collapsedPonds = useLiveStore((s) => s.collapsedPonds);
  const setAllCollapsed = useLiveStore((s) => s.setAllCollapsed);
  const accessLevel = useLiveStore((s) => s.accessLevel);
  // A Pond is collapsible only if it owns Ripples (a Draw has none to hide). Select a stable joined
  // key — not a fresh array — so the panel doesn't re-render on every poll.
  const collapsibleKey = useLiveStore((s) =>
    [...new Set(Object.values(s.ripples).map((r) => r.pondId))].sort().join(',')
  );
  const [secretsOpen, setSecretsOpen] = useState(false);
  const [alertsOpen, setAlertsOpen] = useState(false);
  const [resetConfirm, setResetConfirm] = useState<ConfirmOpts | null>(null);
  const catchmentName = useLiveStore((s) => s.catchment?.name ?? null);

  const collapsibleIds = collapsibleKey ? collapsibleKey.split(',') : [];
  const allCollapsed = collapsibleIds.length > 0 && collapsibleIds.every((id) => collapsedPonds[id]);
  const isFull = accessLevel === 'full';
  if (collapsibleIds.length === 0 && !isFull) return null;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 7, alignItems: 'flex-end' }}>
      {/* A stretch column so the two buttons share the widest one's width. */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 7, alignItems: 'stretch' }}>
        {collapsibleIds.length > 0 && (
          <button onClick={() => setAllCollapsed(!allCollapsed)} style={panelButton}>
            <span style={{ fontSize: 11, color: '#71717a' }}>{allCollapsed ? '▸' : '▾'}</span>
            {allCollapsed ? 'Expand all' : 'Collapse all'}
          </button>
        )}
        {isFull && (
          <button
            onClick={() => { setSecretsOpen((o) => !o); setAlertsOpen(false); }}
            style={{ ...panelButton, justifyContent: 'center', color: secretsOpen ? '#e4e4e7' : '#a1a1aa' }}
          >
            Secrets
          </button>
        )}
        {isFull && (
          <button
            onClick={() => { setAlertsOpen((o) => !o); setSecretsOpen(false); }}
            style={{ ...panelButton, justifyContent: 'center', color: alertsOpen ? '#e4e4e7' : '#a1a1aa' }}
          >
            Alerts
          </button>
        )}
        {isFull && (
          <button
            title="Scrub every Pond's data + state to a fresh-deploy state (keeps deploys, config, secrets)"
            onClick={() => setResetConfirm({
              title: 'Reset Catchment?',
              body:
                'Delete all data and state (e.g. freshness) from all Ponds. ' +
                'All code, config, secrets etc. are retained.\n\n' +
                'This cannot be undone.',
              confirmLabel: 'Reset everything',
              requireTyped: catchmentName || 'reset all',
              action: async () => { await resetCatchment().catch(() => {}); },
            })}
            style={{ ...panelButton, justifyContent: 'center', color: THEME_DANGER, borderColor: `${THEME_DANGER}66` }}
          >
            Reset all
          </button>
        )}
      </div>
      {isFull && resetConfirm && <ConfirmDialog opts={resetConfirm} onClose={() => setResetConfirm(null)} />}
      {isFull && secretsOpen && <SecretsMenu onClose={() => setSecretsOpen(false)} />}
      {isFull && alertsOpen && <AlertsMenu onClose={() => setAlertsOpen(false)} />}
    </div>
  );
}

// ─── Main canvas ─────────────────────────────────────────────────────────────

export function DagCanvas() {
  const ponds = useLiveStore((s) => s.ponds);
  const ripples = useLiveStore((s) => s.ripples);
  const triggers = useLiveStore((s) => s.triggers);
  const pondViews = useLiveStore((s) => s.pondViews);
  const rippleViews = useLiveStore((s) => s.rippleViews);
  const now = useLiveStore((s) => s.now);
  const clearSelection = useLiveStore((s) => s.clearSelection);
  const selectedPondId = useLiveStore((s) => s.selectedPondId);
  const selectedRippleId = useLiveStore((s) => s.selectedRippleId);
  const selectedTriggerId = useLiveStore((s) => s.selectedTriggerId);

  // On mobile, tapping a node zooms to it — the clear "this is selected" signal, and the
  // only way the node text gets readable. The delay lets the bottom sheet open (the canvas
  // shrinks) before the viewport is fitted to the remaining space.
  const { fitView } = useReactFlow();
  const updateNodeInternals = useUpdateNodeInternals();
  const isMobile = useIsMobile();
  useEffect(() => {
    if (!isMobile) return;
    const id = selectedRippleId ?? (selectedTriggerId ? `trigger-${selectedTriggerId}` : selectedPondId);
    if (!id) return;
    const t = setTimeout(() => fitView({ nodes: [{ id }], duration: 350, padding: 0.15, maxZoom: 1.1 }), 120);
    return () => clearTimeout(t);
  }, [isMobile, selectedPondId, selectedRippleId, selectedTriggerId, fitView]);

  // Relayout only when graph structure changes — not on every poll.
  const layoutKey = useMemo(
    () =>
      JSON.stringify({
        ponds: Object.values(ponds).map((p) => ({ id: p.id, sources: p.sources, name: p.name })),
        ripples: Object.values(ripples).map((r) => ({ id: r.id, parents: r.parents, pondId: r.pondId, name: r.name })),
        triggers: Object.keys(triggers),
      }),
    [ponds, ripples, triggers]
  );

  // Each box's minimum width to fit its live stats line (grows with run counts / longer ages).
  const floors = useMemo<ContentFloors>(() => {
    const r: Record<string, number> = {};
    for (const rp of Object.values(ripples)) {
      const rs = rippleViews[rp.id];
      if (!rs) continue;
      const startedF = rs.status === 'running' ? rs.startF : rs.endF;
      r[rp.id] = statsLineWidth({
        pushAge: rs.targetF != null ? formatAge(rs.targetF, now) : null,
        startAge: formatAge(startedF, now),
        startCount: rs.runsStarted,
        endAge: formatAge(rs.endF, now),
        endCount: rs.runsCompleted,
        pad: 20,
      });
    }
    const p: Record<string, number> = {};
    for (const pd of Object.values(ponds)) {
      const ps = pondViews[pd.id];
      if (!ps) continue;
      p[pd.id] = statsLineWidth({
        pushAge: ps.targetF != null ? formatAge(ps.targetF, now) : null,
        startAge: formatAge(ps.startF, now),
        startCount: ps.runsStarted,
        endAge: formatAge(ps.endF, now),
        endCount: ps.runsCompleted,
        pad: 24,
      });
    }
    return { ripples: r, ponds: p };
  }, [ponds, ripples, pondViews, rippleViews, now]);

  // statsLineWidth buckets to 8px, so this key only changes when a box actually needs resizing —
  // keeping the (position-shifting) dagre relayout off the per-tick path.
  const widthKey = useMemo(() => {
    const enc = (m: Record<string, number>) =>
      Object.entries(m)
        .map(([k, v]) => `${k}:${v}`)
        .sort()
        .join(',');
    return `${enc(floors.ripples ?? {})}|${enc(floors.ponds ?? {})}`;
  }, [floors]);

  const collapsedPonds = useLiveStore((s) => s.collapsedPonds);
  // Stable key over just the *collapsed* pond ids — relayout fires when a Pond is collapsed/expanded,
  // not on every poll. The Set fed to computeLayout is rebuilt from it inside the layout memo.
  const collapsedKey = useMemo(
    () => Object.keys(collapsedPonds).filter((id) => collapsedPonds[id]).sort().join(','),
    [collapsedPonds]
  );

  const lineage = useLiveStore((s) => s.lineage);
  const selfId = useLiveStore((s) => s.catchment?.id ?? null);
  // Lineage layout changes only when the upstream topology does (not on every freshness tick) — key
  // off the catchment + pond ids, mirroring layoutKey for the local graph.
  const lineageKey = useMemo(() => {
    if (!lineage) return '';
    return lineage.catchments
      .map((c) => `${c.id}:${c.reachable}:${c.ponds.map((p) => p.id).join('+')}`)
      .join('|') + '#' + lineage.duct_edges.map((e) => `${e.from.catchment}.${e.from.pond}>${e.to.catchment}.${e.to.pond}`).join(',');
  }, [lineage]);

  const { nodes, edges } = useMemo(() => {
    const collapsed = new Set(collapsedKey ? collapsedKey.split(',') : []);
    return computeLayout(ponds, ripples, triggers, floors, isMobile ? 'TB' : 'LR', lineage, selfId, collapsed);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [layoutKey, widthKey, isMobile, lineageKey, selfId, collapsedKey]);

  // Handle positions move between the LR and TB layouts; nudge React Flow to re-measure them
  // when the orientation flips, or edges keep their old anchors. Then re-frame: the fitView prop
  // only fires at init, against the pre-hydration desktop layout (isMobile is false during SSR).
  useEffect(() => {
    updateNodeInternals(nodes.map((n) => n.id));
    const t = setTimeout(() => fitView({ padding: 0.15 }), 100);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isMobile]);

  // Re-frame when the *set* of Ponds changes (one deployed or removed). The controlled viewport
  // otherwise stays put, which can leave a newly-orphaned downstream Pond off-screen after its
  // source is removed. Keyed on Pond ids only, so collapse/expand (a ripple-visibility change)
  // doesn't trigger it; the initial mount is framed by the isMobile effect above.
  const pondIdKey = useMemo(() => Object.keys(ponds).sort().join(','), [ponds]);
  const framedOnce = useRef(false);
  useEffect(() => {
    if (!framedOnce.current) { framedOnce.current = true; return; }
    const t = setTimeout(() => fitView({ padding: 0.15, duration: 350 }), 120);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pondIdKey]);

  // React Flow controlled mode requires these handlers; layout is managed externally, so no-op.
  const onNodesChange = () => {};
  const onEdgesChange = () => {};

  return (
    <div style={{ width: '100%', height: '100%', background: '#0f0f14' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onPaneClick={clearSelection}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodesDraggable={false}
        nodesConnectable={false}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.2}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
        style={{ background: '#0f0f14' }}
      >
        <Background color="#2a2a35" gap={24} size={1} />
        <Panel position="top-left">
          <StatusPanel />
        </Panel>
        <Panel position="top-right">
          <ControlsPanel />
        </Panel>
        <Controls style={{ background: '#1a1a1f', border: '1px solid #3f3f46', borderRadius: 6 }} />
      </ReactFlow>
    </div>
  );
}
