'use client';

import { useEffect, useMemo, useRef } from 'react';
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

import { useLiveStore, consumeEdgeColor, formatAge, THEME_PULL, THEME_PUSH } from '@/lib/store';
import { computeLayout, statsLineWidth, type ContentFloors } from '@/lib/layout';
import { useIsMobile } from '@/lib/useIsMobile';
import { PondNode } from './PondNode';
import { RippleNode } from './RippleNode';
import { TriggerNode } from './TriggerNode';
import { CatchmentGroupNode } from './CatchmentGroupNode';
import { RemotePondNode } from './RemotePondNode';
import { TopBar } from './OptionsMenu';

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
        {/* On mobile the TopBar is a full-width page header rendered above the canvas (in App); on
            desktop it floats as a top-left panel over the canvas. */}
        {!isMobile && (
          <Panel position="top-left">
            <TopBar />
          </Panel>
        )}
        <Controls style={{ background: '#1a1a1f', border: '1px solid #3f3f46', borderRadius: 6 }} />
      </ReactFlow>
    </div>
  );
}
