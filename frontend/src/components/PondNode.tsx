'use client';

import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { useLiveStore, formatAge, stateColor, nodeFill, THEME_BRAND } from '@/lib/store';
import { DemandIndicators } from './DemandIndicators';

// A small grid/table glyph — the affordance to open this Pond's data viewer (shown when it has tables).
function TableIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
      <rect x="1.5" y="2.5" width="13" height="11" rx="1.5" />
      <line x1="1.5" y1="6.5" x2="14.5" y2="6.5" />
      <line x1="6" y1="6.5" x2="6" y2="13.5" />
    </svg>
  );
}

export const PondNode = memo(function PondNode({ data }: NodeProps) {
  const pondId = data.pondId as string;
  // TB (mobile) layout: flow enters at the top and leaves at the bottom; the trigger pill
  // hangs below the outlet, so its edge also lands on the bottom.
  const vertical = data.vertical as boolean | undefined;
  const pond = useLiveStore((s) => s.ponds[pondId]);
  const view = useLiveStore((s) => s.pondViews[pondId]);
  const info = useLiveStore((s) => s.pondInfo[pondId]);
  const selectedPondId = useLiveStore((s) => s.selectedPondId);
  const selectPond = useLiveStore((s) => s.selectPond);
  const selectorActive = useLiveStore((s) => s.selectorMode);
  const selecting = useLiveStore((s) => s.selectorMode && s.selectorPhase === 'select');
  const inScope = useLiveStore((s) => s.selectorScope.includes(pondId));
  const toggleSelect = useLiveStore((s) => s.toggleSelect);
  const refreshPending = useLiveStore((s) => s.pondInfo[pondId]?.refreshPending ?? false);
  const collapsed = useLiveStore((s) => !!s.collapsedPonds[pondId]);
  const toggleCollapse = useLiveStore((s) => s.toggleCollapse);
  // Only a Pond that owns Ripples can collapse — a Draw has nothing to hide, so it shows no caret.
  const hasRipples = useLiveStore((s) => Object.values(s.ripples).some((r) => r.pondId === pondId));
  const hasTables = useLiveStore((s) => s.pondInfo[pondId]?.hasTables ?? false);
  const hasObjects = useLiveStore((s) => s.pondInfo[pondId]?.hasObjects ?? false);
  const openDataViewer = useLiveStore((s) => s.openDataViewer);
  const now = useLiveStore((s) => s.now);

  if (!pond || !view) return null;

  const borderColor = stateColor(view);
  const isSelected = selectedPondId === pondId;
  // A Spout's pond name is "{source}#{spout}" — show just the spout part on the node.
  const displayName = pond.isSpout ? pond.name.split('#').slice(1).join('#') || pond.name : pond.name;
  // Draws and Spouts both cross the Catchment boundary → dashed (ingress vs egress).
  const boundary = pond.isDraw || pond.isSpout;
  // In selector mode, a selected Pond keeps a bright cyan ring through BOTH phases — so during
  // "choose actions" the operator can still see exactly which Ponds the actions will hit. Clicking
  // toggles the scope only in the select phase; in the ops phase a canvas click is inert.
  const highlighted = selectorActive && inScope;

  return (
    <div
      onClick={(e) => {
        e.stopPropagation();
        if (selecting) toggleSelect(pondId);
        else if (!selectorActive) selectPond(pondId);
      }}
      style={{
        width: '100%',
        height: '100%',
        // Draws (ingress) and Spouts (egress) cross the Catchment boundary → dashed, vs a solid Pond.
        border: `2px ${boundary ? 'dashed' : 'solid'} ${borderColor}`,
        boxShadow: highlighted
          ? `0 0 0 3px ${THEME_BRAND}`
          : refreshPending
            ? `0 0 0 2px #ee9333aa`  // pending refresh — an amber hint
            : isSelected ? `0 0 0 3px ${borderColor}40` : undefined,
        borderRadius: 10,
        background: nodeFill(borderColor),
        cursor: 'pointer',
        position: 'relative',
        boxSizing: 'border-box',
        userSelect: 'none',
      }}
    >
      <Handle id="in" type="target" position={vertical ? Position.Top : Position.Left} style={{ background: '#52525b' }} />
      <Handle
        id="trigger-in"
        type="target"
        position={vertical ? Position.Bottom : Position.Right}
        style={{ background: '#52525b', opacity: 0 }}
      />

      {(hasTables || hasObjects) && (
        <span
          role="button"
          title="View data"
          onClick={(e) => {
            e.stopPropagation();
            openDataViewer(pondId);
          }}
          style={{
            position: 'absolute', top: 7, right: 9, zIndex: 1, lineHeight: 0, padding: 3, borderRadius: 5,
            cursor: 'pointer', color: borderColor, background: '#0f0f14cc', opacity: 0.9,
          }}
        >
          <TableIcon />
        </span>
      )}

      <div
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          height: 64,
          padding: '6px 12px',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          gap: 5,
          // No divider when there's no ripple area beneath to separate — a Draw/Spout, or a collapsed Pond.
          borderBottom: boundary || collapsed ? 'none' : `1px solid ${borderColor}30`,
        }}
      >
        <span style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
          {hasRipples && (
            <span
              role="button"
              title={collapsed ? 'Expand ripples' : 'Collapse ripples'}
              onClick={(e) => {
                e.stopPropagation();
                toggleCollapse(pondId);
              }}
              style={{ fontSize: 10, color: '#a1a1aa', cursor: 'pointer', flexShrink: 0, userSelect: 'none' }}
            >
              {collapsed ? '▸' : '▾'}
            </span>
          )}
          {(pond.isDraw || pond.isSpout) && (
            <span style={{ fontSize: 10, fontWeight: 700, color: borderColor, letterSpacing: '0.06em', flexShrink: 0 }}>
              {pond.isDraw ? '[DRAW]' : '[SPOUT]'}
            </span>
          )}
          {pond.dbt && (
            <span
              title="dbt-mode Pond — its Ripples are dbt models"
              style={{
                fontSize: 9, fontWeight: 800, color: '#ff694b', letterSpacing: '0.02em', flexShrink: 0,
                border: '1px solid #ff694b', borderRadius: 3, padding: '0 3px', lineHeight: 1.6, alignSelf: 'center',
              }}
            >
              dbt
            </span>
          )}
          <span style={{ fontSize: 13, fontWeight: 700, color: '#e4e4e7', letterSpacing: '0.04em', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {displayName}
          </span>
          {info && <span style={{ fontSize: 10, color: '#52525b', whiteSpace: 'nowrap' }}>v{info.version}</span>}
        </span>
        <span style={{ fontSize: 11, color: '#71717a', display: 'flex', alignItems: 'center', gap: 6 }}>
          <DemandIndicators hasPull={view.hasPull} targetF={view.targetF} now={now} />
          <span style={{ color: '#a1a1aa' }}>↑{formatAge(view.startF, now)} ({view.runsStarted})</span>
          <span>✓{formatAge(view.endF, now)} ({view.runsCompleted})</span>
        </span>
      </div>

      <Handle id="out" type="source" position={vertical ? Position.Bottom : Position.Right} style={{ background: '#52525b' }} />
    </div>
  );
});
