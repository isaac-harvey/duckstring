'use client';

import { Fragment, useState } from 'react';
import { THEME_PULL, THEME_RUNNING } from '@/lib/store';

// Run-cadence + run-duration trace. Two lines share one y-axis so the run duration can be
// read against the cadence (≈ the bottleneck): well below it ⇒ this node isn't the bottleneck;
// close to it ⇒ it is. The y-axis is clipped near the 90th percentile (plus a margin
// proportional to the mean) so aberrant gaps — e.g. after a stop — don't squash the rest.
//
// Both series' markers are interactive. Hovering a duration marker shows the run's completion time +
// duration; hovering an interval marker shows the previous → this completion times + the gap. Clicking
// either selects that run (opening it in Run Detail). Symmetrically, `selectedKey` enlarges both markers
// of the selected run — a run with no marker here (a different Pond, or a no-duration pass) doesn't
// highlight. Intervals are indexed to the *later* run (the completion that ends the gap), so a run's
// duration and interval markers share its x-position and either one selects it.
const INTERVAL_COLOR = THEME_PULL; // run cadence, tied to Wave/pull (≈ bottleneck under a Wave)
const DURATION_COLOR = THEME_RUNNING; // run duration, tied to the running state

export type TracePointView = { key: string; time: number; duration: number };

type Hover = { i: number; kind: 'dur' | 'int' };

function quantile(sortedAsc: number[], q: number): number {
  if (sortedAsc.length === 0) return 0;
  const i = Math.floor(q * (sortedAsc.length - 1));
  return sortedAsc[i];
}

const clock = (msVal: number): string => {
  const d = new Date(msVal);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
};

export function TraceChart({
  points, selectedKey = null, onSelect,
}: {
  points: TracePointView[];
  selectedKey?: string | null;
  onSelect?: (key: string) => void;
}) {
  const [hover, setHover] = useState<Hover | null>(null);
  const W = 256;
  const H = 100;
  const padL = 6;
  const padR = 46;
  const padT = 10;
  const padB = 14;

  const times = points.map((p) => p.time);
  const durs = points.map((p) => p.duration / 1000); // one per run (s)
  // Interval (s) into run i from the previous completion — indexed to the later run (i ≥ 1).
  const interval = (i: number) => (times[i] - times[i - 1]) / 1000;
  const intervalVals: number[] = [];
  for (let i = 1; i < points.length; i++) intervalVals.push(interval(i));

  const header = (
    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: '#a1a1aa', marginBottom: 4 }}>
      <span style={{ color: INTERVAL_COLOR }}>● interval</span>
      <span style={{ color: DURATION_COLOR }}>● run dur</span>
      <span style={{ color: '#71717a' }}>(s)</span>
    </div>
  );

  if (points.length === 0) {
    return (
      <div>
        {header}
        <div style={{ fontSize: 11, color: '#52525b', padding: '28px 0', textAlign: 'center' }}>No completed runs yet.</div>
      </div>
    );
  }

  // Clip the y-axis: 90th percentile of all plotted values + margin ∝ mean.
  const all = [...intervalVals, ...durs];
  const sorted = [...all].sort((a, b) => a - b);
  const mean = all.reduce((a, b) => a + b, 0) / (all.length || 1);
  const yMax = Math.max(quantile(sorted, 0.9) + 0.4 * mean, 0.01);

  // Shared x-index by completion number (one point per run).
  const n = points.length;
  const x = (i: number) => (n <= 1 ? padL : padL + (i / (n - 1)) * (W - padL - padR));
  const y = (v: number) => padT + (1 - Math.min(v, yMax) / yMax) * (H - padT - padB);

  const meanLast3 = (vals: number[]) => {
    const last3 = vals.slice(-3);
    return last3.length ? last3.reduce((a, b) => a + b, 0) / last3.length : null;
  };
  const meanLine = (m: number | null, color: string) =>
    m == null ? null : (
      <>
        <line x1={padL} y1={y(m)} x2={W - padR} y2={y(m)} stroke={color} strokeWidth={1} strokeDasharray="4 3" />
        <text x={W - padR + 4} y={y(m) + 3} fontSize={10} fill={color} fontWeight={700}>{m.toFixed(1)}s</text>
      </>
    );

  const polyline = (coords: string[], color: string) =>
    coords.length > 1 ? <polyline points={coords.join(' ')} fill="none" stroke={color} strokeWidth={1.5} /> : null;
  const durLine = polyline(durs.map((v, i) => `${x(i)},${y(v)}`), DURATION_COLOR);
  const intLine = polyline(points.slice(1).map((_, k) => `${x(k + 1)},${y(interval(k + 1))}`), INTERVAL_COLOR);

  // Per-run markers for both series (a run's duration marker always; its interval marker for i ≥ 1).
  const marker = (i: number, kind: 'dur' | 'int', color: string) => {
    const cy = kind === 'dur' ? y(durs[i]) : y(interval(i));
    const active = selectedKey === points[i].key || (hover?.i === i && hover.kind === kind);
    return (
      <g key={kind}>
        <circle cx={x(i)} cy={cy} r={active ? 4 : 2} fill={color} stroke={active ? '#fff' : 'none'} strokeWidth={active ? 1 : 0} />
        <circle cx={x(i)} cy={cy} r={7} fill="transparent" style={{ cursor: onSelect ? 'pointer' : 'default' }}
                onMouseEnter={() => setHover({ i, kind })}
                onMouseLeave={() => setHover((h) => (h && h.i === i && h.kind === kind ? null : h))}
                onClick={onSelect ? () => onSelect(points[i].key) : undefined} />
      </g>
    );
  };

  const tip = hover && points[hover.i]
    ? (hover.kind === 'dur'
        ? { cx: x(hover.i), cy: y(durs[hover.i]), color: DURATION_COLOR,
            top: clock(times[hover.i]), val: `${durs[hover.i].toFixed(2)}s` }
        : { cx: x(hover.i), cy: y(interval(hover.i)), color: INTERVAL_COLOR,
            top: `${clock(times[hover.i - 1])} → ${clock(times[hover.i])}`, val: `${interval(hover.i).toFixed(2)}s` })
    : null;

  return (
    <div>
      {header}
      <div style={{ position: 'relative' }}>
        <svg width={W} height={H} style={{ display: 'block' }}>
          <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke="#27272a" strokeWidth={1} />
          {meanLine(meanLast3(intervalVals), INTERVAL_COLOR)}
          {meanLine(meanLast3(durs), DURATION_COLOR)}
          {durLine}
          {intLine}
          {points.map((p, i) => (
            <Fragment key={p.key}>
              {marker(i, 'dur', DURATION_COLOR)}
              {i >= 1 && marker(i, 'int', INTERVAL_COLOR)}
            </Fragment>
          ))}
        </svg>
        {tip && (
          <div
            style={{
              position: 'absolute', left: tip.cx, top: tip.cy, transform: 'translate(-50%, -118%)',
              pointerEvents: 'none', background: '#09090b', border: '1px solid #3f3f46', borderRadius: 5,
              padding: '3px 7px', fontSize: 10.5, lineHeight: 1.5, color: '#e4e4e7', whiteSpace: 'nowrap', zIndex: 5,
            }}
          >
            <div style={{ color: '#a1a1aa' }}>{tip.top}</div>
            <div style={{ color: tip.color, fontWeight: 700 }}>{tip.val}</div>
          </div>
        )}
      </div>
    </div>
  );
}
