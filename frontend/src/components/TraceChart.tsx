'use client';

import { useState } from 'react';
import { THEME_PULL, THEME_RUNNING } from '@/lib/store';

// Run-cadence + run-duration trace. Two lines share one y-axis so the run duration can be
// read against the cadence (≈ the bottleneck): well below it ⇒ this node isn't the bottleneck;
// close to it ⇒ it is. The y-axis is clipped near the 90th percentile (plus a margin
// proportional to the mean) so aberrant gaps — e.g. after a stop — don't squash the rest.
//
// The duration markers are interactive: hovering shows the run's completion time + duration, and
// clicking selects that run (opening it in Run Detail). Symmetrically, `selectedKey` enlarges the
// marker of the currently-selected run — a run with no marker here (e.g. a different Pond, or a
// no-duration pass) simply doesn't highlight.
const INTERVAL_COLOR = THEME_PULL; // run cadence, tied to Wave/pull (≈ bottleneck under a Wave)
const DURATION_COLOR = THEME_RUNNING; // run duration, tied to the running state

export type TracePointView = { key: string; time: number; duration: number };

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
  const [hover, setHover] = useState<number | null>(null);
  const W = 256;
  const H = 100;
  const padL = 6;
  const padR = 46;
  const padT = 10;
  const padB = 14;

  const times = points.map((p) => p.time);
  const durs = points.map((p) => p.duration / 1000); // one per run (s)
  // Intervals (s) between consecutive completions.
  const intervals: number[] = [];
  for (let i = 1; i < times.length; i++) intervals.push((times[i] - times[i - 1]) / 1000);

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
  const all = [...intervals, ...durs];
  const sorted = [...all].sort((a, b) => a - b);
  const mean = all.reduce((a, b) => a + b, 0) / (all.length || 1);
  const yMax = Math.max(quantile(sorted, 0.9) + 0.4 * mean, 0.01);

  // Shared x-index by completion number (one point per run).
  const n = points.length;
  const x = (i: number) => (n <= 1 ? padL : padL + (i / (n - 1)) * (W - padL - padR));
  const y = (v: number) => padT + (1 - Math.min(v, yMax) / yMax) * (H - padT - padB);

  // A non-interactive series (polyline + small dots) — used for the interval cadence.
  const series = (vals: number[], color: string) => {
    if (vals.length === 0) return null;
    const pts = vals.map((v, i) => `${x(i)},${y(v)}`).join(' ');
    return (
      <>
        {vals.length > 1 && <polyline points={pts} fill="none" stroke={color} strokeWidth={1.5} />}
        {vals.map((v, i) => <circle key={i} cx={x(i)} cy={y(v)} r={1.8} fill={color} />)}
      </>
    );
  };

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

  const durPolyline = durs.length > 1
    ? <polyline points={durs.map((v, i) => `${x(i)},${y(v)}`).join(' ')} fill="none" stroke={DURATION_COLOR} strokeWidth={1.5} />
    : null;

  return (
    <div>
      {header}
      <div style={{ position: 'relative' }}>
        <svg width={W} height={H} style={{ display: 'block' }}>
          <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke="#27272a" strokeWidth={1} />
          {meanLine(meanLast3(intervals), INTERVAL_COLOR)}
          {meanLine(meanLast3(durs), DURATION_COLOR)}
          {durPolyline}
          {series(intervals, INTERVAL_COLOR)}
          {/* Interactive duration markers on top: hover → tooltip, click → select, selected → enlarged. */}
          {points.map((p, i) => {
            const cx = x(i);
            const cy = y(durs[i]);
            const active = hover === i || selectedKey === p.key;
            return (
              <g key={p.key}>
                <circle cx={cx} cy={cy} r={active ? 4 : 2} fill={DURATION_COLOR}
                        stroke={active ? '#fff' : 'none'} strokeWidth={active ? 1 : 0} />
                <circle cx={cx} cy={cy} r={8} fill="transparent"
                        style={{ cursor: onSelect ? 'pointer' : 'default' }}
                        onMouseEnter={() => setHover(i)}
                        onMouseLeave={() => setHover((h) => (h === i ? null : h))}
                        onClick={onSelect ? () => onSelect(p.key) : undefined} />
              </g>
            );
          })}
        </svg>
        {hover != null && points[hover] && (
          <div
            style={{
              position: 'absolute', left: x(hover), top: y(durs[hover]), transform: 'translate(-50%, -118%)',
              pointerEvents: 'none', background: '#09090b', border: '1px solid #3f3f46', borderRadius: 5,
              padding: '3px 7px', fontSize: 10.5, lineHeight: 1.5, color: '#e4e4e7', whiteSpace: 'nowrap', zIndex: 5,
            }}
          >
            <div style={{ color: '#a1a1aa' }}>{clock(points[hover].time)}</div>
            <div style={{ color: DURATION_COLOR, fontWeight: 700 }}>{durs[hover].toFixed(2)}s</div>
          </div>
        )}
      </div>
    </div>
  );
}
