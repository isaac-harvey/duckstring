// Pure visual/format logic in the poll store — the palette rules the UI leans on everywhere.
import { describe, expect, it } from 'vitest';

import {
  EDGE_COLORS,
  STATE_COLORS,
  THEME_BLOCKED,
  THEME_BRAND,
  THEME_DANGER,
  THEME_PULL,
  THEME_PUSH,
  consumeEdgeColor,
  formatAge,
  formatDuration,
  nodeFill,
  stateColor,
} from './store';
import type { NodeView } from './types';

const view = (over: Partial<NodeView>): NodeView => ({
  status: 'idle', hasPull: false, targetF: null, startF: 0, endF: 0, changedF: 0, dMs: 0,
  ...over,
} as NodeView);

describe('formatAge', () => {
  const now = 1_000_000_000_000;
  it('NEVER (0) renders as a dash', () => expect(formatAge(0, now)).toBe('—'));
  it('unit-scales with two-digit padding (no width jitter across 9↔10)', () => {
    expect(formatAge(now - 5_000, now)).toBe('05s');
    expect(formatAge(now - 59_000, now)).toBe('59s');
    expect(formatAge(now - 60_000, now)).toBe('01m');
    expect(formatAge(now - 3_600_000, now)).toBe('01h');
    expect(formatAge(now - 24 * 3_600_000, now)).toBe('01d');
    expect(formatAge(now - 30 * 24 * 3_600_000, now)).toBe('01mo');
    expect(formatAge(now - 400 * 24 * 3_600_000, now)).toBe('>1y');
  });
  it('clamps a future freshness to zero age', () => expect(formatAge(now + 5_000, now)).toBe('00s'));
});

describe('formatDuration', () => {
  it('largest whole unit', () => {
    expect(formatDuration(86_400_000)).toBe('1d');
    expect(formatDuration(3_600_000)).toBe('1h');
    expect(formatDuration(2_000)).toBe('2s');
  });
  it('one decimal for a non-integer value', () => expect(formatDuration(5_400_000)).toBe('1.5h'));
});

describe('stateColor', () => {
  it('running is always the brand cyan', () =>
    expect(stateColor(view({ status: 'running' }))).toBe(THEME_BRAND));
  it('queued is coloured by its demand — push wins over pull', () => {
    expect(stateColor(view({ status: 'queued', targetF: 123, hasPull: true }))).toBe(THEME_PUSH);
    expect(stateColor(view({ status: 'queued', targetF: null, hasPull: true }))).toBe(THEME_PULL);
    expect(stateColor(view({ status: 'queued' }))).toBe(STATE_COLORS.queued);
  });
  it('failed and killed both read red; blocked is the muted red', () => {
    expect(stateColor(view({ status: 'failed' }))).toBe(THEME_DANGER);
    expect(stateColor(view({ status: 'killed' }))).toBe(THEME_DANGER);
    expect(stateColor(view({ status: 'blocked' }))).toBe(THEME_BLOCKED);
  });
  it('an unknown status falls back to idle grey', () =>
    expect(stateColor(view({ status: 'nonsense' as never }))).toBe(STATE_COLORS.idle));
});

describe('consumeEdgeColor', () => {
  it('idle when the parent is not fresher than the child start', () => {
    expect(consumeEdgeColor(100, 100, null)).toBe(EDGE_COLORS.idle);
    expect(consumeEdgeColor(50, 100, null)).toBe(EDGE_COLORS.idle);
  });
  it('push when consuming would meet the child push target, else pull', () => {
    expect(consumeEdgeColor(200, 100, 150)).toBe(EDGE_COLORS.push);
    expect(consumeEdgeColor(200, 100, 250)).toBe(EDGE_COLORS.pull);
    expect(consumeEdgeColor(200, 100, null)).toBe(EDGE_COLORS.pull);
  });
});

describe('nodeFill', () => {
  it('is the rim colour washed at the fill alpha', () =>
    expect(nodeFill('#ff0000')).toBe('#ff000017'));
});
