// Duration parsing/formatting for the alert form (freshness SLA + re-notify cadence).
import { describe, expect, it } from 'vitest';

import { fmtStale, parseStale } from './AlertsMenu';

describe('parseStale', () => {
  it('empty means unset (null)', () => expect(parseStale('')).toBeNull());
  it('single and compound units', () => {
    expect(parseStale('30s')).toBe(30_000);
    expect(parseStale('1h')).toBe(3_600_000);
    expect(parseStale('1h30m')).toBe(5_400_000);
    expect(parseStale('2d')).toBe(172_800_000);
    expect(parseStale('1w')).toBe(604_800_000);
  });
  it('is case/whitespace tolerant', () => expect(parseStale(' 1H ')).toBe(3_600_000));
  it('rejects malformed durations loudly', () => {
    for (const bad of ['1', 'h', '1x', '1h banana', '1.5h']) {
      expect(() => parseStale(bad)).toThrow(/invalid duration/);
    }
  });
});

describe('fmtStale', () => {
  it('null/zero renders as a dash', () => {
    expect(fmtStale(null)).toBe('—');
    expect(fmtStale(0)).toBe('—');
  });
  it('largest exact unit', () => {
    expect(fmtStale(86_400_000)).toBe('1d');
    expect(fmtStale(3_600_000)).toBe('1h');
    expect(fmtStale(90_000)).toBe('90s'); // not a whole number of minutes → seconds
    expect(fmtStale(120_000)).toBe('2m');
  });
});
