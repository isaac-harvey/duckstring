'use client';

import { useEffect, useState } from 'react';
import {
  addAlert,
  fetchAlerts,
  fetchDeliveries,
  removeAlert,
  testAlert,
  type RawAlertChannel,
  type RawAlertDelivery,
} from '@/lib/api';

// The full alert event vocabulary (matches alerts/event.py KNOWN_EVENTS).
export const EVENT_KINDS = ['failure', 'contract', 'spout', 'recovery', 'freshness'] as const;
type EventKind = (typeof EVENT_KINDS)[number];
const DEFAULT_EVENTS: EventKind[] = ['failure', 'contract', 'recovery'];

const input: React.CSSProperties = {
  width: '100%',
  boxSizing: 'border-box',
  background: '#1a1a1f',
  border: '1px solid #3f3f46',
  borderRadius: 4,
  color: '#e4e4e7',
  padding: '4px 7px',
  fontSize: 12,
};

const UNIT_S: Record<string, number> = { s: 1, m: 60, h: 3600, d: 86400, w: 604800 };

// "1h30m" → milliseconds; "" → null. Throws on a malformed duration.
export function parseStale(text: string): number | null {
  const s = text.trim().toLowerCase();
  if (!s) return null;
  const parts = [...s.matchAll(/(\d+)([smhdw])/g)];
  if (!parts.length || parts.map((p) => p[0]).join('') !== s) {
    throw new Error("invalid duration — use e.g. 1h, 30m, 1h30m");
  }
  return parts.reduce((acc, p) => acc + Number(p[1]) * UNIT_S[p[2]], 0) * 1000;
}

export function fmtStale(ms: number | null): string {
  if (!ms) return '—';
  const s = Math.round(ms / 1000);
  if (s % 86400 === 0) return `${s / 86400}d`;
  if (s % 3600 === 0) return `${s / 3600}h`;
  if (s % 60 === 0) return `${s / 60}m`;
  return `${s}s`;
}

const chip = (on: boolean): React.CSSProperties => ({
  fontSize: 10,
  padding: '2px 6px',
  borderRadius: 4,
  border: `1px solid ${on ? '#22c55e' : '#3f3f46'}`,
  color: on ? '#22c55e' : '#71717a',
  background: on ? '#22c55e14' : 'transparent',
  cursor: 'pointer',
  userSelect: 'none',
});

// An add-channel form. `fixedScope` pins the channel to one Pond (the pond-panel section) and hides the
// scope field; otherwise a scope input chooses a Pond name or leaves it catchment-wide.
export function AlertChannelForm({ fixedScope, onAdded }: { fixedScope?: string; onAdded: () => void }) {
  const [name, setName] = useState('');
  const [destination, setDestination] = useState('');
  const [scope, setScope] = useState('');
  const [events, setEvents] = useState<EventKind[]>(DEFAULT_EVENTS);
  const [stale, setStale] = useState('');
  const [renotify, setRenotify] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const toggle = (k: EventKind) =>
    setEvents((cur) => (cur.includes(k) ? cur.filter((e) => e !== k) : [...cur, k]));

  const submit = async () => {
    setErr(null);
    let staleMs: number | null;
    let renotifyMs: number | null;
    try {
      staleMs = parseStale(stale);
      renotifyMs = parseStale(renotify);
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'bad duration');
      return;
    }
    if (!events.length) {
      setErr('pick at least one event');
      return;
    }
    const scopeName = (fixedScope ?? scope).trim() || null;
    // Derive a name if none given (scheme, or the scoped pond) — must be unique.
    const scheme = destination.split(':')[0] || 'alert';
    const finalName = name.trim() || (scopeName ? `${scopeName}-alert` : scheme);
    const eventsStr = EVENT_KINDS.every((k) => events.includes(k)) ? 'all' : events.join(',');
    setBusy(true);
    try {
      await addAlert({ name: finalName, destination: destination.trim(), scope: scopeName, events: eventsStr, stale_ms: staleMs, renotify_ms: renotifyMs });
      setName('');
      setDestination('');
      setScope('');
      setStale('');
      setRenotify('');
      setEvents(DEFAULT_EVENTS);
      onAdded();
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'failed to add');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <input value={destination} onChange={(e) => setDestination(e.target.value)}
        placeholder="https://hooks.slack.com/…  or  mailto:you@x.com?smtp=host:587" style={input} />
      <div style={{ display: 'flex', gap: 6 }}>
        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="name (optional)" style={{ ...input, flex: 1 }} />
        {fixedScope === undefined && (
          <input value={scope} onChange={(e) => setScope(e.target.value)} placeholder="pond name@major (blank = all)" style={{ ...input, flex: 1 }} />
        )}
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
        {EVENT_KINDS.map((k) => (
          <span key={k} role="button" onClick={() => toggle(k)} style={chip(events.includes(k))}>{k}</span>
        ))}
      </div>
      {events.includes('freshness') && (
        <input value={stale} onChange={(e) => setStale(e.target.value)}
          placeholder="freshness SLA (e.g. 1h) — alert when stale longer" style={input} />
      )}
      <input value={renotify} onChange={(e) => setRenotify(e.target.value)}
        placeholder="re-notify every (e.g. 6h) — while still failing/stale; blank = once" style={input} />
      <div style={{ fontSize: 10, color: '#52525b', lineHeight: 1.5 }}>
        Use <span style={{ color: '#71717a' }}>{'${secret:NAME}'}</span> / <span style={{ color: '#71717a' }}>{'${env:NAME}'}</span> for tokens; resolved only at send time.
      </div>
      {err && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{err}</div>}
      <button onClick={submit} disabled={busy || !destination.trim()}
        style={{
          background: 'transparent', border: '1px solid #22c55e', color: '#22c55e', borderRadius: 5,
          padding: '4px 12px', fontSize: 12, fontWeight: 600,
          cursor: busy || !destination.trim() ? 'not-allowed' : 'pointer', opacity: busy || !destination.trim() ? 0.5 : 1,
        }}>
        Add channel
      </button>
    </div>
  );
}

// One channel row: scope/events/destination + a Test button (validates the stored channel) and remove.
export function ChannelRow({ channel, onChanged }: { channel: RawAlertChannel; onChanged: () => void }) {
  const [test, setTest] = useState<{ ok: boolean; error?: string } | null>(null);
  const [busy, setBusy] = useState(false);

  const runTest = async () => {
    setBusy(true);
    setTest(null);
    try {
      setTest(await testAlert(channel.name));
    } catch (e) {
      setTest({ ok: false, error: e instanceof Error ? e.message : 'test failed' });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ marginBottom: 6, paddingBottom: 6, borderBottom: '1px solid #1f1f24' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
        <span style={{ fontSize: 12, color: '#e4e4e7', fontWeight: 600 }}>{channel.name}</span>
        <span style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <span role="button" title="Send a test notification" onClick={busy ? undefined : runTest}
            style={{ cursor: busy ? 'wait' : 'pointer', color: '#71717a', fontSize: 11 }}>test</span>
          <span role="button" title="Remove" onClick={() => removeAlert(channel.name).then(onChanged).catch(() => undefined)}
            style={{ cursor: 'pointer', color: '#52525b', fontSize: 13, lineHeight: 1 }}>✕</span>
        </span>
      </div>
      <div style={{ fontSize: 10, color: '#71717a', marginTop: 2 }}>
        {channel.scope ?? 'all ponds'} · {channel.events}{channel.stale_ms ? ` · SLA ${fmtStale(channel.stale_ms)}` : ''}{channel.renotify_ms ? ` · re-notify ${fmtStale(channel.renotify_ms)}` : ''}
      </div>
      <div style={{ fontSize: 10, color: '#52525b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {channel.destination}
      </div>
      {test && (
        <div style={{ fontSize: 10, color: test.ok ? '#22c55e' : '#ef4444', marginTop: 2, wordBreak: 'break-word' }}>
          {test.ok ? '✓ test sent' : `✕ ${test.error ?? 'failed'}`}
        </div>
      )}
    </div>
  );
}

// The catchment-wide alert channels (full access only) — under the "Alerts" button by Collapse-all.
// Lists every channel (with per-channel Test + remove), an add form, and a recent-deliveries log.
export function AlertsMenu({ onClose }: { onClose: () => void }) {
  const [channels, setChannels] = useState<RawAlertChannel[]>([]);
  const [deliveries, setDeliveries] = useState<RawAlertDelivery[]>([]);
  const [tab, setTab] = useState<'channels' | 'log'>('channels');
  const [adding, setAdding] = useState(false);

  const load = () => fetchAlerts().then(setChannels).catch(() => setChannels([]));
  useEffect(() => { void load(); }, []);
  useEffect(() => {
    if (tab === 'log') fetchDeliveries(50).then(setDeliveries).catch(() => setDeliveries([]));
  }, [tab]);

  const statusColor: Record<string, string> = { sent: '#22c55e', pending: '#eab308', failed: '#ef4444' };

  return (
    <div style={{
      marginTop: 8, background: '#15151a', border: '1px solid #27272a', borderRadius: 8, padding: '9px 12px',
      fontFamily: 'ui-monospace, SFMono-Regular, monospace', width: 300, maxWidth: '90vw',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
        <span style={{ fontSize: 10, fontWeight: 700, color: '#a1a1aa', letterSpacing: '0.08em' }}>ALERTS</span>
        <span role="button" onClick={onClose} style={{ cursor: 'pointer', color: '#52525b', fontSize: 13, lineHeight: 1 }}>✕</span>
      </div>
      <div style={{ display: 'flex', gap: 12, marginBottom: 8 }}>
        {(['channels', 'log'] as const).map((t) => (
          <span key={t} role="button" onClick={() => setTab(t)}
            style={{ fontSize: 11, cursor: 'pointer', color: tab === t ? '#e4e4e7' : '#52525b', borderBottom: tab === t ? '1px solid #71717a' : 'none', paddingBottom: 2 }}>
            {t === 'channels' ? 'Channels' : 'Delivery log'}
          </span>
        ))}
      </div>

      {tab === 'channels' ? (
        <>
          {channels.length === 0 && !adding && <div style={{ fontSize: 12, color: '#52525b', marginBottom: 6 }}>No channels.</div>}
          {channels.map((c) => <ChannelRow key={c.name} channel={c} onChanged={load} />)}
          {adding ? (
            <div style={{ marginTop: 8 }}>
              <AlertChannelForm onAdded={() => { setAdding(false); void load(); }} />
              <div style={{ marginTop: 6 }}>
                <span role="button" onClick={() => setAdding(false)} style={{ fontSize: 11, color: '#52525b', cursor: 'pointer' }}>Cancel</span>
              </div>
            </div>
          ) : (
            <div style={{ marginTop: 6 }}>
              <span role="button" onClick={() => setAdding(true)} style={{ fontSize: 12, color: '#38bdf8', cursor: 'pointer' }}>+ Add channel</span>
            </div>
          )}
        </>
      ) : (
        <div style={{ maxHeight: 220, overflowY: 'auto' }}>
          {deliveries.length === 0 && <div style={{ fontSize: 12, color: '#52525b' }}>No deliveries yet.</div>}
          {deliveries.map((d, i) => (
            <div key={i} style={{ marginBottom: 5, fontSize: 11 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                <span style={{ color: '#a1a1aa' }}>{d.channel} · {d.kind}{d.pond ? ` · ${d.pond}` : ''}</span>
                <span style={{ color: statusColor[d.status] ?? '#71717a' }}>{d.status}</span>
              </div>
              {d.error && <div style={{ color: '#71717a', fontSize: 10, wordBreak: 'break-word' }}>{d.error}</div>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
