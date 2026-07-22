'use client';

import { useEffect, useState } from 'react';
import { useLiveStore, THEME_BRAND, THEME_PULL } from '@/lib/store';
import { fetchDuckPools, type DuckConfig, type DuckPool } from '@/lib/api';
import { InstanceTypePicker } from './InstanceTypePicker';

// The per-Pond compute config (plans/cloud-config.md): the Duck TARGET (where it runs) + size, and the
// Flock posture (over-envelope offload). The effective value coalesces override ?? declared ?? default;
// a small tag shows the source. On DSC the S/M/L/XL sizes are just preset pools in the target list — no
// special UI. Full access only (mounted by the Sidebar behind canControl).

const chip = (active: boolean): React.CSSProperties => ({
  background: 'transparent',
  border: `1px solid ${active ? THEME_BRAND : '#3f3f46'}`,
  color: active ? THEME_BRAND : '#a1a1aa',
  borderRadius: 5,
  padding: '3px 8px',
  fontSize: 11,
  cursor: 'pointer',
  fontFamily: 'inherit',
});

const row: React.CSSProperties = { display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap', marginBottom: 6 };
const lbl: React.CSSProperties = { width: 46, fontSize: 11, color: '#a1a1aa', flexShrink: 0 };
const input: React.CSSProperties = {
  flex: 1, minWidth: 90, boxSizing: 'border-box', background: '#1a1a1f', border: '1px solid #3f3f46',
  borderRadius: 4, color: '#e4e4e7', padding: '3px 6px', fontSize: 11,
};

function src(duck: DuckConfig, field: 'duck_target' | 'flock_mode' | 'flock_engine' | 'oom_policy'): string | null {
  if (duck.override[field] != null) return 'override';
  if (duck.declared[field] != null) return 'declared';
  return null; // default → no tag (the common case)
}

function Tag({ from }: { from: string | null }) {
  if (!from) return null;
  return <span style={{ fontSize: 9, color: '#52525b', letterSpacing: '0.04em' }}>({from})</span>;
}

export function DuckSection({ pondId, duck }: { pondId: string; duck: DuckConfig }) {
  const setDuck = useLiveStore((s) => s.setDuck);
  const cloud = useLiveStore((s) => s.cloud);
  const [pools, setPools] = useState<DuckPool[]>([]);
  useEffect(() => { void fetchDuckPools().then(setPools).catch(() => setPools([])); }, []);

  const target = duck.duck_target;
  const cloudEnabled = cloud?.cloud_enabled ?? false;
  const credsInvalid = cloudEnabled && cloud?.creds_valid === false;
  const remoteSelected = target !== 'catchment';
  const hasOverride = Object.values(duck.override).some((v) => v != null);

  const targets = ['catchment', ...pools.map((p) => p.name), 'dedicated'];

  return (
    <div>
      {/* Persistent warning: remote targets here won't launch while the creds are rejected. */}
      {credsInvalid && (
        <div style={{
          fontSize: 10, lineHeight: 1.4, color: '#f59e0b', border: '1px solid #f59e0b66',
          background: '#f59e0b14', borderRadius: 5, padding: '5px 7px', marginBottom: 8,
        }}>
          ⚠ AWS credentials are not authenticating — a remote target won&apos;t launch. Fix in Options → Cloud.
        </div>
      )}
      {/* Where the Duck runs. 'catchment' = the Catchment's own box; a pool / 'dedicated' = remote. */}
      <div style={row}>
        <span style={lbl}>Duck <Tag from={src(duck, 'duck_target')} /></span>
        {targets.map((t) => (
          <button key={t} style={chip(target === t)} onClick={() => setDuck(pondId, { duck_target: t })}>
            {t === 'catchment' ? 'Catchment' : t === 'dedicated' ? 'Dedicated' : t}
          </button>
        ))}
      </div>

      {/* A remote target is valid config anywhere, but only runs remotely once cloud is enabled. */}
      {remoteSelected && !cloudEnabled && (
        <div style={{ fontSize: 10, color: THEME_PULL, marginBottom: 6, lineHeight: 1.4 }}>
          Runs locally until cloud is enabled (attach an S3 data root + AWS creds in Options → Cloud).
        </div>
      )}
      {pools.length === 0 && (
        <div style={{ fontSize: 10, color: '#52525b', marginBottom: 6 }}>
          No Duck Pools — add one in Options → Cloud.
        </div>
      )}

      {/* Dedicated: its own instance type + auto-stop on run completion. The Catchment box is whatever
          the host is; a pool defines its own instance type — so there is no abstract size control. */}
      {target === 'dedicated' && (
        <div style={row}>
          <span style={lbl}>Box</span>
          <InstanceTypePicker
            value={duck.dedicated_instance_type ?? ''}
            style={input}
            placeholder="instance type (e.g. r6i.2xlarge)"
            onCommit={(v) => {
              if (v !== (duck.dedicated_instance_type ?? '')) setDuck(pondId, { dedicated_instance_type: v || null });
            }}
          />
          <button
            style={chip(!!duck.dedicated_auto_stop)}
            title="Terminate the box when the Pond Run completes"
            onClick={() => setDuck(pondId, { dedicated_auto_stop: !duck.dedicated_auto_stop })}
          >
            auto-stop
          </button>
        </div>
      )}

      {/* Flock posture (over-envelope offload). Off is inert; upgrade/always need a configured engine. */}
      <div style={row}>
        <span style={lbl}>Flock <Tag from={src(duck, 'flock_mode')} /></span>
        {(['off', 'upgrade', 'always'] as const).map((m) => (
          <button key={m} style={chip(duck.flock_mode === m)} onClick={() => setDuck(pondId, { flock_mode: m })}>
            {m}
          </button>
        ))}
      </div>

      {duck.flock_mode !== 'off' && (
        <div style={row}>
          <span style={lbl}>Engine</span>
          <input
            style={input}
            defaultValue={duck.flock_engine ?? ''}
            placeholder="athena (default)"
            onBlur={(e) => {
              const v = e.target.value.trim();
              if (v !== (duck.flock_engine ?? '')) setDuck(pondId, { flock_engine: v || null });
            }}
          />
          {(['fail_up', 'fail'] as const).map((p) => (
            <button
              key={p}
              style={chip(duck.oom_policy === p)}
              title={p === 'fail_up' ? 'Offload to the engine on OOM' : 'Hard cap: fail the Pond on OOM'}
              onClick={() => setDuck(pondId, { oom_policy: p })}
            >
              {p}
            </button>
          ))}
        </div>
      )}

      {hasOverride && (
        <button style={{ ...chip(false), color: THEME_PULL, borderColor: THEME_PULL }} onClick={() => setDuck(pondId, { clear: true })}>
          Reset to declared
        </button>
      )}
    </div>
  );
}
