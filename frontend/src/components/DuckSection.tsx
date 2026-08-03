'use client';

import { useEffect, useState } from 'react';
import { useLiveStore, THEME_BRAND, THEME_PULL } from '@/lib/store';
import {
  fetchComputeDefaults, fetchDuckPools, setDuck as apiSetDuck,
  type ComputeDefaults, type DuckConfig, type DuckPool,
} from '@/lib/api';
import { cpuLabel, defaultMemoryFor, FARGATE_CPU, FARGATE_MEMORY, memoryLabel } from '@/lib/aws';
import { DeployConfigForm, deployConfigMissing } from './DeployConfigForm';
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

export function DuckSection({ pondId, duck, flockError }: {
  pondId: string; duck: DuckConfig; flockError?: string | null;
}) {
  const setDuck = useLiveStore((s) => s.setDuck);
  const cloud = useLiveStore((s) => s.cloud);
  const [pools, setPools] = useState<DuckPool[]>([]);
  useEffect(() => { void fetchDuckPools().then(setPools).catch(() => setPools([])); }, []);

  const target = duck.duck_target;
  const cloudEnabled = cloud?.cloud_enabled ?? false;
  const fargateEnabled = cloud?.fargate_enabled ?? false;
  const credsInvalid = cloudEnabled && cloud?.creds_valid === false;
  const remoteSelected = target !== 'catchment';
  const hasOverride = Object.values(duck.override).some((v) => v != null);

  // The S/M/L/XL Fargate presets (managed) aren't an option until Fargate is enabled — they rely on the
  // Catchment's Fargate env config.
  const targets = ['catchment', ...pools.filter((p) => !p.managed || fargateEnabled).map((p) => p.name), 'dedicated'];
  const onUnavailablePreset = !fargateEnabled && !!pools.find((p) => p.name === target)?.managed;

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

      {/* Its declared target is a Fargate preset, but Fargate isn't enabled — it won't launch until it is. */}
      {onUnavailablePreset && (
        <div style={{ fontSize: 10, color: THEME_PULL, marginBottom: 6, lineHeight: 1.4 }}>
          This Pond targets the <b>{target}</b> Fargate preset, but Fargate isn&apos;t enabled — add the
          Fargate deployment config in Options → Cloud, or pick another target.
        </div>
      )}
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

      {/* Dedicated: a remote Duck of one — the SAME provider setup a pool uses (provider + size + region +
          AWS deployment config), scoped to this Duck (plans/compute-config-ui.md). */}
      {target === 'dedicated' && <DedicatedDuckConfig pondId={pondId} duck={duck} />}

      {/* Flock posture (over-envelope offload). Off is inert; upgrade/always need a configured engine. */}
      <div style={row}>
        <span style={lbl}>Flock <Tag from={src(duck, 'flock_mode')} /></span>
        {(['off', 'upgrade', 'always'] as const).map((m) => (
          <button key={m} style={chip(duck.flock_mode === m)} onClick={() => setDuck(pondId, { flock_mode: m })}>
            {m}
          </button>
        ))}
      </div>

      {/* A failed Flock dispatch still completes the run locally, so this warning is the only visible
          tell that the Flock is degrading (quietly paying full local compute). */}
      {duck.flock_mode !== 'off' && flockError && (
        <div style={{ ...row, color: THEME_PULL, fontSize: 11 }} title={flockError}>
          ⚠ Flock degraded to local: {flockError.length > 90 ? `${flockError.slice(0, 90)}…` : flockError}
        </div>
      )}

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

// A Pond's Dedicated Duck: the same provider setup as a pool (provider + size + region + AWS deployment
// config), scoped to this one Duck, validated before it's accepted (the backend rejects an inadequate
// config; the form gates Apply the same way).
function DedicatedDuckConfig({ pondId, duck }: { pondId: string; duck: DuckConfig }) {
  const [defaults, setDefaults] = useState<ComputeDefaults | null>(null);
  useEffect(() => { void fetchComputeDefaults().then(setDefaults).catch(() => setDefaults(null)); }, []);

  const cur = duck.deploy_config ?? {};
  const [provider, setProvider] = useState<'fargate' | 'ec2'>(cur.provider === 'ec2' ? 'ec2' : 'fargate');
  const [cfg, setCfg] = useState<Record<string, string>>(cur);
  const [instanceType, setInstanceType] = useState(cur.instance_type ?? duck.dedicated_instance_type ?? '');
  const [cpu, setCpu] = useState(cur.cpu ?? '1024');
  const [mem, setMem] = useState(cur.memory ?? String(defaultMemoryFor(1024)));
  const [region, setRegion] = useState(cur.region ?? '');
  const [autoStop, setAutoStop] = useState(!!duck.dedicated_auto_stop);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const pickCpu = (v: string) => {
    setCpu(v);
    const m = FARGATE_MEMORY[Number(v)] ?? [];
    if (!m.includes(Number(mem))) setMem(String(m[0] ?? ''));
  };
  const missing = deployConfigMissing(provider, cfg, defaults);

  const apply = async () => {
    setBusy(true); setErr(null);
    const deploy: Record<string, string> = { provider, ...cfg };
    if (region.trim()) deploy.region = region.trim();
    if (provider === 'ec2') { if (instanceType.trim()) deploy.instance_type = instanceType.trim(); }
    else { deploy.cpu = cpu; deploy.memory = mem; }
    try {
      await apiSetDuck(pondId, { duck_target: 'dedicated', deploy_config: deploy, dedicated_auto_stop: autoStop });
    } catch (e) { setErr(e instanceof Error ? e.message : 'failed'); } finally { setBusy(false); }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginTop: 4 }}>
      <select value={provider} onChange={(e) => setProvider(e.target.value as 'fargate' | 'ec2')} style={input}>
        <option value="fargate">fargate (serverless)</option>
        <option value="ec2">ec2 (big / GPU)</option>
      </select>
      <input value={region} onChange={(e) => setRegion(e.target.value)} placeholder="region (e.g. us-east-1)" style={input} />
      {provider === 'fargate' ? (
        <div style={{ display: 'flex', gap: 6 }}>
          <select value={cpu} onChange={(e) => pickCpu(e.target.value)} style={input}>
            {FARGATE_CPU.map((c) => <option key={c} value={c}>{cpuLabel(c)}</option>)}
          </select>
          <select value={mem} onChange={(e) => setMem(e.target.value)} style={input}>
            {(FARGATE_MEMORY[Number(cpu)] ?? []).map((m) => <option key={m} value={m}>{memoryLabel(m)}</option>)}
          </select>
        </div>
      ) : (
        <InstanceTypePicker value={instanceType} onCommit={setInstanceType} region={region.trim() || undefined} style={input} />
      )}
      <div style={{ borderTop: '1px solid #27272a', paddingTop: 6 }}>
        <div style={{ fontSize: 10, color: '#71717a', marginBottom: 4 }}>Deployment ({provider})</div>
        <DeployConfigForm provider={provider} value={cfg} onChange={setCfg} defaults={defaults} />
      </div>
      {err && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{err}</div>}
      <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
        <button style={chip(autoStop)} title="Terminate the box when the Pond Run completes" onClick={() => setAutoStop(!autoStop)}>
          auto-stop
        </button>
        <button
          style={{ ...chip(false), color: THEME_BRAND, borderColor: THEME_BRAND, opacity: busy || missing.length > 0 ? 0.5 : 1 }}
          disabled={busy || missing.length > 0}
          onClick={apply}
        >
          {busy ? 'Saving…' : 'Apply'}
        </button>
      </div>
    </div>
  );
}
