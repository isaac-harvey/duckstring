'use client';

import { useEffect, useRef, useState } from 'react';
import {
  addDuckPool, fetchCloudSettings, fetchDuckPools, fetchMigration, removeDuckPool, setDataRoot, setSecret,
  verifyCloud, type CloudSettings, type CloudVerifyResult, type DuckPool, type MigrationStatus,
} from '@/lib/api';
import { cpuLabel, defaultMemoryFor, FARGATE_CPU, FARGATE_MEMORY, memoryLabel } from '@/lib/aws';
import { useLiveStore } from '@/lib/store';
import { InstanceTypePicker } from './InstanceTypePicker';

// Catchment-level cloud config (plans/cloud-config.md, full access only). Two modes:
//  • DISABLED → an enable flow: capture AWS creds (stored as AWS_* secrets, which the Catchment loads
//    into its env) + a region + an S3 data root, then VERIFY (STS GetCallerIdentity + a bucket write
//    probe) before committing the set-once data root.
//  • ENABLED → the live config: the data root + the Duck Pools remote compute resolves against, with
//    Fargate sizes as constrained cpu/memory dropdowns and EC2 types picked from the live AWS list.

const input: React.CSSProperties = {
  width: '100%', boxSizing: 'border-box', background: '#1a1a1f', border: '1px solid #3f3f46',
  borderRadius: 4, color: '#e4e4e7', padding: '4px 7px', fontSize: 12,
};
const smallInput: React.CSSProperties = { ...input, fontSize: 11, padding: '3px 6px' };
const btn = (color: string, disabled: boolean): React.CSSProperties => ({
  background: 'transparent', border: `1px solid ${color}`, color, borderRadius: 5, padding: '4px 12px',
  fontSize: 12, cursor: disabled ? 'not-allowed' : 'pointer', opacity: disabled ? 0.5 : 1, fontWeight: 600,
});
const heading: React.CSSProperties = { fontSize: 10, fontWeight: 700, color: '#a1a1aa', letterSpacing: '0.08em', marginBottom: 6 };

function fmtBytes(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(0)} KB`;
  return `${n} B`;
}

// Polls the data-plane migration progress and shows a bar while a copy runs (and the outcome). Hidden
// when idle. On completion it refreshes the parent settings (the data root has moved).
function MigrationBanner({ onDone }: { onDone: () => void }) {
  const [mig, setMig] = useState<MigrationStatus | null>(null);
  const doneHandled = useRef(false);

  useEffect(() => {
    let live = true;
    const tick = () => void fetchMigration().then((m) => { if (live) setMig(m); }).catch(() => {});
    tick();
    const id = setInterval(tick, 1000);
    return () => { live = false; clearInterval(id); };
  }, []);

  const status = mig?.status;
  useEffect(() => {
    if (status === 'done' && !doneHandled.current) {
      doneHandled.current = true;
      const t = setTimeout(onDone, 1500);  // let the "complete" tick show, then refresh settings
      return () => clearTimeout(t);
    }
  }, [status, onDone]);

  if (!mig || mig.status === 'idle') return null;

  const total = mig.total_bytes ?? 0;
  const copied = mig.copied_bytes ?? 0;
  const pct = total > 0 ? Math.min(100, Math.round((copied / total) * 100)) : (status === 'copying' ? 0 : 100);
  const color = status === 'failed' ? '#ef4444' : status === 'done' ? '#22c55e' : '#06c4e6';
  return (
    <div style={{ marginBottom: 8, border: `1px solid ${color}55`, background: `${color}12`, borderRadius: 6, padding: '6px 8px' }}>
      <div style={{ fontSize: 11, color, fontWeight: 700, marginBottom: 3 }}>
        {status === 'copying' && `Migrating → ${mig.target || 'local'}`}
        {status === 'adopting' && 'Adopting migrated data…'}
        {status === 'done' && 'Migration complete ✓'}
        {status === 'failed' && 'Migration failed'}
      </div>
      {(status === 'copying' || status === 'adopting') && (
        <>
          <div style={{ height: 5, background: '#27272a', borderRadius: 3, overflow: 'hidden' }}>
            <div style={{ width: `${pct}%`, height: '100%', background: color, transition: 'width 0.4s' }} />
          </div>
          <div style={{ fontSize: 10, color: '#a1a1aa', marginTop: 3 }}>
            {mig.copied_files ?? 0}/{mig.total_files ?? 0} files · {fmtBytes(copied)}{total > 0 ? ` / ${fmtBytes(total)}` : ''}
            {mig.pond ? ` · ${mig.pond}` : ''}
          </div>
        </>
      )}
      {status === 'failed' && <div style={{ fontSize: 10, color: '#ef4444', wordBreak: 'break-word' }}>{mig.error}</div>}
    </div>
  );
}

export function CloudMenu({ onClose }: { onClose: () => void }) {
  const [settings, setSettings] = useState<CloudSettings | null>(null);
  const [pools, setPools] = useState<DuckPool[]>([]);
  const catchment = useLiveStore((s) => s.catchment);
  const catchmentName = catchment?.name || catchment?.id || '';

  const load = () => {
    void fetchCloudSettings().then(setSettings).catch(() => setSettings(null));
    void fetchDuckPools().then(setPools).catch(() => setPools([]));
  };
  useEffect(load, []);

  const enabled = settings?.cloud_enabled;

  return (
    <div style={{
      marginTop: 8, background: '#15151a', border: '1px solid #27272a', borderRadius: 8, padding: '9px 12px',
      fontFamily: 'ui-monospace, SFMono-Regular, monospace', minWidth: 168,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <span style={heading}>CLOUD</span>
        <span role="button" onClick={onClose} style={{ cursor: 'pointer', color: '#52525b', fontSize: 13, lineHeight: 1 }}>✕</span>
      </div>

      {/* Gate summary — always shown. */}
      <div style={{ fontSize: 11, color: '#a1a1aa', lineHeight: 1.6, marginBottom: 8 }}>
        <div>data root: <span style={{ color: '#e4e4e7' }}>{settings?.data_root || '(local disk)'}</span></div>
        <div>AWS creds: <span style={{ color: settings?.aws_configured ? '#22c55e' : '#71717a' }}>{settings?.aws_configured ? 'yes' : 'no'}</span></div>
        <div>cloud: <span style={{ color: enabled ? '#22c55e' : '#71717a' }}>{enabled ? 'enabled' : 'disabled'}</span></div>
      </div>

      <MigrationBanner onDone={load} />

      {/* Persistent warning: enabled but the creds are being rejected — remote compute is broken. */}
      {enabled && settings?.creds_valid === false && (
        <div style={{
          fontSize: 11, lineHeight: 1.5, color: '#f59e0b', border: '1px solid #f59e0b66',
          background: '#f59e0b14', borderRadius: 5, padding: '6px 8px', marginBottom: 8,
        }}>
          <div style={{ fontWeight: 700 }}>⚠ AWS credentials are not authenticating</div>
          <div style={{ color: '#d4d4d8', marginTop: 2 }}>
            Remote Duck launches will fail. {settings.creds_error && <span style={{ color: '#a1a1aa' }}>({settings.creds_error})</span>}
          </div>
          <div style={{ color: '#a1a1aa', marginTop: 2 }}>Update the credentials below, then Verify.</div>
        </div>
      )}

      {settings && !enabled && <EnableFlow settings={settings} catchmentName={catchmentName} onEnabled={load} />}
      {enabled && <EnabledConfig settings={settings!} pools={pools} catchmentName={catchmentName} reload={load} />}
    </div>
  );
}

type SwitchMode = 'empty' | 'adopt' | 'migrate';

// The empty/adopt/migrate choice + a mode-aware explanation, shared by the first-time enable flow and the
// Data Plane switcher. All three keep the CURRENT location intact as a backup.
function SwitchModeChoice({ mode, setMode, catchmentName }: { mode: SwitchMode; setMode: (m: SwitchMode) => void; catchmentName: string }) {
  const chip = (m: SwitchMode, label: string, hint: string) => (
    <button key={m} onClick={() => setMode(m)} title={hint} style={{
      flex: 1, background: 'transparent', border: `1px solid ${mode === m ? '#f59e0b' : '#3f3f46'}`,
      color: mode === m ? '#f59e0b' : '#a1a1aa', borderRadius: 5, padding: '3px 5px', fontSize: 10,
      cursor: 'pointer', fontFamily: 'inherit',
    }}>{label}</button>
  );
  const blurb = mode === 'empty'
    ? <>Empties the data plane — every Pond is left with no data and idle (no auto-rebuild). Re-trigger to rebuild, or hand-copy data across first.</>
    : mode === 'adopt'
      ? <>Picks up data already in the target (you copied it in, or a plane you&apos;re returning to) and resumes with no rebuild. Use only if the target already holds this Catchment&apos;s data.</>
      : <>Copies the current data to the target (server-side where possible), then resumes with no rebuild. May take a while for large data.</>;
  return (
    <>
      <div style={{ display: 'flex', gap: 4 }}>
        {chip('empty', 'Empty + reset', 'Leave every Pond with no data and idle (no auto-rebuild).')}
        {chip('adopt', 'Adopt existing', 'The target already holds this Catchment’s data — pick it up and resume, no rebuild.')}
        {chip('migrate', 'Migrate (copy)', 'Copy the current data to the target (server-side where possible), then resume — no rebuild.')}
      </div>
      <div style={{ fontSize: 10, color: '#f59e0b', lineHeight: 1.4 }}>
        {blurb}{' '}The current location is kept intact as a backup. Type <b>{catchmentName}</b> to confirm.
      </div>
    </>
  );
}

// ─── Enable flow (shown while cloud is disabled) ─────────────────────────────

function EnableFlow({ settings, catchmentName, onEnabled }:
  { settings: CloudSettings; catchmentName: string; onEnabled: () => void }) {
  const [akid, setAkid] = useState('');
  const [secret, setSecret_] = useState('');
  const [region, setRegion] = useState('');
  const [root, setRoot] = useState('');
  const [confirm, setConfirm] = useState('');
  const [mode, setMode] = useState<SwitchMode>('migrate');  // moving local data up → carry it by default
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [result, setResult] = useState<CloudVerifyResult | null>(null);

  const needsCreds = !settings.aws_configured;
  const needsRoot = !settings.data_root;
  // Attaching a data plane to a Catchment that already published locally is a plane SWITCH — the backend
  // requires the catchment name as confirmation, and the operator picks what happens to the local data.
  const needsConfirm = needsRoot && settings.has_data;

  const run = async () => {
    setErr(null); setResult(null); setBusy(true);
    try {
      // 1. Store the creds as AWS_* secrets — the Catchment loads them into its env on set.
      if (akid.trim()) await setSecret('AWS_ACCESS_KEY_ID', akid.trim());
      if (secret.trim()) await setSecret('AWS_SECRET_ACCESS_KEY', secret.trim());
      if (region.trim()) await setSecret('AWS_DEFAULT_REGION', region.trim());
      // 2. Verify the creds (and probe the bucket if a fresh data root was given).
      const probeRoot = needsRoot ? root.trim() : '';
      const res = await verifyCloud(probeRoot || undefined);
      setResult(res);
      if (!res.ok) return;
      // 3. Commit the data root only once creds check out and the bucket is writable. When the Catchment
      //    already has local data, honour the chosen mode (migrate carries it up; empty resets; adopt
      //    picks up a pre-populated bucket).
      if (probeRoot && res.bucket_ok !== false) {
        await setDataRoot(probeRoot, needsConfirm ? confirm.trim() : undefined, needsConfirm ? mode : 'empty');
      }
      onEnabled();
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'failed');
    } finally {
      setBusy(false);
    }
  };

  const canRun = !busy && (
    (needsCreds ? akid.trim() !== '' && secret.trim() !== '' : true) &&
    (needsRoot ? root.trim() !== '' : true) &&
    (needsConfirm ? confirm.trim() === catchmentName : true) &&
    (needsCreds || needsRoot)
  );

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <div style={{ fontSize: 10, color: '#52525b', lineHeight: 1.5, marginBottom: 2 }}>
        Enable remote compute: AWS credentials + an S3 data root a remote box can read. Verified before
        anything is committed. Credentials are sent over the wire — use HTTPS.
      </div>

      {needsCreds ? (
        <>
          <input value={akid} onChange={(e) => setAkid(e.target.value)} placeholder="AWS access key ID" style={input} autoComplete="off" />
          <input value={secret} onChange={(e) => setSecret_(e.target.value)} placeholder="AWS secret access key" type="password" style={input} autoComplete="off" />
          <input value={region} onChange={(e) => setRegion(e.target.value)} placeholder="region (e.g. us-east-1)" style={input} />
        </>
      ) : (
        <div style={{ fontSize: 11, color: '#22c55e' }}>AWS credentials already configured ✓</div>
      )}

      {needsRoot ? (
        <input value={root} onChange={(e) => setRoot(e.target.value)} placeholder="s3://bucket/prefix" style={input} />
      ) : (
        <div style={{ fontSize: 11, color: '#22c55e' }}>data root set: {settings.data_root} ✓</div>
      )}

      {needsConfirm && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <div style={{ fontSize: 10, color: '#a1a1aa', lineHeight: 1.4 }}>
            This Catchment has local data — choose what happens to it when the bucket is attached:
          </div>
          <SwitchModeChoice mode={mode} setMode={setMode} catchmentName={catchmentName} />
          <input value={confirm} onChange={(e) => setConfirm(e.target.value)} placeholder={catchmentName} style={input} />
        </div>
      )}

      {result && (
        <div style={{ fontSize: 10, lineHeight: 1.5, wordBreak: 'break-word',
                      color: result.ok ? '#22c55e' : '#ef4444', border: `1px solid ${result.ok ? '#22c55e40' : '#ef444440'}`,
                      borderRadius: 4, padding: '4px 6px' }}>
          {result.ok ? (
            <>
              <div>authenticated ✓ {result.arn}</div>
              {result.account && <div style={{ color: '#71717a' }}>account {result.account}{result.region ? ` · ${result.region}` : ''}</div>}
              {result.bucket_ok === true && <div>bucket writable ✓</div>}
              {result.bucket_ok === false && <div style={{ color: '#ef4444' }}>bucket not writable: {result.bucket_error}</div>}
            </>
          ) : (
            <div>verification failed: {result.error}</div>
          )}
        </div>
      )}
      {err && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{err}</div>}

      <button onClick={run} disabled={!canRun} style={btn('#06c4e6', !canRun)}>
        {busy ? 'Verifying…' : 'Verify & enable'}
      </button>
    </div>
  );
}

// ─── Enabled config (data root + Duck Pools with real dropdowns) ─────────────

function FixCredentials({ reload }: { reload: () => void }) {
  const [akid, setAkid] = useState('');
  const [secret, setSecret_] = useState('');
  const [region, setRegion] = useState('');
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<CloudVerifyResult | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const save = async () => {
    setErr(null); setResult(null); setBusy(true);
    try {
      if (akid.trim()) await setSecret('AWS_ACCESS_KEY_ID', akid.trim());
      if (secret.trim()) await setSecret('AWS_SECRET_ACCESS_KEY', secret.trim());
      if (region.trim()) await setSecret('AWS_DEFAULT_REGION', region.trim());
      const res = await verifyCloud();
      setResult(res);
      if (res.ok) { setAkid(''); setSecret_(''); reload(); }
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'failed');
    } finally {
      setBusy(false);
    }
  };

  const canSave = !busy && akid.trim() !== '' && secret.trim() !== '';
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: 12 }}>
      <div style={heading}>UPDATE CREDENTIALS</div>
      <input value={akid} onChange={(e) => setAkid(e.target.value)} placeholder="AWS access key ID" style={smallInput} autoComplete="off" />
      <input value={secret} onChange={(e) => setSecret_(e.target.value)} placeholder="AWS secret access key" type="password" style={smallInput} autoComplete="off" />
      <input value={region} onChange={(e) => setRegion(e.target.value)} placeholder="region (optional — leave to keep)" style={smallInput} />
      {result && !result.ok && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>still failing: {result.error}</div>}
      {err && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{err}</div>}
      <button onClick={save} disabled={!canSave} style={btn('#f59e0b', !canSave)}>{busy ? 'Verifying…' : 'Save & verify'}</button>
    </div>
  );
}

function DataPlaneSection({ settings, catchmentName, reload }:
  { settings: CloudSettings; catchmentName: string; reload: () => void }) {
  const [open, setOpen] = useState(false);
  const [target, setTarget] = useState('');   // new data root ('' = local)
  const [confirm, setConfirm] = useState('');
  const [mode, setMode] = useState<SwitchMode>('empty');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const switchTo = async (toLocal: boolean) => {
    setErr(null); setBusy(true);
    try {
      await setDataRoot(toLocal ? '' : target.trim(), confirm.trim(), mode);
      setTarget(''); setConfirm(''); setOpen(false);
      reload();
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'failed');
    } finally {
      setBusy(false);
    }
  };

  const confirmed = confirm.trim() === catchmentName && catchmentName !== '';
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={heading}>DATA PLANE</div>
      <div style={{ fontSize: 11, color: '#a1a1aa', marginBottom: 4 }}>
        current: <span style={{ color: '#e4e4e7' }}>{settings.data_root || '(local disk)'}</span>
      </div>
      {!open ? (
        <button onClick={() => setOpen(true)} style={btn('#f59e0b', false)}>Switch data plane…</button>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <SwitchModeChoice mode={mode} setMode={setMode} catchmentName={catchmentName} />
          <input value={target} onChange={(e) => setTarget(e.target.value)} placeholder="s3://bucket/prefix (blank = local)" style={smallInput} />
          <input value={confirm} onChange={(e) => setConfirm(e.target.value)} placeholder={`type ${catchmentName}`} style={smallInput} />
          {err && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{err}</div>}
          <div style={{ display: 'flex', gap: 6 }}>
            <button onClick={() => switchTo(false)} disabled={busy || !confirmed || !target.trim()} style={btn('#f59e0b', busy || !confirmed || !target.trim())}>
              {busy ? 'Switching…' : 'Switch to bucket'}
            </button>
            <button onClick={() => switchTo(true)} disabled={busy || !confirmed} style={btn('#a1a1aa', busy || !confirmed)}
                    title="Point the data plane back at local disk">
              Switch to local
            </button>
          </div>
          <button onClick={() => { setOpen(false); setErr(null); }} style={{ ...btn('#52525b', false), alignSelf: 'flex-start' }}>Cancel</button>
        </div>
      )}
    </div>
  );
}

function EnabledConfig({ settings, pools, catchmentName, reload }:
  { settings: CloudSettings; pools: DuckPool[]; catchmentName: string; reload: () => void }) {
  const [busy, setBusy] = useState(false);
  const [pName, setPName] = useState('');
  const [pProvider, setPProvider] = useState('fargate');
  const [pType, setPType] = useState('');
  const [pRegion, setPRegion] = useState('');
  const [pCpu, setPCpu] = useState('1024');
  const [pMem, setPMem] = useState(String(defaultMemoryFor(1024)));
  const [pErr, setPErr] = useState<string | null>(null);

  const pickCpu = (v: string) => {
    setPCpu(v);
    const mems = FARGATE_MEMORY[Number(v)] ?? [];
    if (!mems.includes(Number(pMem))) setPMem(String(mems[0] ?? ''));
  };

  const addPool = async () => {
    setPErr(null); setBusy(true);
    try {
      await addDuckPool({
        name: pName.trim(),
        provider: pProvider,
        instance_type: pProvider === 'ec2' ? pType.trim() || null : null,
        cpu: pProvider === 'fargate' && pCpu ? Number(pCpu) : null,
        memory: pProvider === 'fargate' && pMem ? Number(pMem) : null,
        region: pProvider === 'ec2' && pRegion.trim() ? pRegion.trim() : null,
      });
      setPName(''); setPType(''); setPRegion('');
      reload();
    } catch (e) {
      setPErr(e instanceof Error ? e.message : 'failed');
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      {settings.creds_valid === false && <FixCredentials reload={reload} />}
      <DataPlaneSection settings={settings} catchmentName={catchmentName} reload={reload} />


      <div style={heading}>DUCK POOLS</div>
      {pools.map((p) => {
        const spec = (p.provider || 'fargate') === 'fargate'
          ? (p.cpu || p.memory ? `${p.cpu ?? '?'}cpu / ${p.memory ?? '?'}MiB` : '—')
          : (p.instance_type || '—');
        return (
          <div key={p.name} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <span style={{ fontSize: 12, color: '#a1a1aa' }}>
              {p.name}
              <span style={{ color: '#52525b', fontSize: 10 }}> [{p.provider || 'fargate'}] {spec}{p.managed ? ' · preset' : ''}</span>
            </span>
            {!p.managed && (
              <span role="button" title="Remove" onClick={() => removeDuckPool(p.name).then(reload).catch(() => undefined)}
                    style={{ cursor: 'pointer', color: '#52525b', fontSize: 13, lineHeight: 1 }}>✕</span>
            )}
          </div>
        );
      })}

      <div style={{ marginTop: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
        <input value={pName} onChange={(e) => setPName(e.target.value)} placeholder="pool name (e.g. heavy)" style={smallInput} />
        <select value={pProvider} onChange={(e) => setPProvider(e.target.value)} style={smallInput}>
          <option value="fargate">fargate (serverless)</option>
          <option value="ec2">ec2 (big / GPU)</option>
        </select>
        {pProvider === 'fargate' ? (
          <div style={{ display: 'flex', gap: 6 }}>
            <select value={pCpu} onChange={(e) => pickCpu(e.target.value)} style={smallInput}>
              {FARGATE_CPU.map((c) => <option key={c} value={c}>{cpuLabel(c)}</option>)}
            </select>
            <select value={pMem} onChange={(e) => setPMem(e.target.value)} style={smallInput}>
              {(FARGATE_MEMORY[Number(pCpu)] ?? []).map((m) => <option key={m} value={m}>{memoryLabel(m)}</option>)}
            </select>
          </div>
        ) : (
          <>
            <input value={pRegion} onChange={(e) => setPRegion(e.target.value)} placeholder="region (blank = Catchment default)" style={smallInput} />
            <InstanceTypePicker value={pType} onCommit={setPType} region={pRegion.trim() || undefined} style={smallInput} />
          </>
        )}
        {pErr && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{pErr}</div>}
        <button onClick={addPool} disabled={busy || !pName.trim()} style={btn('#22c55e', busy || !pName.trim())}>
          Add pool
        </button>
      </div>
    </>
  );
}
