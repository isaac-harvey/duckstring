'use client';

import { useEffect, useState } from 'react';
import {
  addDuckPool, fetchCloudSettings, fetchDuckPools, removeDuckPool, setDataRoot, setSecret, verifyCloud,
  type CloudSettings, type CloudVerifyResult, type DuckPool,
} from '@/lib/api';
import { cpuLabel, defaultMemoryFor, FARGATE_CPU, FARGATE_MEMORY, memoryLabel } from '@/lib/aws';
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

export function CloudMenu({ onClose }: { onClose: () => void }) {
  const [settings, setSettings] = useState<CloudSettings | null>(null);
  const [pools, setPools] = useState<DuckPool[]>([]);

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

      {settings && !enabled && <EnableFlow settings={settings} onEnabled={load} />}
      {enabled && <EnabledConfig settings={settings!} pools={pools} reload={load} />}
    </div>
  );
}

// ─── Enable flow (shown while cloud is disabled) ─────────────────────────────

function EnableFlow({ settings, onEnabled }: { settings: CloudSettings; onEnabled: () => void }) {
  const [akid, setAkid] = useState('');
  const [secret, setSecret_] = useState('');
  const [region, setRegion] = useState('');
  const [root, setRoot] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [result, setResult] = useState<CloudVerifyResult | null>(null);

  const needsCreds = !settings.aws_configured;
  const needsRoot = !settings.data_root;

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
      // 3. Commit the data root only once creds check out and the bucket is writable.
      if (probeRoot && res.bucket_ok !== false) await setDataRoot(probeRoot);
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

function EnabledConfig({ settings, pools, reload }: { settings: CloudSettings; pools: DuckPool[]; reload: () => void }) {
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
      {settings.has_data && (
        <div style={{ fontSize: 10, color: '#52525b', marginBottom: 10, lineHeight: 1.5 }}>
          The data root is set-once — the Catchment has published data (switching would strand it).
        </div>
      )}

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
