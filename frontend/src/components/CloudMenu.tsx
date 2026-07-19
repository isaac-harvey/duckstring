'use client';

import { useEffect, useState } from 'react';
import {
  addDuckPool, fetchCloudSettings, fetchDuckPools, removeDuckPool, setDataRoot,
  type CloudSettings, type DuckPool,
} from '@/lib/api';

// Catchment-level cloud config (plans/cloud-config.md, full access only): the S3 data-plane target +
// the cloud-enable gate, and the named Duck Pools remote compute resolves against. On DSC the S/M/L/XL
// presets appear here as pools — this same panel serves both.

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
  const [dataRoot, setDR] = useState('');
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // Add-pool form.
  const [pName, setPName] = useState('');
  const [pProvider, setPProvider] = useState('fargate');
  const [pType, setPType] = useState('');
  const [pCpu, setPCpu] = useState('');
  const [pMem, setPMem] = useState('');
  const [pErr, setPErr] = useState<string | null>(null);

  const load = () => {
    void fetchCloudSettings().then(setSettings).catch(() => setSettings(null));
    void fetchDuckPools().then(setPools).catch(() => setPools([]));
  };
  useEffect(load, []);

  const attach = async () => {
    setErr(null); setBusy(true);
    try {
      await setDataRoot(dataRoot.trim());
      setDR('');
      load();
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'failed');
    } finally {
      setBusy(false);
    }
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
      });
      setPName(''); setPType(''); setPCpu(''); setPMem('');
      load();
    } catch (e) {
      setPErr(e instanceof Error ? e.message : 'failed');
    } finally {
      setBusy(false);
    }
  };

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

      {/* Data plane + the cloud-enable gate. */}
      <div style={{ fontSize: 11, color: '#a1a1aa', lineHeight: 1.6, marginBottom: 6 }}>
        <div>data root: <span style={{ color: '#e4e4e7' }}>{settings?.data_root || '(local disk)'}</span></div>
        <div>AWS creds: <span style={{ color: settings?.aws_configured ? '#22c55e' : '#71717a' }}>{settings?.aws_configured ? 'yes' : 'no'}</span></div>
        <div>cloud: <span style={{ color: enabled ? '#22c55e' : '#71717a' }}>{enabled ? 'enabled' : 'disabled'}</span>
          {!enabled && <span style={{ color: '#52525b' }}> — needs a remote root + AWS creds</span>}
        </div>
      </div>

      {settings?.has_data ? (
        <div style={{ fontSize: 10, color: '#52525b', marginBottom: 10, lineHeight: 1.5 }}>
          The data root is set-once — the Catchment has published data (switching would strand it).
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: 10 }}>
          <input value={dataRoot} onChange={(e) => setDR(e.target.value)} placeholder="s3://bucket/prefix" style={input} />
          {err && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{err}</div>}
          <button onClick={attach} disabled={busy || !dataRoot.trim()} style={btn('#06c4e6', busy || !dataRoot.trim())}>
            Attach data root
          </button>
        </div>
      )}

      {/* Duck Pools — the built-in S/M/L/XL presets (managed) + user pools. */}
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
              <span role="button" title="Remove" onClick={() => removeDuckPool(p.name).then(load).catch(() => undefined)}
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
            <input value={pCpu} onChange={(e) => setPCpu(e.target.value)} placeholder="cpu (e.g. 1024)" inputMode="numeric" style={smallInput} />
            <input value={pMem} onChange={(e) => setPMem(e.target.value)} placeholder="mem MiB (e.g. 4096)" inputMode="numeric" style={smallInput} />
          </div>
        ) : (
          <input value={pType} onChange={(e) => setPType(e.target.value)} placeholder="instance type (e.g. m6i.large)" style={smallInput} />
        )}
        {pErr && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{pErr}</div>}
        <button onClick={addPool} disabled={busy || !pName.trim()} style={btn('#22c55e', busy || !pName.trim())}>
          Add pool
        </button>
      </div>
    </div>
  );
}
