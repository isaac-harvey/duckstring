'use client';

import { useEffect, useState } from 'react';
import { fetchInstanceTypes, type InstanceType } from '@/lib/api';

// A region-aware EC2 instance-type input backed by the Catchment's live DescribeInstanceTypes. A native
// datalist (type-to-filter autocomplete) annotated with each type's vCPU/memory/GPU, plus vCPU/memory
// range filters that narrow the list — so an operator can dial in "≥8 vCPU, 32–64 GiB" and pick from what
// fits, rather than typing blind. Free text is still allowed (the fallback when AWS is unreachable, or for
// a type not in the current-generation list).

let idSeq = 0;

function spec(t: InstanceType): string {
  const parts: string[] = [];
  if (t.vcpu != null) parts.push(`${t.vcpu} vCPU`);
  if (t.memory_gib != null) parts.push(`${t.memory_gib} GiB`);
  if (t.gpu) parts.push(`${t.gpu} GPU`);
  return parts.join(' · ');
}

const filterInput: React.CSSProperties = {
  width: 40, boxSizing: 'border-box', background: '#1a1a1f', border: '1px solid #3f3f46',
  borderRadius: 3, color: '#e4e4e7', padding: '2px 4px', fontSize: 10, fontFamily: 'inherit',
};

export function InstanceTypePicker({
  value, onCommit, region, style, placeholder = 'instance type (e.g. m6i.large)',
}: {
  value: string;
  onCommit: (v: string) => void;
  region?: string;
  style?: React.CSSProperties;
  placeholder?: string;
}) {
  const [types, setTypes] = useState<InstanceType[]>([]);
  const [note, setNote] = useState<string | null>(null);
  const [draft, setDraft] = useState(value);
  const [prevValue, setPrevValue] = useState(value);
  const [listId] = useState(() => `it-${++idSeq}`);
  const [minCpu, setMinCpu] = useState('');
  const [maxCpu, setMaxCpu] = useState('');
  const [minMem, setMinMem] = useState('');
  const [maxMem, setMaxMem] = useState('');

  // Sync an externally-changed value into the local draft during render (not in an effect — that would
  // trip react-hooks/set-state-in-effect). This is React's "adjust state on prop change" pattern.
  if (value !== prevValue) {
    setPrevValue(value);
    setDraft(value);
  }

  useEffect(() => {
    let live = true;
    void fetchInstanceTypes(region)
      .then((r) => {
        if (!live) return;
        setTypes(r.types);
        setNote(r.available ? null : (r.error ?? 'instance list unavailable — enter a type manually'));
      })
      .catch(() => { if (live) { setTypes([]); setNote(null); } });
    return () => { live = false; };
  }, [region]);

  const inRange = (n: number | null, lo: string, hi: string) => {
    if (n == null) return !lo && !hi;  // unknown spec only passes when no bound is set
    if (lo && n < Number(lo)) return false;
    if (hi && n > Number(hi)) return false;
    return true;
  };
  const filtered = types.filter((t) => inRange(t.vcpu, minCpu, maxCpu) && inRange(t.memory_gib, minMem, maxMem));

  const known = types.length > 0 && draft.trim() !== '' && !types.some((t) => t.name === draft.trim());
  const match = types.find((t) => t.name === draft.trim());

  return (
    <div style={{ flex: 1, minWidth: 90 }}>
      {types.length > 0 && (
        <div style={{ display: 'flex', gap: 3, alignItems: 'center', marginBottom: 4, fontSize: 10, color: '#71717a', flexWrap: 'wrap' }}>
          <span>vCPU</span>
          <input value={minCpu} onChange={(e) => setMinCpu(e.target.value)} placeholder="min" inputMode="numeric" style={filterInput} />
          <span>–</span>
          <input value={maxCpu} onChange={(e) => setMaxCpu(e.target.value)} placeholder="max" inputMode="numeric" style={filterInput} />
          <span style={{ marginLeft: 5 }}>GiB</span>
          <input value={minMem} onChange={(e) => setMinMem(e.target.value)} placeholder="min" inputMode="numeric" style={filterInput} />
          <span>–</span>
          <input value={maxMem} onChange={(e) => setMaxMem(e.target.value)} placeholder="max" inputMode="numeric" style={filterInput} />
          <span style={{ marginLeft: 'auto', color: '#52525b' }}>{filtered.length}/{types.length}</span>
        </div>
      )}
      <input
        list={listId}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={() => { const v = draft.trim(); if (v !== value) onCommit(v); }}
        placeholder={placeholder}
        style={{ width: '100%', boxSizing: 'border-box', ...style }}
      />
      <datalist id={listId}>
        {filtered.map((t) => (
          <option key={t.name} value={t.name}>{spec(t)}</option>
        ))}
      </datalist>
      {match && (
        <div style={{ fontSize: 10, color: '#52525b', marginTop: 2 }}>{spec(match)}</div>
      )}
      {known && (
        <div style={{ fontSize: 10, color: '#ee9333', marginTop: 2 }}>not a current-generation type in this region</div>
      )}
      {note && (
        <div style={{ fontSize: 10, color: '#52525b', marginTop: 2, wordBreak: 'break-word' }}>{note}</div>
      )}
    </div>
  );
}
