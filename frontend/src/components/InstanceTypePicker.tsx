'use client';

import { useEffect, useState } from 'react';
import { fetchInstanceTypes, type InstanceType } from '@/lib/api';

// A region-aware EC2 instance-type input backed by the Catchment's live DescribeInstanceTypes. It's a
// native datalist (type-to-filter autocomplete) annotated with each type's vCPU/memory/GPU, so an
// operator picks a real, informed value instead of typing one blind — but free text is still allowed
// (the fallback when AWS is unreachable, or for a type not in the current-generation list). Shown only
// where cloud is enabled, so the fetch always has creds.

let idSeq = 0;

function spec(t: InstanceType): string {
  const parts: string[] = [];
  if (t.vcpu != null) parts.push(`${t.vcpu} vCPU`);
  if (t.memory_gib != null) parts.push(`${t.memory_gib} GiB`);
  if (t.gpu) parts.push(`${t.gpu} GPU`);
  return parts.join(' · ');
}

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

  const known = types.length > 0 && draft.trim() !== '' && !types.some((t) => t.name === draft.trim());
  const match = types.find((t) => t.name === draft.trim());

  return (
    <div style={{ flex: 1, minWidth: 90 }}>
      <input
        list={listId}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={() => { const v = draft.trim(); if (v !== value) onCommit(v); }}
        placeholder={placeholder}
        style={{ width: '100%', boxSizing: 'border-box', ...style }}
      />
      <datalist id={listId}>
        {types.map((t) => (
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
