'use client';

import { useEffect, useState } from 'react';
import { useLiveStore, atLeast, THEME_BRAND, THEME_SUCCESS } from '@/lib/store';
import { fetchCatalog, serveQuery, promoteServe, exposeTable, type Catalog, type CatalogPond } from '@/lib/api';

// The Catalog / data-service view (plans/data-serving.md) — the consumer + owner lens, separate from
// the orchestration canvas: browse the serviceable surface (pond=schema), connect a BI tool, run
// read-only SQL, and (full) toggle exposure + promote the served major. Access-level-aware.

const panel: React.CSSProperties = { background: '#15151a', border: '1px solid #27272a', borderRadius: 8 };
const input: React.CSSProperties = {
  width: '100%', boxSizing: 'border-box', background: '#1a1a1f', border: '1px solid #3f3f46',
  borderRadius: 4, color: '#e4e4e7', padding: '5px 8px', fontSize: 12, fontFamily: 'inherit',
};

function qualified(pond: CatalogPond, major: number, table: string): string {
  return major === pond.served_major ? `${pond.name}.${table}` : `${pond.name}_v${major}.${table}`;
}

export function CatalogView() {
  const setCatalogOpen = useLiveStore((s) => s.setCatalogOpen);
  const level = useLiveStore((s) => s.accessLevel);
  const full = atLeast(level, 'full');

  const [cat, setCat] = useState<Catalog | null>(null);
  const [sel, setSel] = useState<string | null>(null);
  const [major, setMajor] = useState<number | null>(null);
  const [search, setSearch] = useState('');
  const [sql, setSql] = useState('');
  const [result, setResult] = useState<{ columns: string[]; rows: unknown[][] } | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const load = () => fetchCatalog().then((c) => {
    setCat(c);
    if (!sel && c.ponds[0]) { setSel(c.ponds[0].name); setMajor(c.ponds[0].served_major); }
  }).catch(() => setCat({ catchment: null, ponds: [], connect: {} }));
  useEffect(() => { void load(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const pond = cat?.ponds.find((p) => p.name === sel) ?? null;
  const shownMajor = major ?? pond?.served_major ?? null;
  const ponds = (cat?.ponds ?? []).filter((p) =>
    !search || p.name.includes(search) || p.tables.some((t) => t.table.includes(search)));

  const runQuery = async () => {
    setErr(null);
    try { setResult(await serveQuery(sql, 500)); }
    catch (e) { setErr(e instanceof Error ? e.message : 'query failed'); setResult(null); }
  };

  const tables = (pond?.tables ?? []).filter((t) => t.major === shownMajor);

  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 900, background: '#09090b', display: 'flex',
                  flexDirection: 'column', fontFamily: 'ui-monospace, SFMono-Regular, monospace', color: '#e4e4e7' }}>
      {/* Header: identity + connect + back to canvas. */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '8px 14px', borderBottom: '1px solid #27272a' }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: '#f4f4f5' }}>Catalog</span>
        <span style={{ fontSize: 11, color: '#71717a' }}>{cat?.catchment ?? ''}</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 10, alignItems: 'center', fontSize: 11, color: '#a1a1aa' }}>
          {Object.entries(cat?.connect ?? {}).map(([k, v]) => (
            <span key={k} title="Point a BI tool / client here (password = your API key)">
              {k}: <span style={{ color: '#e4e4e7' }}>{v}</span>
            </span>
          ))}
          {Object.keys(cat?.connect ?? {}).length === 0 && (
            <span style={{ color: '#52525b' }}>no wire ports (set DUCKSTRING_SERVE_PG_PORT / _FLIGHT_PORT)</span>
          )}
          <button onClick={() => setCatalogOpen(false)}
                  style={{ ...input, width: 'auto', cursor: 'pointer', color: THEME_BRAND, borderColor: THEME_BRAND }}>
            ← Canvas
          </button>
        </div>
      </div>

      <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
        {/* Left: searchable pond list. */}
        <div style={{ width: 220, borderRight: '1px solid #27272a', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
          <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="search ponds / tables"
                 style={{ ...input, margin: 8, width: 'calc(100% - 16px)' }} />
          <div style={{ overflowY: 'auto', flex: 1 }}>
            {ponds.map((p) => (
              <div key={p.name} onClick={() => { setSel(p.name); setMajor(p.served_major); }}
                   style={{ padding: '6px 12px', cursor: 'pointer', fontSize: 12,
                            background: p.name === sel ? '#1f1f27' : 'transparent',
                            color: p.name === sel ? '#f4f4f5' : '#a1a1aa' }}>
                {p.name} <span style={{ color: '#52525b', fontSize: 10 }}>v{p.served_major}</span>
              </div>
            ))}
            {ponds.length === 0 && <div style={{ padding: 12, color: '#52525b', fontSize: 12 }}>Nothing serviceable.</div>}
          </div>
        </div>

        {/* Middle: the selected pond's tables + version selector + promote. */}
        <div style={{ flex: 1, minWidth: 0, padding: 14, overflowY: 'auto' }}>
          {pond && (
            <>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                <span style={{ fontSize: 13, fontWeight: 700 }}>{pond.name}</span>
                <select value={shownMajor ?? ''} onChange={(e) => setMajor(Number(e.target.value))}
                        style={{ ...input, width: 'auto', fontSize: 11 }}>
                  {pond.majors.map((m) => (
                    <option key={m} value={m}>v{m}{m === pond.served_major ? ' (served)' : ''}</option>
                  ))}
                </select>
                {full && shownMajor != null && shownMajor !== pond.served_major && (
                  <button onClick={() => promoteServe(pond.name, shownMajor).then(load).catch((e) => setErr(String(e.message)))}
                          style={{ ...input, width: 'auto', cursor: 'pointer', color: THEME_SUCCESS, borderColor: THEME_SUCCESS }}>
                    Promote to default
                  </button>
                )}
              </div>
              {tables.map((t) => (
                <div key={t.table} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0' }}>
                  {full && (
                    <span role="button" title={t.exposed ? 'Hide' : 'Expose'}
                          onClick={() => exposeTable(pond.name, t.table, !t.exposed, shownMajor ?? undefined).then(load).catch(() => undefined)}
                          style={{ cursor: 'pointer', color: t.exposed ? THEME_BRAND : '#52525b', fontSize: 13 }}>
                      {t.exposed ? '●' : '○'}
                    </span>
                  )}
                  <span style={{ fontSize: 12, color: t.exposed ? '#e4e4e7' : '#52525b' }}>{t.table}</span>
                  <span style={{ fontSize: 10, color: '#52525b' }}>{full ? `[${t.source}]` : ''}</span>
                  <span role="button" title="Copy the query name"
                        onClick={() => navigator.clipboard?.writeText(qualified(pond, shownMajor!, t.table))}
                        style={{ marginLeft: 'auto', cursor: 'pointer', color: '#71717a', fontSize: 10 }}>
                    {qualified(pond, shownMajor!, t.table)} ⧉
                  </span>
                </div>
              ))}
              {tables.length === 0 && <div style={{ color: '#52525b', fontSize: 12 }}>No exposed tables on this major.</div>}
            </>
          )}
        </div>

        {/* Right: the query console. */}
        <div style={{ width: 360, borderLeft: '1px solid #27272a', padding: 12, display: 'flex', flexDirection: 'column', gap: 8, minHeight: 0 }}>
          <span style={{ fontSize: 10, fontWeight: 700, color: '#a1a1aa', letterSpacing: '0.08em' }}>QUERY</span>
          <textarea value={sql} onChange={(e) => setSql(e.target.value)} placeholder="SELECT * FROM sales.revenue LIMIT 100"
                    rows={4} style={{ ...input, fontFamily: 'inherit', resize: 'vertical' }} />
          <button onClick={runQuery} disabled={!sql.trim()}
                  style={{ ...input, width: 'auto', alignSelf: 'flex-start', cursor: sql.trim() ? 'pointer' : 'not-allowed',
                           color: THEME_BRAND, borderColor: THEME_BRAND, opacity: sql.trim() ? 1 : 0.5 }}>
            Run
          </button>
          {err && <div style={{ fontSize: 11, color: '#ef4444', wordBreak: 'break-word' }}>{err}</div>}
          {result && (
            <div style={{ ...panel, overflow: 'auto', flex: 1, padding: 6 }}>
              <table style={{ borderCollapse: 'collapse', fontSize: 11 }}>
                <thead><tr>{result.columns.map((c) => (
                  <th key={c} style={{ textAlign: 'left', padding: '2px 8px', color: '#a1a1aa', borderBottom: '1px solid #27272a' }}>{c}</th>
                ))}</tr></thead>
                <tbody>{result.rows.map((row, i) => (
                  <tr key={i}>{row.map((v, j) => (
                    <td key={j} style={{ padding: '2px 8px', color: '#d4d4d8', whiteSpace: 'nowrap' }}>{v === null ? '∅' : String(v)}</td>
                  ))}</tr>
                ))}</tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
