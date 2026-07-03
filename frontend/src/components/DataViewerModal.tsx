'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { useLiveStore, atLeast, THEME_BLOCKED, THEME_BRAND } from '@/lib/store';
import {
  fetchTables, fetchFreshness, fetchHistory, fetchCount, fetchPage, fetchObjects, downloadObject,
  deleteTable, deleteObject,
  type DataQuery, type TableInfo, type TrickleMode, type PageResult, type ObjectInfo, UnauthorizedError,
} from '@/lib/api';
import type { PondId } from '@/lib/types';
import { ConfirmDialog, type ConfirmOpts } from './ConfirmDialog';

const ROW_H = 26;
const NUM_W = 60;
const COL_W = 180;
const CHUNK = 400;
const OVERSCAN = 80;

// Trickle system columns surfaced by the consolidated browse view.
const ACTIVE = '_duckstring_active'; // +1 present / -1 deleted — drives row colour, never shown
const FRESH = '_duckstring_f'; // most-recent run freshness
const UPDATES = '_duckstring_updates'; // count of +1 changelog events
const EVENT = '_duckstring_event'; // create | update | delete (per-record history)
const DELTA = '_duckstring_d'; // Z-set weight: +1 insert / -1 retraction — shown on raw __changelog views
const COL_LABELS: Record<string, string> = { [FRESH]: 'freshness', [UPDATES]: 'updates', [EVENT]: 'event', [DELTA]: 'Δ' };
// History event → label colour (reusing the theme): create = white, update = brand cyan, delete = blocked red.
const EVENT_COLOR: Record<string, string> = { create: '#f4f4f5', update: THEME_BRAND, delete: THEME_BLOCKED };

// A Trickle companion (X__changelog / X__band / X__droplog / X__base) belongs to base table X — a delete
// takes the whole collection, so a companion resolves to its base (mirrors trickle_io.base_table_name).
function baseTableName(name: string): string {
  for (const s of ['__changelog', '__band', '__droplog', '__base']) {
    if (name.endsWith(s) && name.length > s.length) return name.slice(0, -s.length);
  }
  return name;
}

const browseSql = (pond: string, table: string) => `SELECT * FROM "${pond}"."${table}" LIMIT 1000`;
const on401 = (e: unknown) => e instanceof UnauthorizedError && useLiveStore.setState({ needsKey: true });
// A freshness ISO → compact, stable 'YYYY-MM-DD HH:MM:SS' (backend serialises in UTC).
const fmtTs = (iso: string) => iso.slice(0, 19).replace('T', ' ');

export function DataViewerModal() {
  const pondId = useLiveStore((s) => s.dataViewerPondId);
  if (!pondId) return null;
  return <DataViewer key={pondId} pondId={pondId} />;
}

function DataViewer({ pondId }: { pondId: PondId }) {
  const close = useLiveStore((s) => s.closeDataViewer);
  const pondName = useLiveStore((s) => s.ponds[pondId]?.name ?? pondId);
  const hasObjects = useLiveStore((s) => s.pondInfo[pondId]?.hasObjects ?? false);
  const canManage = useLiveStore((s) => atLeast(s.accessLevel, 'full'));

  // Tabular data vs non-tabular Objects (models/blobs). Objects are a separate published surface.
  const [view, setView] = useState<'tables' | 'objects'>('tables');
  const [tables, setTables] = useState<TableInfo[] | null>(null);
  const [table, setTable] = useState<string | null>(null);
  const [mode, setMode] = useState<'browse' | 'query'>('browse');
  const [sqlText, setSqlText] = useState('');
  const [activeSql, setActiveSql] = useState('');
  const [expanded, setExpanded] = useState(false);
  const [total, setTotal] = useState<number | null>(null);
  const [tablesError, setTablesError] = useState<string | null>(null);
  // Trickle freshness window (browse only): inclusive [fLo, fHi]; null = unbounded.
  const [freshness, setFreshness] = useState<string[]>([]);
  const [floor, setFloor] = useState<string | null>(null);
  const [fLo, setFLo] = useState<string | null>(null);
  const [fHi, setFHi] = useState<string | null>(null);
  // The record whose changelog history is open (merge only).
  const [historyPk, setHistoryPk] = useState<Record<string, unknown> | null>(null);
  // A pending destructive confirmation (themed in-app dialog), shared by the table + object deletes.
  const [confirm, setConfirm] = useState<ConfirmOpts | null>(null);
  // Opt-in column sort (null = the efficient base order). Clicking a header cycles asc → desc → off.
  const [sort, setSort] = useState<{ col: string | null; desc: boolean }>({ col: null, desc: false });
  const cycleSort = (col: string) =>
    setSort((s) => (s.col !== col ? { col, desc: false } : !s.desc ? { col, desc: true } : { col: null, desc: false }));
  const taRef = useRef<HTMLTextAreaElement>(null);

  const tableInfo = tables?.find((t) => t.name === table) ?? null;
  const trickle: TrickleMode | null = tableInfo?.trickle ?? null;

  const loadFreshness = useCallback(
    async (tbl: string, ti: TableInfo | undefined) => {
      if (!ti?.trickle) {
        setFreshness([]);
        setFloor(null);
        return;
      }
      try {
        const r = await fetchFreshness(pondId, tbl);
        setFreshness(r.freshness);
        setFloor(r.floor);
      } catch (e) {
        on401(e);
        setFreshness([]);
      }
    },
    [pondId]
  );

  // Load the table list once on mount; auto-select the first table (deferred — no sync setState in effect).
  useEffect(() => {
    const t = setTimeout(async () => {
      try {
        const ts = await fetchTables(pondId);
        setTables(ts);
        if (ts.length) {
          setTable(ts[0].name);
          setSqlText(browseSql(pondName, ts[0].name));
          void loadFreshness(ts[0].name, ts[0]);
        } else if (hasObjects) {
          setView('objects'); // no tables — open straight to the Objects tab
        }
      } catch (e) {
        on401(e);
        setTablesError(e instanceof Error ? e.message : String(e));
        setTables([]);
      }
    }, 0);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    // When a confirm dialog is open it owns Escape (it closes itself); don't also close the modal.
    const onKey = (e: KeyboardEvent) =>
      e.key === 'Escape' && !confirm && (historyPk ? setHistoryPk(null) : close());
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [close, historyPk, confirm]);

  const expand = () => {
    setExpanded(true);
    const ta = taRef.current;
    if (ta && ta.offsetHeight < 110) ta.style.height = '110px';
  };
  const collapse = () => {
    setExpanded(false);
    if (taRef.current) taRef.current.style.height = '38px';
  };

  const selectTable = (t: string) => {
    const ti = tables?.find((x) => x.name === t);
    setTable(t);
    setMode('browse');
    setSqlText(browseSql(pondName, t));
    setFLo(null);
    setFHi(null);
    setTotal(null);
    setSort({ col: null, desc: false }); // columns differ between tables
    void loadFreshness(t, ti);
  };
  const deleteCurrentTable = () => {
    if (!table) return;
    const target = table;
    // A Trickle companion (changelog/band/droplog/base) is one deletable unit with its base table.
    const base = baseTableName(target);
    const baseMode = tables?.find((t) => t.name === base)?.trickle ?? null;
    const notes: string[] = [];
    if (base !== target) notes.push(`“${target}” is part of table “${base}”, so the whole table is deleted.`);
    if (baseMode === 'merge') notes.push('Its changelog is removed too.');
    else if (baseMode === 'append') notes.push('It is an append Trickle — its droplog and accumulated history are dropped.');
    setConfirm({
      title: `Delete “${base}”?`,
      body: `This cannot be undone.${notes.length ? ' ' + notes.join(' ') : ''}`,
      confirmLabel: 'Delete table',
      action: async () => {
        try {
          await deleteTable(pondId, target); // the Catchment resolves a companion to its base
          const ts = await fetchTables(pondId);
          setTables(ts);
          setTable(ts[0]?.name ?? null);
          if (ts[0]) {
            setSqlText(browseSql(pondName, ts[0].name));
            void loadFreshness(ts[0].name, ts[0]);
          }
        } catch (e) {
          on401(e);
          setTablesError(e instanceof Error ? e.message : String(e));
        }
      },
    });
  };
  const runQuery = () => {
    if (!sqlText.trim()) return;
    setActiveSql(sqlText);
    setMode('query');
    setTotal(null);
    setSort({ col: null, desc: false });
  };
  const clearQuery = () => {
    setMode('browse');
    setTotal(null);
    setSort({ col: null, desc: false });
    if (table) setSqlText(browseSql(pondName, table));
  };

  // The active grid query + a key that remounts the grid whenever the source/window/sort changes.
  let query: DataQuery | null =
    mode === 'query'
      ? { pond: pondId, sql: activeSql }
      : table
        ? trickle
          ? { pond: pondId, table, trickle, pk: tableInfo?.pk ?? [], fLo, fHi }
          : { pond: pondId, table }
        : null;
  if (query && sort.col) query = { ...query, orderBy: sort.col, orderDesc: sort.desc };
  const queryKey =
    (mode === 'query' ? `sql:${activeSql}` : `tbl:${table}:${trickle}:${fLo}:${fHi}`) + `:${sort.col}:${sort.desc}`;

  return (
    <div
      onClick={close}
      style={{
        position: 'fixed', inset: 0, zIndex: 1100, display: 'flex', alignItems: 'center',
        justifyContent: 'center', background: 'rgba(9, 9, 11, 0.78)', backdropFilter: 'blur(2px)',
        fontFamily: 'ui-monospace, SFMono-Regular, monospace',
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: '#101014', border: '1px solid #27272a', borderRadius: 10,
          width: '92vw', height: '88vh', display: 'flex', flexDirection: 'column', overflow: 'hidden',
          position: 'relative',
        }}
      >
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px', borderBottom: '1px solid #27272a', flexShrink: 0 }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: '#e4e4e7' }}>{pondName}</span>
          {hasObjects && tables && tables.length > 0 && (
            <span style={{ display: 'inline-flex', border: '1px solid #3f3f46', borderRadius: 6, overflow: 'hidden' }}>
              {(['tables', 'objects'] as const).map((v) => (
                <button
                  key={v}
                  onClick={() => setView(v)}
                  style={{
                    background: view === v ? '#27272a' : 'transparent', border: 'none', padding: '5px 11px',
                    color: view === v ? '#e4e4e7' : '#71717a', fontSize: 12, fontWeight: 700, cursor: 'pointer',
                    fontFamily: 'inherit',
                  }}
                >
                  {v === 'tables' ? 'Tables' : 'Objects'}
                </button>
              ))}
            </span>
          )}
          {view === 'tables' && tables && tables.length > 0 && (
            <span style={{ position: 'relative', display: 'inline-flex', alignItems: 'center' }}>
              <select
                value={mode === 'browse' ? table ?? '' : ''}
                onChange={(e) => selectTable(e.target.value)}
                style={{
                  appearance: 'none', WebkitAppearance: 'none', MozAppearance: 'none',
                  background: '#18181b', border: '1px solid #3f3f46', borderRadius: 6, padding: '5px 26px 5px 9px',
                  color: '#e4e4e7', fontSize: 12.5, fontFamily: 'inherit', outline: 'none', cursor: 'pointer',
                }}
              >
                {mode === 'query' && <option value="">(query)</option>}
                {tables.map((t) => (
                  <option key={t.name} value={t.name}>{t.name}{t.trickle ? ` · ${t.trickle}` : ''}</option>
                ))}
              </select>
              <span style={{ position: 'absolute', right: 9, pointerEvents: 'none', color: '#71717a', fontSize: 9 }}>▼</span>
            </span>
          )}
          {view === 'tables' && mode === 'browse' && table && canManage && (
            <button
              onClick={deleteCurrentTable}
              title={`Delete "${table}" (drops its data + state, then rebuilds)`}
              style={{
                background: 'transparent', border: `1px solid ${THEME_BLOCKED}66`, borderRadius: 5,
                color: THEME_BLOCKED, fontSize: 11.5, fontWeight: 700, padding: '4px 9px', cursor: 'pointer',
                fontFamily: 'inherit',
              }}
            >
              Delete
            </button>
          )}
          {view === 'tables' && mode === 'query' && (
            <span style={{ fontSize: 10, fontWeight: 700, color: '#ee9333', letterSpacing: '0.06em' }}>QUERY</span>
          )}
          {view === 'tables' && (
            <span style={{ fontSize: 11, color: '#71717a' }}>
              {total == null ? '' : `${total.toLocaleString()} row${total === 1 ? '' : 's'}`}
            </span>
          )}
          <button
            onClick={close}
            title="Close (Esc)"
            style={{
              marginLeft: 'auto', background: 'transparent', border: '1px solid #3f3f46', borderRadius: 5,
              color: '#a1a1aa', fontSize: 13, lineHeight: 1, padding: '4px 9px', cursor: 'pointer', fontFamily: 'inherit',
            }}
          >
            ✕
          </button>
        </div>

        {/* Freshness window — only for a Trickle table being browsed */}
        {view === 'tables' && mode === 'browse' && trickle && (
          <FreshnessWindow
            freshness={freshness}
            floor={floor}
            fLo={fLo}
            fHi={fHi}
            setLo={setFLo}
            setHi={setFHi}
          />
        )}

        {/* SQL box */}
        {view === 'tables' && (
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 8, padding: '10px 14px', borderBottom: '1px solid #27272a', flexShrink: 0 }}>
          <textarea
            ref={taRef}
            value={sqlText}
            onChange={(e) => setSqlText(e.target.value)}
            onFocus={expand}
            onBlur={collapse}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
                e.preventDefault();
                runQuery();
              }
            }}
            spellCheck={false}
            style={{
              flex: 1, resize: expanded ? 'vertical' : 'none', height: 38, minHeight: 38, maxHeight: '45vh',
              background: '#18181b', border: '1px solid #3f3f46', borderRadius: 6, padding: '8px 10px',
              color: '#e4e4e7', fontSize: 12.5, fontFamily: 'inherit', outline: 'none', lineHeight: 1.5,
            }}
          />
          <div style={{ display: 'flex', gap: 6 }}>
            <button
              onClick={runQuery}
              title="Run (⌘/Ctrl+Enter)"
              style={{
                display: 'inline-flex', alignItems: 'center', justifyContent: 'center', height: 38, padding: '0 16px',
                background: '#06c4e6', border: 'none', borderRadius: 6,
                color: '#09090b', fontSize: 12.5, fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
              }}
            >
              Run
            </button>
            {mode === 'query' && (
              <button
                onClick={clearQuery}
                title="Clear query — back to browsing the table"
                style={{
                  display: 'inline-flex', alignItems: 'center', justifyContent: 'center', height: 38, padding: '0 14px',
                  background: 'transparent', border: '1px solid #3f3f46', borderRadius: 6,
                  color: '#a1a1aa', fontSize: 12.5, fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
                }}
              >
                Clear
              </button>
            )}
          </div>
        </div>
        )}

        {/* Body: the tabular grid, or the Objects list */}
        <div style={{ flex: 1, minHeight: 0 }}>
          {view === 'objects' ? (
            <ObjectsPanel pondId={pondId} canManage={canManage} requestConfirm={setConfirm} />
          ) : tablesError ? (
            <div style={{ padding: 16, color: '#ef4444', fontSize: 12.5 }}>{tablesError}</div>
          ) : tables == null ? (
            <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>Loading…</div>
          ) : query == null ? (
            <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>
              This Pond has no exported tables.{hasObjects ? ' See the Objects tab.' : ''}
            </div>
          ) : (
            <VirtualGrid key={queryKey} query={query} onTotal={setTotal} onRowClick={setHistoryPk} sort={sort} onSort={cycleSort} />
          )}
        </div>

        {historyPk && table && (
          <HistoryOverlay pond={pondId} pondName={pondName} table={table} pk={historyPk} onClose={() => setHistoryPk(null)} />
        )}

        {confirm && <ConfirmDialog opts={confirm} onClose={() => setConfirm(null)} />}
      </div>
    </div>
  );
}

// ─── Objects list ────────────────────────────────────────────────────────────

function fmtBytes(n: number | null): string {
  if (n == null) return '';
  if (n < 1024) return `${n} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

function ObjectsPanel({ pondId, canManage, requestConfirm }: {
  pondId: PondId; canManage: boolean; requestConfirm: (o: ConfirmOpts) => void;
}) {
  const [objects, setObjects] = useState<ObjectInfo[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setObjects(await fetchObjects(pondId));
    } catch (e) {
      on401(e);
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [pondId]);

  useEffect(() => {
    const t = setTimeout(() => void load(), 0);
    return () => clearTimeout(t);
  }, [load]);

  const download = async (o: ObjectInfo) => {
    setBusy(o.name);
    try {
      await downloadObject(pondId, o);
    } catch (e) {
      on401(e);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  };

  const remove = (o: ObjectInfo) => {
    requestConfirm({
      title: `Delete “${o.name}”?`,
      body: 'This Object is removed. It returns only if a Ripple writes it again.',
      confirmLabel: 'Delete object',
      action: async () => {
        setBusy(o.name);
        try {
          await deleteObject(pondId, o.name);
          await load();
        } catch (e) {
          on401(e);
          setError(e instanceof Error ? e.message : String(e));
        } finally {
          setBusy(null);
        }
      },
    });
  };

  if (error) return <div style={{ padding: 16, color: '#ef4444', fontSize: 12.5 }}>{error}</div>;
  if (objects == null) return <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>Loading…</div>;
  if (objects.length === 0) return <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>No objects.</div>;

  return (
    <div style={{ height: '100%', overflow: 'auto' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 12, color: '#d4d4d8' }}>
        <thead style={{ position: 'sticky', top: 0, zIndex: 1 }}>
          <tr>
            <th style={th({})}>name</th>
            <th style={th({})}>kind</th>
            <th style={th({ textAlign: 'right' })}>size</th>
            <th style={th({})}>freshness</th>
            <th style={th({ textAlign: 'right' })}></th>
          </tr>
        </thead>
        <tbody>
          {objects.map((o, i) => (
            <tr key={o.name} style={{ height: ROW_H, background: i % 2 ? '#121217' : 'transparent' }}>
              <td style={td({ color: '#e4e4e7', fontWeight: 600 })} title={o.name}>{o.name}</td>
              <td style={td({ color: '#a1a1aa' })}>{o.is_dir ? 'directory' : `file${o.ext ? ` · ${o.ext}` : ''}`}</td>
              <td style={td({ textAlign: 'right', color: '#a1a1aa' })}>{fmtBytes(o.size)}</td>
              <td style={td({ color: '#a1a1aa' })}>{o.f ? fmtTs(o.f) : <span style={{ color: '#3f3f46' }}>·</span>}</td>
              <td style={td({ textAlign: 'right' })}>
                <span style={{ display: 'inline-flex', gap: 6, justifyContent: 'flex-end' }}>
                  <button
                    onClick={() => download(o)}
                    disabled={busy === o.name}
                    title={o.is_dir ? 'Download as a zip' : 'Download'}
                    style={{
                      background: 'transparent', border: '1px solid #3f3f46', borderRadius: 5, padding: '2px 10px',
                      color: busy === o.name ? '#52525b' : '#06c4e6', fontSize: 11.5, fontWeight: 700,
                      cursor: busy === o.name ? 'default' : 'pointer', fontFamily: 'inherit',
                    }}
                  >
                    {busy === o.name ? '…' : o.is_dir ? 'Download .zip' : 'Download'}
                  </button>
                  {canManage && (
                    <button
                      onClick={() => remove(o)}
                      disabled={busy === o.name}
                      title="Delete this Object"
                      style={{
                        background: 'transparent', border: `1px solid ${THEME_BLOCKED}66`, borderRadius: 5,
                        padding: '2px 10px', color: THEME_BLOCKED, fontSize: 11.5, fontWeight: 700,
                        cursor: busy === o.name ? 'default' : 'pointer', fontFamily: 'inherit',
                      }}
                    >
                      Delete
                    </button>
                  )}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Freshness window control ────────────────────────────────────────────────

// One bound: a run-freshness select (newest-first) with the given specials, plus a datetime override.
function Bound({
  value, onChange, freshness, specials, lo,
}: {
  value: string | null;
  onChange: (v: string | null) => void;
  freshness: string[];
  specials: { v: string; label: string }[];
  lo: string | null; // the lower bound (for "= from")
}) {
  const toLocal = (iso: string | null) => {
    if (!iso) return '';
    const d = new Date(iso);
    const p = (n: number) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`;
  };
  const selVal = value && freshness.includes(value) ? value : '';
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      <select
        value={selVal}
        onChange={(e) => {
          const v = e.target.value;
          onChange(v === '__same__' ? lo : v === '' ? null : v);
        }}
        style={{
          appearance: 'none', WebkitAppearance: 'none', MozAppearance: 'none',
          background: '#18181b', border: '1px solid #3f3f46', borderRadius: 6, padding: '4px 22px 4px 8px',
          color: '#e4e4e7', fontSize: 12, fontFamily: 'inherit', outline: 'none', cursor: 'pointer',
        }}
      >
        {specials.map((s) => <option key={s.v} value={s.v}>{s.label}</option>)}
        {freshness.map((f) => <option key={f} value={f}>{fmtTs(f)}</option>)}
      </select>
      <input
        type="datetime-local"
        value={toLocal(selVal ? null : value)}
        onChange={(e) => onChange(e.target.value ? new Date(e.target.value).toISOString() : null)}
        title="or pick a time"
        style={{
          background: '#18181b', border: '1px solid #3f3f46', borderRadius: 6, padding: '3px 6px',
          color: '#a1a1aa', fontSize: 11, fontFamily: 'inherit', outline: 'none', colorScheme: 'dark',
        }}
      />
    </span>
  );
}

function FreshnessWindow({
  freshness, floor, fLo, fHi, setLo, setHi,
}: {
  freshness: string[];
  floor: string | null;
  fLo: string | null;
  fHi: string | null;
  setLo: (v: string | null) => void;
  setHi: (v: string | null) => void;
}) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 10, padding: '8px 14px', borderBottom: '1px solid #27272a', flexShrink: 0, fontSize: 12, color: '#a1a1aa' }}>
      <span style={{ fontWeight: 700, color: '#71717a' }}>Freshness</span>
      <Bound value={fLo} onChange={setLo} freshness={freshness} lo={fLo} specials={[{ v: '', label: 'Earliest' }]} />
      <span style={{ color: '#52525b' }}>→</span>
      <Bound value={fHi} onChange={setHi} freshness={freshness} lo={fLo} specials={[{ v: '', label: 'Now' }, { v: '__same__', label: '= from' }]} />
      {floor && <span style={{ color: '#52525b', fontSize: 11 }}>floor {fmtTs(floor)}</span>}
    </div>
  );
}

// ─── Virtual grid ──────────────────────────────────────────────────────────────

function VirtualGrid({
  query, onTotal, onRowClick, sort, onSort,
}: {
  query: DataQuery;
  onTotal: (n: number) => void;
  onRowClick: (pk: Record<string, unknown>) => void;
  sort: { col: string | null; desc: boolean };
  onSort: (col: string) => void;
}) {
  const [total, setTotal] = useState<number | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [windowStart, setWindowStart] = useState(0);
  const [rows, setRows] = useState<unknown[][]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inFlight = useRef(false);
  const queued = useRef<number | null>(null);
  const loaded = useRef({ start: 0, end: 0 });
  const fetchWindowRef = useRef<(start: number) => void>(() => {});

  const fetchWindow = useCallback(
    async (start: number) => {
      // Serialise: one /page request in flight at a time. While one runs, keep only the latest window
      // the user has scrolled to — a fast scroll (or scrollbar drag) collapses to the in-flight request
      // plus a single follow-up, instead of one request per row crossed (which hung the page).
      if (inFlight.current) {
        queued.current = start;
        return;
      }
      inFlight.current = true;
      try {
        const res = await fetchPage({ ...query, limit: CHUNK, offset: start });
        setColumns((prev) => (res.columns.length ? res.columns : prev));
        setRows(res.rows);
        setWindowStart(start);
        loaded.current = { start, end: start + res.rows.length };
      } catch (e) {
        on401(e);
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        inFlight.current = false;
        // Land the most recent window scrolled to while this request was in flight.
        const next = queued.current;
        queued.current = null;
        if (next !== null && next !== loaded.current.start) void fetchWindowRef.current(next);
      }
    },
    [query]
  );
  useEffect(() => {
    fetchWindowRef.current = fetchWindow;
  }, [fetchWindow]);

  useEffect(() => {
    const t = setTimeout(async () => {
      try {
        // Always fetch the first window — the count only *sizes* the scroll; gating the page on it would
        // let a 0/stale count silently hide real rows (and suppress the /page request entirely).
        const [c] = await Promise.all([fetchCount(query), fetchWindow(0)]);
        setTotal(c);
        onTotal(c);
      } catch (e) {
        on401(e);
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    }, 0);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Trust the loaded rows over the reported count for sizing/empty checks — never let a bad count hide
  // real rows. `effTotal` is at least what we've actually loaded.
  const effTotal = Math.max(total ?? 0, windowStart + rows.length);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const firstVisible = Math.floor(el.scrollTop / ROW_H);
    const lastVisible = firstVisible + Math.ceil(el.clientHeight / ROW_H);
    const { start, end } = loaded.current;
    const needAbove = start > 0 && firstVisible < start + OVERSCAN;
    const needBelow = end < effTotal && lastVisible > end - OVERSCAN;
    if (needAbove || needBelow) {
      const newStart = Math.max(0, Math.min(firstVisible - OVERSCAN, Math.max(0, effTotal - CHUNK)));
      if (newStart !== start) void fetchWindow(newStart);
    }
  };

  if (error) return <div style={{ padding: 16, color: '#ef4444', fontSize: 12.5, whiteSpace: 'pre-wrap' }}>{error}</div>;
  if (loading) return <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>Loading…</div>;
  if (rows.length === 0 || columns.length === 0) return <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>No rows.</div>;

  // Hide the active-flag column; it drives row colour instead. Merge rows are clickable (→ history).
  // On a raw __changelog view the Z-set weight column is kept visible (relabelled Δ) but also tints
  // retractions (d < 0) in the BLOCKED theme.
  const activeIdx = columns.indexOf(ACTIVE);
  const deltaIdx = columns.indexOf(DELTA);
  const display = columns.map((_, i) => i).filter((i) => i !== activeIdx);
  const clickable = query.trickle === 'merge';
  const pkCols = query.pk ?? [];

  const topPad = windowStart * ROW_H;
  const botPad = Math.max(0, (effTotal - windowStart - rows.length) * ROW_H);
  const tableWidth = NUM_W + display.length * COL_W;

  return (
    <div ref={scrollRef} onScroll={onScroll} style={{ height: '100%', overflow: 'auto' }}>
      <table style={{ tableLayout: 'fixed', borderCollapse: 'collapse', width: tableWidth, fontSize: 12, color: '#d4d4d8' }}>
        <colgroup>
          <col style={{ width: NUM_W }} />
          {display.map((i) => <col key={i} style={{ width: COL_W }} />)}
        </colgroup>
        <thead style={{ position: 'sticky', top: 0, zIndex: 1 }}>
          <tr>
            <th style={th({ color: '#52525b', textAlign: 'right' })}>#</th>
            {display.map((i) => {
              const c = columns[i];
              const arrow = sort.col === c ? (sort.desc ? ' ▼' : ' ▲') : '';
              return (
                <th
                  key={i}
                  onClick={() => onSort(c)}
                  title={`${c} — click to sort`}
                  style={th({ cursor: 'pointer', userSelect: 'none', color: sort.col === c ? '#e4e4e7' : undefined })}
                >
                  {(COL_LABELS[c] ?? c) + arrow}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {topPad > 0 && (
            <tr style={{ height: topPad }}><td colSpan={display.length + 1} style={{ padding: 0, border: 0 }} /></tr>
          )}
          {rows.map((row, i) => {
            const idx = windowStart + i;
            const inactive =
              (activeIdx >= 0 && Number(row[activeIdx]) < 0) || (deltaIdx >= 0 && Number(row[deltaIdx]) < 0);
            return (
              <tr
                key={idx}
                onClick={clickable ? () => onRowClick(Object.fromEntries(pkCols.map((c) => [c, row[columns.indexOf(c)]]))) : undefined}
                style={{
                  height: ROW_H,
                  cursor: clickable ? 'pointer' : 'default',
                  background: inactive ? `${THEME_BLOCKED}22` : idx % 2 ? '#121217' : 'transparent',
                  color: inactive ? '#fca5a5' : undefined,
                }}
              >
                <td style={td({ color: '#52525b', textAlign: 'right' })}>{idx + 1}</td>
                {display.map((ci) => (
                  <td key={ci} style={td({})} title={row[ci] == null ? '' : String(row[ci])}>
                    {row[ci] == null
                      ? <span style={{ color: '#3f3f46' }}>·</span>
                      : columns[ci] === FRESH ? fmtTs(String(row[ci])) : String(row[ci])}
                  </td>
                ))}
              </tr>
            );
          })}
          {botPad > 0 && (
            <tr style={{ height: botPad }}><td colSpan={display.length + 1} style={{ padding: 0, border: 0 }} /></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

// ─── Per-record history (merge) ──────────────────────────────────────────────

function HistoryOverlay({
  pond, pondName, table, pk, onClose,
}: {
  pond: PondId;
  pondName: string;
  table: string;
  pk: Record<string, unknown>;
  onClose: () => void;
}) {
  const [data, setData] = useState<PageResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const t = setTimeout(async () => {
      try {
        setData(await fetchHistory(pond, table, pk));
      } catch (e) {
        on401(e);
        setError(e instanceof Error ? e.message : String(e));
      }
    }, 0);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const cols = data?.columns ?? [];
  const label = Object.entries(pk).map(([k, v]) => `${k}=${String(v)}`).join(', ');

  return (
    <div
      onClick={onClose}
      style={{
        position: 'absolute', inset: 0, zIndex: 10, display: 'flex', alignItems: 'center', justifyContent: 'center',
        background: 'rgba(9, 9, 11, 0.6)',
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: '#101014', border: '1px solid #3f3f46', borderRadius: 10, width: '70%', maxHeight: '76%',
          display: 'flex', flexDirection: 'column', overflow: 'hidden',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', borderBottom: '1px solid #27272a', flexShrink: 0 }}>
          <span style={{ fontSize: 12.5, fontWeight: 700, color: '#e4e4e7' }}>History</span>
          <span style={{ fontSize: 11.5, color: '#71717a' }}>{pondName}.{table} · {label}</span>
          <button
            onClick={onClose}
            style={{
              marginLeft: 'auto', background: 'transparent', border: '1px solid #3f3f46', borderRadius: 5,
              color: '#a1a1aa', fontSize: 12, lineHeight: 1, padding: '3px 8px', cursor: 'pointer', fontFamily: 'inherit',
            }}
          >
            ✕
          </button>
        </div>
        <div style={{ overflow: 'auto', minHeight: 0 }}>
          {error ? (
            <div style={{ padding: 16, color: '#ef4444', fontSize: 12.5 }}>{error}</div>
          ) : data == null ? (
            <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>Loading…</div>
          ) : data.rows.length === 0 ? (
            <div style={{ padding: 16, color: '#71717a', fontSize: 12.5 }}>No changelog entries.</div>
          ) : (
            <table style={{ borderCollapse: 'collapse', fontSize: 12, color: '#d4d4d8', width: '100%' }}>
              <thead style={{ position: 'sticky', top: 0 }}>
                <tr>
                  {cols.map((c) => <th key={c} style={th({})} title={c}>{COL_LABELS[c] ?? c}</th>)}
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row, i) => {
                  const ev = String(row[cols.indexOf(EVENT)] ?? '');
                  return (
                    <tr key={i} style={{ height: ROW_H, background: ev === 'delete' ? `${THEME_BLOCKED}22` : i % 2 ? '#121217' : 'transparent' }}>
                      {cols.map((c, j) => (
                        <td key={j} style={td({})} title={row[j] == null ? '' : String(row[j])}>
                          {row[j] == null ? <span style={{ color: '#3f3f46' }}>·</span>
                            : c === FRESH ? fmtTs(String(row[j]))
                            : c === EVENT ? <span style={{ color: EVENT_COLOR[ev], fontWeight: 700 }}>{ev}</span>
                            : String(row[j])}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}


function th(extra: React.CSSProperties): React.CSSProperties {
  return {
    textAlign: 'left', padding: '0 12px', height: ROW_H, lineHeight: `${ROW_H}px`,
    borderBottom: '1px solid #3f3f46', background: '#1a1a1f', color: '#a1a1aa', fontWeight: 700,
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', ...extra,
  };
}

function td(extra: React.CSSProperties): React.CSSProperties {
  return {
    padding: '0 12px', height: ROW_H, lineHeight: `${ROW_H}px`,
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', ...extra,
  };
}
