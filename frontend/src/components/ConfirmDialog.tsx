'use client';

import { useEffect, useState } from 'react';
import { THEME_BLOCKED } from '@/lib/store';

// A destructive-confirmation request. `action` is the work to run on confirm (the dialog awaits it, then
// closes). `requireTyped`, when set, gates the confirm button behind the user typing that exact string —
// for the heaviest, irreversible actions (a whole-Catchment reset).
export type ConfirmOpts = {
  title: string;
  body: string;
  confirmLabel: string;
  requireTyped?: string;
  action: () => Promise<void> | void;
};

// A centred, themed modal confirmation (replaces window.confirm). Fixed to the viewport (centres on screen
// even when opened over another modal), Escape / click-outside to cancel, a busy state while the action runs.
export function ConfirmDialog({ opts, onClose }: { opts: ConfirmOpts; onClose: () => void }) {
  const [busy, setBusy] = useState(false);
  const [typed, setTyped] = useState('');
  const gated = !!opts.requireTyped;
  const canConfirm = !busy && (!gated || typed === opts.requireTyped);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !busy) onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [busy, onClose]);

  const go = async () => {
    if (!canConfirm) return;
    setBusy(true);
    try {
      await opts.action();
    } finally {
      onClose();
    }
  };

  return (
    <div
      onClick={() => !busy && onClose()}
      style={{
        position: 'fixed', inset: 0, zIndex: 2000, display: 'flex', alignItems: 'center',
        justifyContent: 'center', background: 'rgba(9, 9, 11, 0.72)', padding: 16,
        fontFamily: 'ui-monospace, SFMono-Regular, monospace',
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        role="alertdialog"
        aria-label={opts.title}
        style={{
          background: '#101014', border: '1px solid #3f3f46', borderRadius: 10,
          width: 'min(440px, 92vw)', padding: '18px 18px 16px', boxShadow: '0 10px 40px rgba(0,0,0,0.5)',
        }}
      >
        <div style={{ fontSize: 13.5, fontWeight: 700, color: '#e4e4e7', marginBottom: 8 }}>{opts.title}</div>
        <div style={{ fontSize: 12.5, color: '#a1a1aa', lineHeight: 1.55, marginBottom: gated ? 12 : 16, whiteSpace: 'pre-wrap' }}>
          {opts.body}
        </div>
        {gated && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 11.5, color: '#71717a', marginBottom: 6 }}>
              Type <span style={{ color: '#e4e4e7', fontWeight: 700 }}>{opts.requireTyped}</span> to confirm.
            </div>
            <input
              autoFocus
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') void go(); }}
              spellCheck={false}
              style={{
                width: '100%', boxSizing: 'border-box', background: '#18181b',
                border: `1px solid ${canConfirm && gated ? THEME_BLOCKED : '#3f3f46'}`, borderRadius: 6,
                padding: '8px 10px', color: '#e4e4e7', fontSize: 12.5, fontFamily: 'inherit', outline: 'none',
              }}
            />
          </div>
        )}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          <button
            onClick={onClose}
            disabled={busy}
            style={{
              background: 'transparent', border: '1px solid #3f3f46', borderRadius: 6, padding: '8px 15px',
              color: '#a1a1aa', fontSize: 12.5, fontWeight: 700, cursor: busy ? 'default' : 'pointer',
              fontFamily: 'inherit',
            }}
          >
            Cancel
          </button>
          <button
            onClick={go}
            disabled={!canConfirm}
            style={{
              background: THEME_BLOCKED, border: 'none', borderRadius: 6, padding: '8px 16px',
              color: canConfirm ? '#f4f4f5' : '#71717a', fontSize: 12.5, fontWeight: 700,
              cursor: canConfirm ? 'pointer' : 'default', fontFamily: 'inherit', opacity: canConfirm ? 1 : 0.6,
            }}
          >
            {busy ? '…' : opts.confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
