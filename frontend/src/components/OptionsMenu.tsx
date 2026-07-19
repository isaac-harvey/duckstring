'use client';

import { Fragment, useState } from 'react';
import { useLiveStore, THEME_SUCCESS, THEME_DANGER } from '@/lib/store';
import { useIsMobile } from '@/lib/useIsMobile';
import type { AccessLevel } from '@/lib/api';
import { SecretsMenu } from './SecretsMenu';
import { AlertsMenu } from './AlertsMenu';
import { CloudMenu } from './CloudMenu';

// The caller's API access level as three capabilities — Manage | Demand | Read — each prefixed with a
// green ✓ when the key grants it, a grey – when not. (Moved off the always-on badge into the menu.)
const ACCESS_CHIPS: { label: string; active: (l: AccessLevel) => boolean; hint: string }[] = [
  { label: 'Manage', active: (l) => l === 'full', hint: 'Deploy, control, and manage secrets.' },
  { label: 'Demand', active: (l) => l === 'full' || l === 'demand', hint: 'Create demand: tap, wave, pulse, tide.' },
  { label: 'Read', active: () => true, hint: 'Read status and query data.' },
];

function AccessChips() {
  const level = useLiveStore((s) => s.accessLevel);
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, letterSpacing: '0.02em', padding: '2px 2px' }}>
      {ACCESS_CHIPS.map(({ label, active, hint }, i) => {
        const on = active(level);
        return (
          <Fragment key={label}>
            {i > 0 && <span style={{ color: '#3f3f46' }}>|</span>}
            <span title={hint} style={{ display: 'inline-flex', alignItems: 'center', gap: 3, color: on ? '#e4e4e7' : '#3f3f46' }}>
              <span style={{ color: on ? THEME_SUCCESS : '#3f3f46' }}>{on ? '✓' : '–'}</span>
              {label}
            </span>
          </Fragment>
        );
      })}
    </div>
  );
}

// A menu leaf's panel (Secrets, Alerts): a popout to the right of the menu on desktop, a full-screen
// modal on mobile (the Secrets behaviour). The child owns its own card chrome + ✕ (calls onClose).
function Popout({ onClose, children }: { onClose: () => void; children: React.ReactNode }) {
  const isMobile = useIsMobile();
  if (isMobile) {
    return (
      <div
        onClick={onClose}
        style={{
          position: 'fixed', inset: 0, zIndex: 1200, display: 'flex', alignItems: 'flex-start',
          justifyContent: 'center', background: 'rgba(9,9,11,0.72)', padding: '10vh 12px 12px',
          overflowY: 'auto',
        }}
      >
        <div onClick={(e) => e.stopPropagation()} style={{ width: 'min(420px, 94vw)' }}>{children}</div>
      </div>
    );
  }
  return (
    <div style={{ position: 'absolute', top: 0, left: '100%', marginLeft: 8, zIndex: 20, width: 260 }}>
      {children}
    </div>
  );
}

const menuItem: React.CSSProperties = {
  display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8,
  width: '100%', boxSizing: 'border-box', background: 'transparent', border: 'none',
  borderRadius: 6, padding: '8px 10px', fontFamily: 'inherit', fontSize: 12, color: '#d4d4d8',
  cursor: 'pointer', textAlign: 'left',
};

// The top-left panel: a single bar — Logo | Name | Status ● | ☰ Options — with the Options menu
// dropping beneath it at the bar's width. Replaces the old StatusPanel + top-right ControlsPanel.
export function TopBar() {
  const connected = useLiveStore((s) => s.connected);
  const error = useLiveStore((s) => s.error);
  const catchment = useLiveStore((s) => s.catchment);
  const accessLevel = useLiveStore((s) => s.accessLevel);
  const collapsedPonds = useLiveStore((s) => s.collapsedPonds);
  const setAllCollapsed = useLiveStore((s) => s.setAllCollapsed);
  const enterSelector = useLiveStore((s) => s.enterSelector);
  // Stable key over the collapsible Pond ids — avoids re-rendering on every poll.
  const collapsibleKey = useLiveStore((s) =>
    [...new Set(Object.values(s.ripples).map((r) => r.pondId))].sort().join(',')
  );

  const [open, setOpen] = useState(false);
  const [panel, setPanel] = useState<'secrets' | 'alerts' | 'cloud' | null>(null);
  const isMobile = useIsMobile();

  const label = catchment?.name || (catchment?.id ? catchment.id.slice(0, 8) : 'Catchment');
  const isFull = accessLevel === 'full';
  const collapsibleIds = collapsibleKey ? collapsibleKey.split(',') : [];
  const allCollapsed = collapsibleIds.length > 0 && collapsibleIds.every((id) => collapsedPonds[id]);
  const statusText = connected ? null : error ? 'unreachable' : 'connecting…';

  const close = () => { setOpen(false); setPanel(null); };

  return (
    // On mobile the bar spans the full page width, pinned to the top (a page header); on desktop it is a
    // floating top-left card.
    <div style={{
      position: 'relative', display: isMobile ? 'flex' : 'inline-flex', width: isMobile ? '100%' : undefined,
      flexDirection: 'column', fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    }}>
      {/* The bar: a big logo alongside a two-row identity (name + status, then the access chips), with
          the Options button on the right. The two rows give the logo room without a tall single line. */}
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 7, background: '#15151a',
          border: isMobile ? 'none' : '1px solid #27272a', borderBottom: '1px solid #27272a',
          borderRadius: isMobile ? 0 : open ? '8px 8px 0 0' : 8, padding: '4px 12px 4px 6px', fontSize: 12, color: '#a1a1aa',
        }}
      >
        <span
          aria-label="Duckstring"
          style={{
            width: isMobile ? 48 : 44, height: isMobile ? 48 : 44, flexShrink: 0,
            backgroundImage: 'url(/logo-mark.svg)', backgroundSize: 'contain',
            backgroundRepeat: 'no-repeat', backgroundPosition: 'center',
          }}
        />
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
            {/* The status dot sits before the name so its position is fixed regardless of name length. */}
            <span
              title={connected ? 'connected' : error ? 'unreachable' : 'connecting'}
              style={{ width: 8, height: 8, flexShrink: 0, borderRadius: '50%', background: connected ? THEME_SUCCESS : THEME_DANGER }}
            />
            <span title={catchment?.id ?? undefined} style={{ fontSize: 13, fontWeight: 700, color: '#f4f4f5', whiteSpace: 'nowrap' }}>
              {label}
            </span>
            {statusText && <span style={{ color: '#71717a', fontSize: 11 }}>{statusText}</span>}
          </div>
          <AccessChips />
        </div>
        <button
          aria-label="Options"
          onClick={() => (open ? close() : setOpen(true))}
          style={{
            marginLeft: 'auto', alignSelf: 'stretch', background: open ? '#27272a' : 'transparent',
            border: '1px solid #27272a', borderRadius: 6, padding: '0 9px', color: open ? '#e4e4e7' : '#a1a1aa',
            cursor: 'pointer', fontFamily: 'inherit', fontSize: 14, lineHeight: 1,
          }}
        >
          ☰
        </button>
      </div>

      {/* The dropdown, beneath the bar at the bar's width */}
      {open && (
        <div
          style={{
            position: 'relative', background: '#15151a', border: '1px solid #27272a', borderTop: 'none',
            borderRadius: '0 0 8px 8px', padding: 5, display: 'flex', flexDirection: 'column', gap: 2,
            minWidth: 190,
          }}
        >
          {collapsibleIds.length > 0 && (
            <button style={menuItem} onClick={() => setAllCollapsed(!allCollapsed)}>
              <span>{allCollapsed ? 'Expand all' : 'Collapse all'}</span>
              <span style={{ color: '#71717a', fontSize: 11 }}>{allCollapsed ? '▸' : '▾'}</span>
            </button>
          )}
          {isFull && (
            <button
              style={{ ...menuItem, color: panel === 'secrets' ? '#e4e4e7' : '#d4d4d8' }}
              onClick={() => setPanel(panel === 'secrets' ? null : 'secrets')}
            >
              <span>Secrets</span>
              <span style={{ color: '#71717a', fontSize: 11 }}>›</span>
            </button>
          )}
          {isFull && (
            <button
              style={{ ...menuItem, color: panel === 'alerts' ? '#e4e4e7' : '#d4d4d8' }}
              onClick={() => setPanel(panel === 'alerts' ? null : 'alerts')}
            >
              <span>Alerts</span>
              <span style={{ color: '#71717a', fontSize: 11 }}>›</span>
            </button>
          )}
          {isFull && (
            <button
              style={{ ...menuItem, color: panel === 'cloud' ? '#e4e4e7' : '#d4d4d8' }}
              onClick={() => setPanel(panel === 'cloud' ? null : 'cloud')}
            >
              <span>Cloud</span>
              <span style={{ color: '#71717a', fontSize: 11 }}>›</span>
            </button>
          )}
          {isFull && (
            <button style={{ ...menuItem, color: '#d4d4d8' }} onClick={() => { enterSelector(); close(); }}>
              <span>Pond Actions…</span>
            </button>
          )}

          {panel === 'secrets' && <Popout onClose={() => setPanel(null)}><SecretsMenu onClose={() => setPanel(null)} /></Popout>}
          {panel === 'alerts' && <Popout onClose={() => setPanel(null)}><AlertsMenu onClose={() => setPanel(null)} /></Popout>}
          {panel === 'cloud' && <Popout onClose={() => setPanel(null)}><CloudMenu onClose={() => setPanel(null)} /></Popout>}
        </div>
      )}
    </div>
  );
}
