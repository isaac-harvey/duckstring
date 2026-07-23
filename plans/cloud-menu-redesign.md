# Cloud menu redesign — a modal with coherent sections

Status: **to build.** The Cloud config outgrew its dropdown popup (creds + data root + pools + migration
progress all crammed in), and the two-UI split (onboarding EnableFlow vs the switcher) made basic actions
— like reverting to local — unreachable or wrongly gated. Redesign as a **full modal** with one section
per concern.

## Problems being fixed

- **Crowded popup.** Everything stacks in a narrow dropdown.
- **Revert-to-local gated behind a bucket.** Reverting to local disk needs no creds and no target, but the
  UI blocked it (the switcher only rendered when cloud was *enabled*, and the enable flow requires a root).
  A local Catchment could get stranded on a mis-typed root with no way back.
- **Two UIs for one action.** "Attach a data root" (EnableFlow) vs "switch the data root" (DataPlaneSection)
  are the same operation with different affordances and different gating.
- **Creds + data root entangled** in the onboarding wizard.

## Shape: `CloudModal` (replaces the popup)

A centered modal (backdrop + scrollable panel, the `PondOptionsModal` pattern), opened from Options →
Cloud. Sections, top to bottom:

1. **Status** — `enabled ✓` / `disabled`, with the gate reasons inline (remote data root? AWS creds?
   creds valid?). The single source of truth for "is cloud on".
2. **Data plane** — the current root, and **one** change form: a target field (blank = local) with the
   scheme/bare-name validation inline, the empty/adopt/migrate mode choice, the confirm field, and Apply.
   Plus a always-enabled **Revert to local** action (confirm only — no target, no creds; adopts the local
   data so the original is restored). The migration progress banner lives here.
3. **AWS credentials** — configured/valid status + a set/update form (access key / secret / region) + a
   Verify button (STS). Unifies the old enable-flow creds step and the FixCredentials fixup.
4. **Duck pools** — the pool list + add (Fargate cpu/mem dropdowns / EC2 instance picker). Noted (not
   hidden) when cloud is disabled, since a pool is inert until cloud is enabled.

Enabling is **emergent**, not a wizard: set a remote root (Data plane) + valid creds (Credentials) → the
gate flips. No separate onboarding path.

## Invariants

- **Revert to local is always available** and needs neither creds nor a target — just the confirm.
- **Any data-root change is confirmed** by the catchment name (it moves where data lives), and validated
  (object URI or absolute path; a bare name is rejected — see the backend guard).
- Non-destructive: the old location is always kept.

## Not changing

The backend surface (`PUT /api/catchment/settings` modes, `/migration`, `/instance-types`, `/cloud/verify`,
secrets) is unchanged — this is a frontend restructure. Reuses `SwitchModeChoice`, `MigrationBanner`,
`InstanceTypePicker`, the Fargate matrix, and the bare-root hint.
