"""The alert event model + the event-kind vocabulary (see plans/alerts.md).

An :class:`AlertEvent` is the rendered, **sanitised** payload a notifier delivers. It reuses what
``/api/runs`` surfaces (error message, freshness, pond) but never a raw traceback — a channel destination
can be third-party, and a traceback can leak paths/connection strings (the same concern behind the API's
``_redact_tracebacks``). Keep it JSON-serialisable: it is stored verbatim in the ``alert_delivery`` outbox.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

# The event vocabulary. `freshness` is tick-driven (the SLA sweep); the rest are transition-driven.
KNOWN_EVENTS = ("failure", "contract", "spout", "recovery", "freshness")

# Machine-consumer kinds: valid subscriptions, but deliberately EXCLUDED from the `all` expansion —
# an ops Slack channel subscribed to `all` must never receive raw catalog events. `openlineage` carries
# a standard OpenLineage RunEvent per completed Pond Run (plans/lineage.md Phase 4); the webhook
# notifier posts it verbatim (no Slack `text` wrapper).
EXPLICIT_EVENTS = ("openlineage",)

# Default severity per kind (a channel filters by kind, not severity — this is only for the payload/label).
SEVERITY = {
    "failure": "error",
    "contract": "error",
    "spout": "error",
    "freshness": "warning",
    "recovery": "info",
    "openlineage": "info",
}


def normalise_events(events: str | None) -> tuple[str, ...]:
    """Parse a channel's ``events`` CSV (or ``all``/empty) into a validated tuple of known kinds.

    ``all`` / empty → every *notification* kind (machine-consumer kinds like ``openlineage`` need an
    explicit subscription). Raises :class:`ValueError` naming an unknown kind."""
    if not events or events.strip().lower() == "all":
        return KNOWN_EVENTS
    out = []
    for raw in events.split(","):
        kind = raw.strip().lower()
        if not kind:
            continue
        if kind not in KNOWN_EVENTS and kind not in EXPLICIT_EVENTS:
            raise ValueError(
                f"unknown alert event {kind!r} — choose from "
                f"{', '.join(KNOWN_EVENTS + EXPLICIT_EVENTS)} (or 'all')"
            )
        out.append(kind)
    if not out:
        return KNOWN_EVENTS
    return tuple(dict.fromkeys(out))  # de-duplicated, order preserved


@dataclass
class AlertEvent:
    """A rendered notification. ``detail`` carries kind-specific extras (e.g. the blocked-downstream blast
    radius for a failure); never put a traceback in it."""

    kind: str
    pond: str | None
    title: str
    message: str
    severity: str = ""
    f: str | None = None
    catchment: str | None = None
    ts: str = ""
    detail: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.severity:
            self.severity = SEVERITY.get(self.kind, "info")
        if not self.ts:
            self.ts = datetime.now(timezone.utc).isoformat()

    def summary(self) -> str:
        """A one-line human summary (the webhook ``text`` / the email subject line)."""
        where = f" [{self.catchment}]" if self.catchment else ""
        return f"{self.severity.upper()}{where}: {self.title}"

    def to_payload(self) -> dict:
        return {
            "kind": self.kind,
            "severity": self.severity,
            "pond": self.pond,
            "f": self.f,
            "title": self.title,
            "message": self.message,
            "catchment": self.catchment,
            "ts": self.ts,
            "detail": self.detail,
        }

    @classmethod
    def from_payload(cls, payload: dict) -> "AlertEvent":
        return cls(
            kind=payload.get("kind", ""),
            pond=payload.get("pond"),
            title=payload.get("title", ""),
            message=payload.get("message", ""),
            severity=payload.get("severity", ""),
            f=payload.get("f"),
            catchment=payload.get("catchment"),
            ts=payload.get("ts", ""),
            detail=payload.get("detail") or {},
        )
