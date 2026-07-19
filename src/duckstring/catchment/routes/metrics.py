"""``GET /metrics`` — a Prometheus text-exposition endpoint (see plans/alerts.md "Metrics").

Deliberately mounted at the **root** (`/metrics`, not `/api/metrics`) and **unauthenticated** — the
Prometheus posture every exporter takes; a scraper sends no key. It is therefore outside the ``/api``
access-level audit (which only classifies ``/api`` routes). Pond names are exposed as labels, so an
operator who considers those sensitive should network-restrict the endpoint (a scrape ACL / private
listener) rather than expose it publicly. No new dependency: the exposition format is hand-rendered.

Metric families (all `duckstring_*`):
  up                            — 1 while the Catchment is serving
  pond_freshness_lag_seconds    — now − end_f, per non-spout node (the headline "is it stale" signal)
  pond_failed/blocked/killed    — 0/1 state flags per Pond
  pond_runs_completed_total     — completed Pond Runs (rebuilt from the DB → monotonic across restarts)
  pond_failures_total           — cumulative failed Pond Runs per (pond, major)
  pond_run_seconds_total        — cumulative Duck execution seconds (summed Ripple-Run spans) — cost signal
  pond_duck_target              — 1 for where a Pond's Duck runs (catchment/pool/dedicated)
  pond_flock                    — 1 for a Pond's effective Flock posture (mode + engine)
  spout_delivery_lag_seconds    — now − delivered_f, per Spout
  spout_failed                  — 0/1 per Spout
  alert_deliveries_total        — queued notifications by status (sent/pending/failed)
"""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

router = APIRouter()


def _esc(v: str) -> str:
    """Escape a Prometheus label value (backslash, double-quote, newline)."""
    return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels(**kv: object) -> str:
    inner = ",".join(f'{k}="{_esc(str(v))}"' for k, v in kv.items())
    return "{" + inner + "}" if inner else ""


def render_metrics(snap: dict) -> str:
    """Render a :meth:`Driver.metrics_snapshot` dict as the Prometheus text exposition format."""
    out: list[str] = []

    def family(name: str, kind: str, help_text: str) -> None:
        out.append(f"# HELP duckstring_{name} {help_text}")
        out.append(f"# TYPE duckstring_{name} {kind}")

    def line(name: str, value: float, **labels: object) -> None:
        v = int(value) if float(value).is_integer() else value
        out.append(f"duckstring_{name}{_labels(**labels)} {v}")

    family("up", "gauge", "1 while the Catchment is serving.")
    line("up", 1)

    ponds = [n for n in snap["nodes"] if not n["is_spout"]]
    spouts = [n for n in snap["nodes"] if n["is_spout"]]

    family("pond_freshness_lag_seconds", "gauge", "Seconds since a Pond last became fresh (now − end_f).")
    for n in ponds:
        if n["lag_seconds"] is not None:
            line("pond_freshness_lag_seconds", n["lag_seconds"], pond=n["name"], major=n["major"], kind=n["kind"])

    for flag, help_text in (
        ("failed", "1 if the Pond's most recent Run failed."),
        ("blocked", "1 if the Pond is blocked by a down Source."),
        ("killed", "1 if the Pond was killed by an operator."),
    ):
        family(f"pond_{flag}", "gauge", help_text)
        for n in ponds:
            line(f"pond_{flag}", 1 if n[f"is_{flag}"] else 0, pond=n["name"], major=n["major"])

    family("pond_runs_completed_total", "counter", "Completed Pond Runs.")
    for n in ponds:
        line("pond_runs_completed_total", n["runs_completed"], pond=n["name"], major=n["major"])

    family("pond_failures_total", "counter", "Cumulative failed Pond Runs.")
    for n in ponds:
        line("pond_failures_total", snap["failures"].get((n["name"], n["major"]), 0), pond=n["name"], major=n["major"])

    # Compute-cost signals (plans/cloud-config.md): the Duck's cumulative execution time + where it runs
    # + its Flock posture, so a scrape can attribute spend across the fleet.
    family("pond_run_seconds_total", "counter", "Cumulative Duck execution seconds (summed Ripple-Run spans).")
    for n in ponds:
        line("pond_run_seconds_total", snap["runtimes"].get((n["name"], n["major"]), 0),
             pond=n["name"], major=n["major"])

    family("pond_duck_target", "gauge", "1 for the Duck target a Pond runs on (catchment/pool/dedicated).")
    for n in ponds:
        if "duck_target" in n:
            line("pond_duck_target", 1, pond=n["name"], major=n["major"], target=n["duck_target"])

    family("pond_flock", "gauge", "1 for a Pond's effective Flock posture (mode + engine).")
    for n in ponds:
        if "flock_mode" in n:
            line("pond_flock", 1, pond=n["name"], major=n["major"],
                 mode=n["flock_mode"], engine=n["flock_engine"])

    family("spout_delivery_lag_seconds", "gauge", "Seconds since a Spout last delivered (now − delivered_f).")
    for n in spouts:
        if n["lag_seconds"] is not None:
            line("spout_delivery_lag_seconds", n["lag_seconds"], spout=n["name"], major=n["major"])

    family("spout_failed", "gauge", "1 if a Spout's most recent delivery failed.")
    for n in spouts:
        line("spout_failed", 1 if n["is_failed"] else 0, spout=n["name"], major=n["major"])

    family("alert_deliveries_total", "counter", "Queued alert notifications by status.")
    for status in ("sent", "pending", "failed"):
        line("alert_deliveries_total", snap["alert_deliveries"].get(status, 0), status=status)

    return "\n".join(out) + "\n"


# text/plain in the Prometheus exposition content type (version 0.0.4).
_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


@router.get("/metrics")
def metrics(request: Request) -> Response:
    return Response(render_metrics(request.app.state.driver.metrics_snapshot()), media_type=_CONTENT_TYPE)
