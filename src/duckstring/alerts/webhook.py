"""The webhook notifier (``http``/``https``) — the highest-leverage channel.

POSTs a JSON body that is both a plain structured event **and** Slack-incoming-webhook compatible: a
top-level ``text`` summary (which Slack renders, and a generic receiver can read) plus the full structured
event. So one driver covers Slack, a generic webhook receiver, and (via a proxy) PagerDuty's Events API.
Any credential/token in the URL is a ``${env:}``/``${secret:}`` reference, resolved only at send time.
"""

from __future__ import annotations

from .base import Destination, NotifierError
from .event import AlertEvent

_TIMEOUT = 15.0


class WebhookNotifier:
    SCHEMES = ("http", "https")

    def __init__(self, dest: Destination):
        self.dest = dest

    def _post(self, event: AlertEvent) -> None:
        import httpx

        from ..egress import credentials

        url = credentials.resolve(self.dest.raw)  # resolve any ${env:}/${secret:} token — never logged
        if event.kind == "openlineage" and event.detail.get("event"):
            body = event.detail["event"]  # a standard OpenLineage RunEvent — posted verbatim, no wrapper
        else:
            body = {"text": event.summary(), **event.to_payload()}  # `text` for Slack; the rest generic
        try:
            resp = httpx.post(url, json=body, timeout=_TIMEOUT)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # Sanitise: report status + reason, never the URL (it may carry a token in the path/query).
            raise NotifierError(f"webhook returned {exc.response.status_code}") from None
        except httpx.HTTPError as exc:
            raise NotifierError(f"webhook delivery failed: {type(exc).__name__}") from None

    def send(self, event: AlertEvent) -> None:
        self._post(event)

    def test(self) -> None:
        self._post(AlertEvent(
            kind="recovery", pond=None, title="Duckstring alert channel test",
            message="This is a test notification — your alert channel is configured correctly.",
        ))
