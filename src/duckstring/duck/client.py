"""CatchmentClient — the Duck's HTTP link to the Catchment.

Catchment→Duck is a long-poll the Duck holds open (``GET /api/duck/{pond}/jobs``) for ``begin_run`` /
``shutdown`` commands; Duck→Catchment is a plain POST per event. Both are Duck-initiated, so the same
client works identically for a local subprocess and (later) a remote Duck. All calls are best-effort:
failures are swallowed so the run ledger remains the durable source of truth and events buffer for
replay.
"""

from __future__ import annotations

import httpx


class CatchmentClient:
    def __init__(self, base_url: str, pond_name: str, major: int, token: str, poll_timeout: float = 25.0):
        self.base = base_url.rstrip("/")
        self.pond = pond_name
        self.major = major
        self.token = token
        self.poll_timeout = poll_timeout
        self._client = httpx.Client(timeout=poll_timeout + 5.0, headers={"X-Duck-Token": token})

    def poll_jobs(self) -> list[dict]:
        """Long-poll for commands. Returns a list of ``{"kind": "begin_run", "f": ...}`` /
        ``{"kind": "shutdown"}`` dicts; empty on timeout. Returns ``[]`` on any transport error."""
        try:
            r = self._client.get(
                f"{self.base}/api/duck/{self.pond}/{self.major}/jobs", params={"wait": self.poll_timeout}
            )
            r.raise_for_status()
            return r.json().get("jobs", [])
        except Exception:
            return []

    def fetch_artifact(self) -> bytes:
        """Fetch the deployed version's source bundle (an uncompressed tar) — the remote-Duck boot
        path (prereqs D6): a Duck that can't read the Catchment's disk pulls its code over the duck
        channel. NOT best-effort — a Duck without its source cannot serve, so this raises."""
        r = self._client.get(f"{self.base}/api/duck/{self.pond}/{self.major}/artifact", timeout=60.0)
        r.raise_for_status()
        return r.content

    def post_event(self, payload: dict) -> bool:
        """Deliver one buffered event. Returns True on success (so the Duck drops it from the buffer)."""
        try:
            r = self._client.post(f"{self.base}/api/duck/{self.pond}/{self.major}/events", json=payload)
            r.raise_for_status()
            return True
        except Exception:
            return False

    def close(self) -> None:
        self._client.close()
