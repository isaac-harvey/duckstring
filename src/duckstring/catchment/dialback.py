"""The shared remote dial-back address (plans/cloud-config.md §4b). Every remote Duck — EC2 or Fargate
— dials back to the *same* reachable Catchment URL, and the auto-relay that provides it for a local
Catchment is shared across the backends. So this holds the URL + the relay in one place; each remote
backend registers a drain callback, fired once when the URL becomes known (an explicit public URL, the
bind address when no relay is needed, or the relay's tunnel once it's up)."""

from __future__ import annotations


class RemoteDialback:
    def __init__(self, base_url: str | None, relay=None, public_url: str | None = None):
        self.relay = relay
        # Explicit public URL wins; else the relay provides it lazily; else (no relay) the bind address
        # is directly reachable. None = not yet known → remote spawns defer until it resolves.
        self.url = public_url or (None if relay is not None else base_url)
        self._drains: list = []

    def register(self, drain) -> None:
        """A remote backend registers its ``_drain_pending`` — called when the URL resolves."""
        self._drains.append(drain)

    def request(self) -> None:
        """A backend calls this on a remote spawn while the URL is unknown — brings up the relay once
        (in the background); its readiness resolves the URL and drains the pending spawns."""
        if self.url is None and self.relay is not None and self.relay.configured():
            self.relay.start_async(self._resolve)

    def set_base_url(self, url: str) -> None:
        """The dispatcher learned the Catchment's bind address. With no relay, that *is* the reachable
        dial-back URL; with a relay, the relay provides a different (public) one, so this is ignored."""
        if self.url is None and self.relay is None:
            self._resolve(url)

    def _resolve(self, url: str) -> None:
        self.url = url
        for drain in self._drains:
            drain()

    def stop(self) -> None:
        if self.relay is not None:
            self.relay.stop()
