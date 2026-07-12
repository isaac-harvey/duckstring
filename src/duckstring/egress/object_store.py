"""Object-store egress driver (see plans/egress.md "Object-store egress").

Two write shapes, chosen by the Spout's ``mode``:

- **snapshot** (``mode=full``/``auto``): each egressed table as one Parquet file under the destination
  prefix (``{prefix}/{table}.parquet``) — the simplest correct "land my Pond's output over there".
- **mirror** (``mode=append``): the table's **published Duckstring collection** (per-run parts +
  changelog/band/base tiers + the sidecar + the Extension-1 ``state/`` snapshots), reconciled by file
  name per delivery — O(new parts), and the destination stays directly readable by any
  Duckstring-layout consumer (``ParquetDataPlane.read_select``, a downstream Catchment, DuckFlock).

- ``file://`` — local path, written atomically (tmp + ``os.replace``).
- ``s3://`` / ``gs://`` — snapshots via DuckDB ``httpfs`` + the secret manager; mirrors via the
  :mod:`duckstring.storage` fsspec layer (byte-copies, listing, deletes). Credentials come from the URI
  query (``?key_id=${env:..}&secret=${env:..}&region=..``, resolved at egress time); with none given,
  ``s3://`` falls back to the AWS credential chain (env / instance profile). A single PUT / completed
  multipart upload is effectively atomic, so target keys are written directly.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import credentials
from .base import Capabilities
from .destination import Destination

_SECRET = "__duckstring_egress"  # transient per-connection secret the COPY uses


def _safe_table(name: str) -> str:
    if not name or name in (".", "..") or "/" in name or "\\" in name or os.sep in name:
        raise ValueError(f"unsafe table name for an object-store path: {name!r}")
    return name


def _q(value: str) -> str:
    """A single-quoted SQL string literal (doubling embedded quotes)."""
    return "'" + value.replace("'", "''") + "'"


class ObjectStoreEgressDriver:
    SCHEMES = ("file", "s3", "gs")

    def __init__(self, dest: Destination):
        self.dest = dest

    def capabilities(self) -> Capabilities:
        return Capabilities(supports_delta=False, supports_delete=False, transactional=False,
                            mirrors_layout=True)

    def ensure(self, *, table: str, schema: dict | None, pk: list[str] | None) -> None:
        if self.dest.scheme == "file":
            self._file_base().mkdir(parents=True, exist_ok=True)  # buckets must already exist for s3/gs

    def write_full(self, con, relation, *, table: str, pk: list[str] | None, f: datetime) -> None:
        t = _safe_table(table)
        if self.dest.scheme == "file":
            base = self._file_base()
            base.mkdir(parents=True, exist_ok=True)
            target = base / f"{t}.parquet"
            tmp = base / f".{t}.{os.getpid()}.tmp.parquet"  # same dir → atomic os.replace
            try:
                relation.write_parquet(str(tmp))
                os.replace(tmp, target)
            finally:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
        else:
            self._prepare_remote(con)
            relation.write_parquet(self._remote_target(t))  # httpfs upload — atomic on completion

    def apply_delta(self, delta, *, table: str, pk: list[str] | None, f: datetime) -> None:
        raise NotImplementedError(
            "object-store egress has no row-level apply_delta — use mode=append (mirror) or mode=full"
        )

    # ─── the incremental path: mirror the published collection (mode=append) ─────

    def mirror(self, source_store, table: str, entry: dict, *, f: datetime | None = None) -> None:
        """Incrementally mirror ``table``'s **published collection** from the source data dir to the
        destination prefix — the same layout the data plane publishes, so any Duckstring-layout reader
        (``ParquetDataPlane.read_select``, a downstream Catchment, DuckFlock) can consume the
        destination directly.

        Per delivery it reconciles each artifact set by **name** (parts are immutable + idempotent by
        name, so the file-set diff is the watermark — O(new parts), not O(history)): missing parts are
        copied, parts the source no longer holds are removed (a warm fold / cold compaction / retention
        moved their rows — leaving them would double-count a reconstruction), the current run's ``f``
        part is always re-copied (a same-``f`` replay may have rewritten its content), and wholesale
        files (an overwrite table; a legacy single-file merge base) copy only when the source entry's
        ``f``/``f_base`` advanced past the destination sidecar's. The Extension-1 ``state/`` snapshots
        ride along (they make the mirror DuckFlock-resumable). The destination sidecar entry is written
        **last** — the commit point, exactly like a publish."""
        from ..storage import get_storage
        from ..trickle.io import (
            DROPLOG_SUFFIX,
            base_dir_name,
            changelog_name,
            load_sidecar,
            part_name,
            warm_name,
            write_sidecar,
        )

        t = _safe_table(table)
        dest = get_storage(credentials.resolve(self.dest.raw))
        dest.mkdir()
        dest_sidecar = load_sidecar(dest)
        dest_entry = dest_sidecar.get(t) if isinstance(dest_sidecar.get(t), dict) else {}

        current_part = part_name(f) if f is not None else None
        for d in (t, changelog_name(t), warm_name(t), base_dir_name(t), f"{t}{DROPLOG_SUFFIX}"):
            src_names = set(source_store.parquet_names(d))
            dst_names = set(dest.parquet_names(d))
            copy = (src_names - dst_names) | ({current_part} if current_part in src_names else set())
            if copy:
                dest.mkdir(d)
            for n in sorted(copy):
                dest.write_bytes(source_store.read_bytes(d, n), d, n)
            for n in sorted(dst_names - src_names):  # fold/compaction/retention reconcile
                dest.remove(d, n)
        for kind in ("agg", "acc"):  # Extension-1 accumulator snapshots (latest-only, same reconcile)
            src_names = set(source_store.parquet_names("state", kind, t))
            dst_names = set(dest.parquet_names("state", kind, t))
            if src_names - dst_names:
                dest.mkdir("state", kind, t)
            for n in sorted(src_names - dst_names):
                dest.write_bytes(source_store.read_bytes("state", kind, t, n), "state", kind, t, n)
            for n in sorted(dst_names - src_names):
                dest.remove("state", kind, t, n)
        # Wholesale file (overwrite output / legacy single-file merge base): gate the O(base) copy on the
        # sidecar watermark it rewrites under — `f` for an overwrite table, `f_base` for a merge base.
        if source_store.exists(f"{t}.parquet"):
            gate = "f_base" if entry.get("mode") == "merge" else "f"
            if not dest.exists(f"{t}.parquet") or dest_entry.get(gate) != entry.get(gate):
                dest.write_bytes(source_store.read_bytes(f"{t}.parquet"), f"{t}.parquet")
        elif dest.exists(f"{t}.parquet") and entry.get("mode") in ("merge", "append"):
            dest.remove(f"{t}.parquet")  # the source's base went chunked — drop the stale wholesale copy
        dest_sidecar[t] = dict(entry)  # last — the reader's commit point
        state_section = dest_sidecar.get("state")
        src_state = (load_sidecar(source_store).get("state") or {})
        mine = {k: v for k, v in src_state.items() if k.split("/", 1)[1] == t}
        if mine:
            dest_sidecar["state"] = {**(state_section or {}), **mine}
        write_sidecar(dest, dest_sidecar)

    def test_connection(self, con) -> None:
        """Probe without writing data: ``file://`` does a write+delete probe in the target directory
        (so it exactly tests the write permission egress needs); ``s3://``/``gs://`` configure httpfs +
        the secret and list the prefix (credentials resolve, the bucket is reachable + authorised)."""
        if self.dest.scheme == "file":
            base = self._file_base()
            base.mkdir(parents=True, exist_ok=True)
            probe = base / f".duckstring_egress_test.{os.getpid()}.tmp"
            try:
                probe.write_bytes(b"ok")
            finally:
                probe.unlink(missing_ok=True)
            return
        self._prepare_remote(con)  # loads httpfs + the secret (sanitised error on a bad credential)
        u = urlparse(credentials.resolve(self.dest.raw))
        if not u.netloc:
            raise ValueError(f"{self.dest.scheme}:// destination needs a bucket: {self.dest.scheme}://bucket/prefix")
        prefix = u.path.strip("/")
        pattern = f"{self.dest.scheme}://{u.netloc}/{prefix}/*" if prefix else f"{self.dest.scheme}://{u.netloc}/*"
        try:
            # glob lists the prefix (0 rows for an empty/new prefix is fine) — surfaces auth/host errors.
            # The pattern carries no credentials (those live in the secret), so the error is safe to show.
            con.execute(f"SELECT 1 FROM glob({_q(pattern)}) LIMIT 1").fetchall()
        except Exception as exc:
            raise RuntimeError(f"could not reach the {self.dest.scheme} destination ({exc})") from None

    # ─── file:// ─────────────────────────────────────────────────────────────

    def _file_base(self) -> Path:
        """The local directory for a ``file://`` destination, ``${env:NAME}`` resolved (at egress time,
        never stored/logged)."""
        u = urlparse(credentials.resolve(self.dest.raw))
        if u.netloc and u.netloc not in ("", "localhost"):
            raise ValueError(
                f"file:// destination must be an absolute local path (file:///path); got host {u.netloc!r}"
            )
        return Path(u.path)

    # ─── s3:// / gs:// ───────────────────────────────────────────────────────

    def _remote_target(self, table: str) -> str:
        """``{scheme}://{bucket}/{prefix}/{table}.parquet`` — the target key, with no credential query
        (those live in the secret), so an error message can never echo a secret."""
        u = urlparse(credentials.resolve(self.dest.raw))
        if not u.netloc:
            raise ValueError(f"{self.dest.scheme}:// destination needs a bucket: {self.dest.scheme}://bucket/prefix")
        prefix = u.path.strip("/")
        key = f"{prefix}/{table}.parquet" if prefix else f"{table}.parquet"
        return f"{self.dest.scheme}://{u.netloc}/{key}"

    def _secret_sql(self) -> str:
        """A ``CREATE OR REPLACE SECRET`` from the destination's query params (``${env}`` resolved).
        s3 with no key falls back to ``credential_chain``; gs requires HMAC key_id + secret."""
        u = urlparse(credentials.resolve(self.dest.raw))
        q = {k.lower(): v[0] for k, v in parse_qs(u.query).items()}
        clauses: list[str] = []

        def add(qkey: str, name: str) -> None:
            if qkey in q:
                clauses.append(f"{name} {_q(q[qkey])}")

        if self.dest.scheme == "gs":
            if "key_id" not in q or "secret" not in q:
                raise ValueError(
                    "gs:// egress needs HMAC credentials in the URI: ?key_id=${env:...}&secret=${env:...}"
                )
            clauses += ["TYPE gcs"]
            add("key_id", "KEY_ID")
            add("secret", "SECRET")
        else:  # s3
            clauses.append("TYPE s3")
            if "key_id" in q or "secret" in q:
                add("key_id", "KEY_ID")
                add("secret", "SECRET")
                add("session_token", "SESSION_TOKEN")
            else:
                clauses.append("PROVIDER credential_chain")
            add("region", "REGION")
            add("endpoint", "ENDPOINT")
            add("url_style", "URL_STYLE")
            if "use_ssl" in q:
                clauses.append(f"USE_SSL {'true' if q['use_ssl'].lower() in ('1', 'true', 'yes') else 'false'}")
        return f"CREATE OR REPLACE SECRET {_SECRET} (" + ", ".join(clauses) + ")"

    def _prepare_remote(self, con) -> None:
        con.execute("INSTALL httpfs; LOAD httpfs")  # offline/install errors surface (no credentials in them)
        secret_sql = self._secret_sql()  # ValueError (gs creds) / CredentialError (missing env) surface — safe
        try:
            con.execute(secret_sql)
        except Exception:
            # The statement carries resolved credentials; never let a driver error echo them.
            raise RuntimeError(
                f"failed to configure {self.dest.scheme} credentials (check the destination's auth params)"
            ) from None
