"""The **storage seam** — a directory-shaped location the data plane writes its blobs into.

The Catchment runtime splits along two storage classes (see ``plans/storage-decoupling.md``):

- **Hot state** (``duck.db``, the per-Pond ``pond.db`` ledgers, the ``registry.duckdb`` working
  registries, ``config.toml``) needs POSIX semantics — byte-range writes, ``fsync``, advisory locking —
  so it **always** lives on a local POSIX path and never routes through this seam.
- **Data blobs** (Parquet parts, Iceberg metadata + data files, ``__base/`` chunks, ``__band/`` bands,
  the ``_trickle.json`` sidecars, the catalog) are write-once / atomic-overwrite objects that only ever
  need an object-level atomic PUT — so they can live in an object store (S3/GCS/ABFS) or a Databricks
  Volume. This seam is what lets ``DUCKSTRING_DATA_ROOT`` be a URI rather than a local path.

Two implementations:

- :class:`LocalStorage` — today's behaviour exactly: a ``Path`` root with ``tmp + os.replace`` atomic
  writes. A Databricks Volume mounted at ``/Volumes/…`` is just a local path here (the zero-config
  fallback), as is the default (the state root's ``ponds/``).
- :class:`ObjectStorage` — ``fsspec``-backed (``s3://``, ``gs://``, ``abfss://``, the ``databricks``
  filesystem). An atomic-overwrite is a single-object PUT — object stores make that atomic, so the
  rename dance is unnecessary; ``list`` is a list-prefix and ``delete`` is a delete-object.

Bulk Parquet I/O (``COPY … TO`` / ``read_parquet`` / ``iceberg_scan``) does **not** go byte-by-byte
through this seam — DuckDB reads and writes object storage natively over ``httpfs``. So the seam returns
**URIs** (:meth:`Storage.uri` / :meth:`Storage.glob`) that a DuckDB statement targets directly, and
implements only the filesystem-shaped operations around them (list / exists / size / delete / atomic
single-file commit). :meth:`Storage.duckdb_setup` configures a connection's credentials once.

Single-writer-per-line (the "run exactly ONE app process" invariant) is what makes object-store commits
safe without distributed locks — there is never a competing writer to a given ``name@major`` line.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Iterator
from urllib.parse import urlsplit, urlunsplit

# URI schemes that mean "object store / fsspec", as opposed to a local POSIX path. A bare path or a
# ``file://`` URI is local; everything else routes through fsspec + DuckDB httpfs.
_OBJECT_SCHEMES = {"s3", "s3a", "gs", "gcs", "abfs", "abfss", "az", "wasb", "wasbs", "databricks", "dbfs"}


def _split_query(uri: str) -> tuple[str, dict[str, str]]:
    """Split a data-root URI into ``(base_without_query, query_params)``. Credentials and options ride
    the query string (``s3://bucket/p?region=eu-west-1&key_id=${env:K}&secret=${env:S}``); they are
    stripped from the URI DuckDB/fsspec address and resolved separately at runtime."""
    parts = urlsplit(uri)
    params: dict[str, str] = {}
    if parts.query:
        from urllib.parse import parse_qsl

        params = dict(parse_qsl(parts.query, keep_blank_values=True))
    base = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    return base, params


def is_object_uri(uri: str) -> bool:
    """Whether ``uri`` addresses an object store (fsspec scheme) rather than a local POSIX path. A bare
    path, ``~``-path, or ``file://`` URI is local; ``s3://`` / ``gs://`` / ``abfss://`` / ``databricks://``
    etc. are object stores."""
    scheme = urlsplit(uri).scheme.lower()
    return scheme in _OBJECT_SCHEMES


def get_storage(location: str | os.PathLike, params: dict[str, str] | None = None) -> "Storage":
    """The :class:`Storage` for a data-root ``location`` (a local path or an object-store URI). Any
    credential/option query params already split off the URI are passed back in via ``params``."""
    text = os.fspath(location)
    if is_object_uri(text):
        base, q = _split_query(text)
        merged = {**q, **(params or {})}
        return ObjectStorage(base, merged)
    # A local path or file:// URI.
    if text.startswith("file://"):
        text = urlsplit(text).path
    return LocalStorage(Path(text).expanduser())


class Storage:
    """A directory location the data plane reads and writes blobs in. Address files/subdirs beneath it
    by **POSIX-relative** name parts (``"table"``, ``"part.parquet"``, ``"t__base"``). ``None``/no parts
    addresses the directory itself."""

    is_local: bool = True

    # ─── addressing ──────────────────────────────────────────────────────────────

    def child(self, *parts: str) -> "Storage":
        """A :class:`Storage` rooted at a subdirectory of this one."""
        raise NotImplementedError

    def uri(self, *parts: str) -> str:
        """The DuckDB-/fsspec-addressable string for a file (or this dir) — what a ``COPY … TO`` /
        ``read_parquet`` statement targets. Single-quotes are doubled for safe SQL interpolation."""
        raise NotImplementedError

    def glob(self, pattern: str = "*.parquet", *parts: str) -> str:
        """A DuckDB glob string (e.g. ``…/table/*.parquet``) for ``read_parquet``."""
        raise NotImplementedError

    # ─── inspection ──────────────────────────────────────────────────────────────

    def exists(self, *parts: str) -> bool:
        raise NotImplementedError

    def is_dir(self, *parts: str) -> bool:
        raise NotImplementedError

    def size(self, *parts: str) -> int:
        """The byte size of a file, or ``0`` if it does not exist."""
        raise NotImplementedError

    def parquet_names(self, *parts: str) -> list[str]:
        """The ``*.parquet`` file names directly under the addressed dir, sorted."""
        raise NotImplementedError

    def names(self, *parts: str) -> list[str]:
        """*All* file names (not just ``*.parquet``) directly under the addressed dir, sorted — used by
        the Iceberg orphan-file GC, which sweeps ``.metadata.json`` / ``.avro`` / data files alike."""
        raise NotImplementedError

    def subdir_names(self) -> list[str]:
        """The immediate subdirectory names of this dir, sorted."""
        raise NotImplementedError

    def warehouse_location(self, *parts: str) -> str:
        """The **raw** (un-escaped) location string for pyiceberg's ``warehouse`` / FileIO — a ``file://``
        URI locally, the object URI (``s3://…``) on an object store. Distinct from :meth:`uri` (which
        SQL-escapes for DuckDB interpolation)."""
        raise NotImplementedError

    def iceberg_properties(self) -> dict[str, str]:
        """pyiceberg catalog/FileIO properties so its *own* writer can authenticate to this storage
        (it writes table data/metadata itself, not via DuckDB). Empty for local; the resolved
        ``s3.*`` / ``gcs.*`` / ``adls.*`` credentials for an object store."""
        return {}

    # ─── mutation ────────────────────────────────────────────────────────────────

    def mkdir(self, *parts: str) -> None:
        """Ensure the addressed directory exists (a no-op on object stores, which have no real dirs)."""

    def remove(self, *parts: str) -> None:
        """Delete a single file. Missing is not an error."""
        raise NotImplementedError

    def rmtree(self, *parts: str) -> None:
        """Delete a directory and everything under it. Missing is not an error."""
        raise NotImplementedError

    def read_text(self, *parts: str) -> str | None:
        """The file's text, or ``None`` if it does not exist."""
        raise NotImplementedError

    def read_bytes(self, *parts: str) -> bytes:
        raise NotImplementedError

    def write_text(self, text: str, *parts: str) -> None:
        """Atomically write ``text`` to the addressed file."""
        self.write_bytes(text.encode("utf-8"), *parts)

    def write_bytes(self, data: bytes, *parts: str) -> None:
        """Atomically write ``data`` to the addressed file."""
        raise NotImplementedError

    @contextmanager
    def copy_to(self, *parts: str) -> Iterator[str]:
        """Yield a URI for DuckDB to ``COPY … TO``, committing the result **atomically** to the addressed
        file on clean exit. Local: write a ``.tmp`` then ``os.replace``. Object store: write the object
        directly (a single-object PUT is atomic at the object level)."""
        raise NotImplementedError

    @contextmanager
    def copy_dir_to(self, *parts: str) -> Iterator[str]:
        """Yield a URI for a DuckDB ``COPY … TO '<dir>' (… FILE_SIZE_BYTES …)`` that writes a *directory*
        of size-bounded Parquet files. The staging directory is cleared first and on exit the caller is
        responsible for moving/keeping its produced files (see :meth:`move_into`)."""
        raise NotImplementedError

    def move_into(self, dest: "Storage", src_name: str, dest_name: str) -> None:
        """Move a file from this storage to ``dest`` under a new name (same backend; used to commit a
        staging chunk to its token-named home). Local: ``os.replace``. Object: an fsspec ``mv``."""
        raise NotImplementedError

    def put_tree(self, local: Path, *parts: str) -> None:
        """Publish a local file-or-directory ``local`` to the addressed location, **replacing** whatever
        is there (used to commit an :mod:`~duckstring.objects` Object's payload into ``objects/{name}/``).
        The addressed location mirrors ``local``'s contents. Local: an atomic dir swap; object store: drop
        the old prefix then ``put`` recursively."""
        raise NotImplementedError

    def fetch(self, dest: Path, *parts: str) -> Path:
        """Materialise the addressed file-or-subtree to a **local** path and return it. A local backend
        returns the real path in place (no copy — the result is read-only by contract); an object store
        downloads to ``dest`` (a file path for a single object, a directory for a subtree) and returns it."""
        raise NotImplementedError

    def duckdb_setup(self, con) -> None:
        """Configure ``con`` so DuckDB can read/write this storage (load ``httpfs`` + a credential
        secret for an object store). A no-op for local paths."""


class LocalStorage(Storage):
    """A local POSIX directory — today's behaviour, byte-for-byte. Atomic writes are ``tmp + os.replace``."""

    is_local = True

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _abs(self, *parts: str) -> Path:
        return self.root.joinpath(*parts) if parts else self.root

    def child(self, *parts: str) -> "LocalStorage":
        return LocalStorage(self._abs(*parts))

    def uri(self, *parts: str) -> str:
        return str(self._abs(*parts)).replace("'", "''")

    def glob(self, pattern: str = "*.parquet", *parts: str) -> str:
        return str(self._abs(*parts, pattern)).replace("'", "''")

    def exists(self, *parts: str) -> bool:
        return self._abs(*parts).exists()

    def is_dir(self, *parts: str) -> bool:
        return self._abs(*parts).is_dir()

    def size(self, *parts: str) -> int:
        p = self._abs(*parts)
        return p.stat().st_size if p.exists() else 0

    def parquet_names(self, *parts: str) -> list[str]:
        d = self._abs(*parts)
        return sorted(p.name for p in d.glob("*.parquet")) if d.is_dir() else []

    def names(self, *parts: str) -> list[str]:
        d = self._abs(*parts)
        return sorted(p.name for p in d.iterdir() if p.is_file()) if d.is_dir() else []

    def subdir_names(self) -> list[str]:
        return sorted(p.name for p in self.root.iterdir() if p.is_dir()) if self.root.is_dir() else []

    def warehouse_location(self, *parts: str) -> str:
        return self._abs(*parts).as_uri()

    def mkdir(self, *parts: str) -> None:
        self._abs(*parts).mkdir(parents=True, exist_ok=True)

    def remove(self, *parts: str) -> None:
        self._abs(*parts).unlink(missing_ok=True)

    def rmtree(self, *parts: str) -> None:
        import shutil

        shutil.rmtree(self._abs(*parts), ignore_errors=True)

    def read_text(self, *parts: str) -> str | None:
        p = self._abs(*parts)
        return p.read_text(encoding="utf-8") if p.exists() else None

    def read_bytes(self, *parts: str) -> bytes:
        return self._abs(*parts).read_bytes()

    def write_bytes(self, data: bytes, *parts: str) -> None:
        dest = self._abs(*parts)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(dest)

    @contextmanager
    def copy_to(self, *parts: str) -> Iterator[str]:
        dest = self._abs(*parts)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        yield str(tmp).replace("'", "''")
        tmp.replace(dest)

    @contextmanager
    def copy_dir_to(self, *parts: str) -> Iterator[str]:
        import shutil

        staging = self._abs(*parts)
        if staging.exists():
            shutil.rmtree(staging)
        staging.parent.mkdir(parents=True, exist_ok=True)
        yield str(staging).replace("'", "''")

    def move_into(self, dest: "Storage", src_name: str, dest_name: str) -> None:
        assert isinstance(dest, LocalStorage)
        target = dest._abs(dest_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._abs(src_name).replace(target)

    def put_tree(self, local: Path, *parts: str) -> None:
        import shutil

        dest = self._abs(*parts)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.copytree(local, tmp)  # `local` is always a directory (an Object's staged payload)
        shutil.rmtree(dest, ignore_errors=True)
        tmp.replace(dest)

    def fetch(self, dest: Path, *parts: str) -> Path:
        return self._abs(*parts)  # already local — hand back the real path (read-only by contract)


class ObjectStorage(Storage):
    """An object-store directory addressed by a URI, ``fsspec``-backed. Atomic-overwrite = a single-object
    PUT (no tmp+rename). Credentials/options ride the URI query and were split off into ``params``;
    ``${env:NAME}`` references are resolved at construction (runtime), never persisted.

    Bulk Parquet I/O still runs through DuckDB over ``httpfs`` (see :meth:`duckdb_setup`); ``fsspec`` here
    only does the listing / existence / delete / small-file work."""

    is_local = False

    def __init__(self, base: str, params: dict[str, str] | None = None) -> None:
        self.base = base.rstrip("/")
        self.params = dict(params or {})
        self._fs = None  # lazily created fsspec filesystem

    # ─── fsspec ──────────────────────────────────────────────────────────────────

    @property
    def fs(self):
        if self._fs is None:
            import fsspec

            from .egress import credentials

            opts = {}
            for k, v in self.params.items():
                if k in ("region",) or k.startswith("client_kwargs") or k in (
                    "key", "key_id", "secret", "token", "account_name", "account_key", "anon",
                ):
                    opts[_FSSPEC_OPT.get(k, k)] = credentials.resolve(v)
            scheme = urlsplit(self.base).scheme
            self._fs = fsspec.filesystem(_FSSPEC_PROTOCOL.get(scheme, scheme), **opts)
        return self._fs

    def _key(self, *parts: str) -> str:
        """The fsspec path (scheme stripped of nothing — fsspec accepts the full URL) for the addressed
        file/dir."""
        if not parts:
            return self.base
        return self.base + "/" + str(PurePosixPath(*parts))

    # ─── addressing ──────────────────────────────────────────────────────────────

    def child(self, *parts: str) -> "ObjectStorage":
        return ObjectStorage(self._key(*parts), self.params)

    def uri(self, *parts: str) -> str:
        return self._key(*parts).replace("'", "''")

    def glob(self, pattern: str = "*.parquet", *parts: str) -> str:
        return (self._key(*parts) + "/" + pattern).replace("'", "''")

    # ─── inspection ──────────────────────────────────────────────────────────────

    def exists(self, *parts: str) -> bool:
        return self.fs.exists(self._key(*parts))

    def is_dir(self, *parts: str) -> bool:
        return self.fs.isdir(self._key(*parts))

    def size(self, *parts: str) -> int:
        key = self._key(*parts)
        try:
            return int(self.fs.info(key)["size"])
        except FileNotFoundError:
            return 0

    def parquet_names(self, *parts: str) -> list[str]:
        key = self._key(*parts)
        if not self.fs.isdir(key):
            return []
        return sorted(
            k.rstrip("/").rsplit("/", 1)[-1]
            for k in self.fs.ls(key, detail=False)
            if k.endswith(".parquet")
        )

    def names(self, *parts: str) -> list[str]:
        key = self._key(*parts)
        if not self.fs.isdir(key):
            return []
        return sorted(
            e["name"].rstrip("/").rsplit("/", 1)[-1]
            for e in self.fs.ls(key, detail=True)
            if e.get("type") == "file"
        )

    def subdir_names(self) -> list[str]:
        if not self.fs.isdir(self.base):
            return []
        return sorted(
            e["name"].rstrip("/").rsplit("/", 1)[-1]
            for e in self.fs.ls(self.base, detail=True)
            if e.get("type") == "directory"
        )

    def warehouse_location(self, *parts: str) -> str:
        return self._key(*parts)

    def iceberg_properties(self) -> dict[str, str]:
        from .egress import credentials

        scheme = urlsplit(self.base).scheme.lower()

        def res(key: str) -> str | None:
            val = self.params.get(key)
            return credentials.resolve(val) if val else None

        out: dict[str, str] = {}
        if scheme in ("s3", "s3a"):
            mapping = (("key_id", "s3.access-key-id"), ("key", "s3.access-key-id"),
                       ("secret", "s3.secret-access-key"), ("region", "s3.region"),
                       ("token", "s3.session-token"), ("endpoint", "s3.endpoint"))
        elif scheme in ("gs", "gcs"):
            mapping = (("token", "gcs.oauth2.token"), ("project", "gcs.project-id"))
        elif scheme in ("abfs", "abfss", "az", "wasb", "wasbs"):
            mapping = (("account_name", "adls.account-name"), ("account_key", "adls.account-key"),
                       ("sas_token", "adls.sas-token"))
        else:
            mapping = ()
        for src, dst in mapping:
            val = res(src)
            if val is not None:
                out.setdefault(dst, val)  # key_id wins over key when both are given
        return out

    # ─── mutation ────────────────────────────────────────────────────────────────

    def remove(self, *parts: str) -> None:
        key = self._key(*parts)
        try:
            self.fs.rm_file(key)
        except (FileNotFoundError, AttributeError):
            try:
                self.fs.rm(key)
            except FileNotFoundError:
                pass

    def rmtree(self, *parts: str) -> None:
        key = self._key(*parts)
        try:
            self.fs.rm(key, recursive=True)
        except FileNotFoundError:
            pass

    def read_text(self, *parts: str) -> str | None:
        key = self._key(*parts)
        if not self.fs.exists(key):
            return None
        return self.fs.cat_file(key).decode("utf-8")

    def read_bytes(self, *parts: str) -> bytes:
        return self.fs.cat_file(self._key(*parts))

    def write_bytes(self, data: bytes, *parts: str) -> None:
        self.fs.pipe_file(self._key(*parts), data)  # single-object PUT — atomic

    @contextmanager
    def copy_to(self, *parts: str) -> Iterator[str]:
        # A single-object PUT is atomic at the object level → write the final object directly, no tmp.
        yield self._key(*parts).replace("'", "''")

    @contextmanager
    def copy_dir_to(self, *parts: str) -> Iterator[str]:
        key = self._key(*parts)
        try:
            self.fs.rm(key, recursive=True)
        except FileNotFoundError:
            pass
        yield key.replace("'", "''")

    def move_into(self, dest: "Storage", src_name: str, dest_name: str) -> None:
        assert isinstance(dest, ObjectStorage)
        self.fs.mv(self._key(src_name), dest._key(dest_name))

    def put_tree(self, local: Path, *parts: str) -> None:
        key = self._key(*parts)
        try:
            self.fs.rm(key, recursive=True)
        except FileNotFoundError:
            pass
        # `local` is a directory of the Object's payload → its contents land under `key`.
        self.fs.put(str(local), key, recursive=True)

    def fetch(self, dest: Path, *parts: str) -> Path:
        key = self._key(*parts)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if self.fs.isdir(key):
            self.fs.get(key, str(dest), recursive=True)
        else:
            self.fs.get_file(key, str(dest))
        return dest

    # ─── DuckDB credentials ──────────────────────────────────────────────────────

    def duckdb_setup(self, con) -> None:
        """Load ``httpfs`` and register a DuckDB secret so ``COPY``/``read_parquet`` reach the bucket.
        Mirrors the s3:// egress driver. With no explicit key the ambient credential chain (instance
        profile / managed identity) is used; the secret-``CREATE`` error is masked so it can never echo a
        credential value."""
        from .egress import credentials

        scheme = urlsplit(self.base).scheme.lower()
        try:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
        except Exception:
            pass
        duck_type = {"s3": "S3", "s3a": "S3", "gs": "GCS", "gcs": "GCS"}.get(scheme)
        if duck_type is None:
            return  # abfss / databricks etc. configured via env / fsspec; DuckDB httpfs covers s3/gcs
        bits = [f"TYPE {duck_type}"]
        key = self.params.get("key_id") or self.params.get("key")
        secret = self.params.get("secret")
        region = self.params.get("region")
        try:
            if key and secret:
                bits.append(f"KEY_ID '{credentials.resolve(key)}'")
                bits.append(f"SECRET '{credentials.resolve(secret)}'")
            else:
                bits.append("PROVIDER credential_chain")
            if region:
                bits.append(f"REGION '{credentials.resolve(region)}'")
            con.execute(f"CREATE OR REPLACE SECRET duckstring_data ({', '.join(bits)})")
        except Exception:  # never echo a credential value in the error
            raise RuntimeError(f"failed to configure DuckDB {duck_type} credentials for the data plane") from None


# fsspec protocol / option-name remaps for the schemes we accept.
_FSSPEC_PROTOCOL = {"gs": "gcs", "s3a": "s3", "az": "abfs", "wasb": "abfs", "wasbs": "abfs", "dbfs": "databricks"}
_FSSPEC_OPT = {"key_id": "key", "account_key": "account_key"}
