"""Persist (plans/persist.md phase 2): the local→durable mirror (`persist_tree`) and the local-first
read resolution (`resolve_data_dir` / `Pond._source_data_dir`). The end-to-end proof (real Ducks,
async persist, persisted_f) is `test_runtime.test_local_first_publish_persists_to_data_root`."""

from __future__ import annotations

import json

from duckstring.dataplane import persist_tree
from duckstring.storage import LocalStorage


def _seed_local(root):
    """A representative local publish: sidecar + wholesale table + a parts dir + state snapshots + the
    (local-only) Iceberg catalog entries."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "_trickle.json").write_text(json.dumps({"t": {"mode": "append"}}))
    (root / "whole.parquet").write_bytes(b"v1")
    (root / "t").mkdir()
    (root / "t" / "2026-01-01T00_00_00+00_00.parquet").write_bytes(b"p1")
    (root / "t" / "2026-01-02T00_00_00+00_00.parquet").write_bytes(b"p2")
    (root / "state" / "agg" / "t").mkdir(parents=True)
    (root / "state" / "agg" / "t" / "2026-01-02T00_00_00+00_00.parquet").write_bytes(b"s1")
    (root / "catalog.json").write_text("{}")  # iceberg pointer — local-only
    (root / "pond").mkdir()  # iceberg warehouse — local-only
    (root / "pond" / "meta.json").write_text("{}")


def test_persist_tree_mirrors_flat_layout_and_skips_iceberg(tmp_path):
    local, dest = tmp_path / "local", LocalStorage(tmp_path / "durable")
    _seed_local(local)
    n = persist_tree(LocalStorage(local), dest)
    assert n > 0
    d = tmp_path / "durable"
    assert (d / "_trickle.json").exists() and (d / "whole.parquet").read_bytes() == b"v1"
    assert sorted(p.name for p in (d / "t").glob("*.parquet")) == [
        "2026-01-01T00_00_00+00_00.parquet", "2026-01-02T00_00_00+00_00.parquet"]
    assert (d / "state" / "agg" / "t" / "2026-01-02T00_00_00+00_00.parquet").exists()
    # The Iceberg catalog layer is local-only (its metadata embeds absolute local paths).
    assert not (d / "catalog.json").exists() and not (d / "pond").exists()


def test_persist_tree_incremental_and_pruning(tmp_path):
    local, dest = tmp_path / "local", LocalStorage(tmp_path / "durable")
    _seed_local(local)
    persist_tree(LocalStorage(local), dest)
    d = tmp_path / "durable"

    # Parts are idempotent by name: a re-mirror does not rewrite them (mtime probe via content marker).
    (d / "t" / "2026-01-01T00_00_00+00_00.parquet").write_bytes(b"REMOTE-KEPT")
    # Wholesale files are always re-uploaded (rewritten per run).
    (local / "whole.parquet").write_bytes(b"v2")
    # A new part appears; an old one is retention-trimmed locally; the state snapshot rolls forward.
    (local / "t" / "2026-01-03T00_00_00+00_00.parquet").write_bytes(b"p3")
    (local / "t" / "2026-01-02T00_00_00+00_00.parquet").unlink()
    (local / "state" / "agg" / "t" / "2026-01-02T00_00_00+00_00.parquet").unlink()
    (local / "state" / "agg" / "t" / "2026-01-03T00_00_00+00_00.parquet").write_bytes(b"s2")
    persist_tree(LocalStorage(local), dest)

    assert (d / "t" / "2026-01-01T00_00_00+00_00.parquet").read_bytes() == b"REMOTE-KEPT"  # not re-sent
    assert (d / "whole.parquet").read_bytes() == b"v2"  # wholesale re-uploaded
    assert sorted(p.name for p in (d / "t").glob("*.parquet")) == [
        "2026-01-01T00_00_00+00_00.parquet", "2026-01-03T00_00_00+00_00.parquet"]  # add + prune
    assert sorted(p.name for p in (d / "state" / "agg" / "t").glob("*.parquet")) == [
        "2026-01-03T00_00_00+00_00.parquet"]  # snapshot pruned + rolled

    # A directory dropped locally (warm fold / table delete) is pruned at the destination.
    import shutil
    shutil.rmtree(local / "t")
    persist_tree(LocalStorage(local), dest)
    assert not (d / "t").exists()


def test_persist_tree_refuses_an_empty_local(tmp_path):
    """A fresh/lost box (no local sidecar) must never wipe the durable layer."""
    local, dest = tmp_path / "local", LocalStorage(tmp_path / "durable")
    _seed_local(local)
    persist_tree(LocalStorage(local), dest)
    empty = tmp_path / "empty"
    empty.mkdir()
    assert persist_tree(LocalStorage(empty), dest) == 0
    assert (tmp_path / "durable" / "_trickle.json").exists(), "an empty local wiped the durable layer"


def test_resolve_data_dir_prefers_the_local_publish(tmp_path):
    from duckstring.catchment.registry import resolve_data_dir

    remote = tmp_path / "durable"
    # No local publish → the data root.
    dd = resolve_data_dir(tmp_path, "p", 1, str(remote))
    assert str(remote) in dd.uri()
    # A local publish (sidecar present) → the local layout, data root notwithstanding.
    local = tmp_path / "ponds" / "p" / "m1" / "data"
    local.mkdir(parents=True)
    (local / "_trickle.json").write_text("{}")
    dd = resolve_data_dir(tmp_path, "p", 1, str(remote))
    assert str(local) in dd.uri()
    # No data root at all → always local.
    assert str(local) in resolve_data_dir(tmp_path, "p", 1, None).uri()


def test_pond_source_read_resolves_local_first(tmp_path):
    """`Pond._source_data_dir` (the Duck-side foreign read): a co-located Source published locally is
    read from the local layout even with a data root configured."""
    from duckstring.core import Pond

    local = tmp_path / "ponds" / "src" / "m1" / "data"
    local.mkdir(parents=True)
    (local / "_trickle.json").write_text("{}")
    pond = Pond("me", "1.0.0", con=None, root=str(tmp_path),
                source_majors={"src": 1, "faraway": 2}, data_root=str(tmp_path / "durable"))
    assert str(local) in pond._source_data_dir("src").uri()
    assert str(tmp_path / "durable") in pond._source_data_dir("faraway").uri()
