"""A Postgres wire-protocol front door onto the serving core (plans/data-serving.md) — the default,
universal-reach adapter (psql / Metabase / Tableau / any pg driver). A thin translator: it speaks
enough of the v3 protocol to run the client's SQL on the sandboxed serving connection and stream the
result back; **DuckDB itself answers the `pg_catalog`/`information_schema` introspection** the tools
issue (over the serving connection's schemas), so we don't hand-fake the catalog.

Auth: the pg password is a Duckstring API key → its access level (open mode → full). Read/demand →
the sandboxed connection (serviceable-only, no external access); full → the ops connection.

Results are sent in **text format** with a best-effort type OID per column — the widely-compatible
approach for a read surface. Both the simple ('Q') and extended (Parse/Bind/Describe/Execute) query
flows are handled; extended parameters are bound text-format via DuckDB's ``$n`` placeholders. Robust
BI-tool coverage beyond this (binary params, every catalog quirk) is a follow-up.
"""

from __future__ import annotations

import logging
import socket
import struct
import threading

from . import auth

log = logging.getLogger("duckstring.pg")

# A small set of pg type OIDs; unknown DuckDB types fall back to text (25). Values still go as text.
_TEXT, _BOOL, _INT8, _INT4, _INT2, _FLOAT8, _FLOAT4, _NUMERIC, _DATE, _TS, _TSTZ, _VARCHAR = (
    25, 16, 20, 23, 21, 701, 700, 1700, 1082, 1114, 1184, 1043)


def _oid(t: str) -> int:
    t = (t or "").upper()
    if t in ("BOOLEAN", "BOOL"):
        return _BOOL
    if t in ("BIGINT", "HUGEINT", "INT8", "LONG", "UBIGINT"):
        return _INT8
    if t in ("INTEGER", "INT", "INT4", "UINTEGER"):
        return _INT4
    if t in ("SMALLINT", "TINYINT", "INT2"):
        return _INT2
    if t in ("DOUBLE", "FLOAT8"):
        return _FLOAT8
    if t in ("FLOAT", "REAL", "FLOAT4"):
        return _FLOAT4
    if t.startswith("DECIMAL") or t.startswith("NUMERIC"):
        return _NUMERIC
    if t == "DATE":
        return _DATE
    if t.startswith("TIMESTAMP WITH"):
        return _TSTZ
    if t.startswith("TIMESTAMP"):
        return _TS
    return _VARCHAR if t.startswith("VARCHAR") else _TEXT


def _fmt(v) -> bytes | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return b"t" if v else b"f"
    return str(v).encode("utf-8")


# ─── wire message writers ────────────────────────────────────────────────────────

def _msg(tag: bytes, body: bytes) -> bytes:
    return tag + struct.pack("!I", len(body) + 4) + body


def _auth_ok() -> bytes:
    return _msg(b"R", struct.pack("!I", 0))


def _auth_cleartext() -> bytes:
    return _msg(b"R", struct.pack("!I", 3))


def _param_status(k: str, v: str) -> bytes:
    return _msg(b"S", k.encode() + b"\x00" + v.encode() + b"\x00")


def _ready(status: bytes = b"I") -> bytes:
    return _msg(b"Z", status)


def _row_description(desc) -> bytes:
    body = struct.pack("!H", len(desc))
    for col in desc:
        name, dtype = col[0], (col[1] if len(col) > 1 else "")
        body += name.encode("utf-8") + b"\x00"
        body += struct.pack("!IhIhih", 0, 0, _oid(str(dtype)), -1, -1, 0)  # tableoid, attr, typeoid, len, mod, fmt(text)
    return _msg(b"T", body)


def _data_row(row) -> bytes:
    body = struct.pack("!H", len(row))
    for v in row:
        b = _fmt(v)
        body += struct.pack("!i", -1) if b is None else (struct.pack("!I", len(b)) + b)
    return _msg(b"D", body)


def _command_complete(tag: str) -> bytes:
    return _msg(b"C", tag.encode() + b"\x00")


def _error(message: str, code: str = "42000") -> bytes:
    body = b"S" + b"ERROR\x00" + b"C" + code.encode() + b"\x00" + b"M" + message.encode("utf-8") + b"\x00" + b"\x00"
    return _msg(b"E", body)


def _tag_for(sql: str, n: int) -> str:
    verb = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    return f"SELECT {n}" if verb in ("SELECT", "WITH", "VALUES", "TABLE", "PRAGMA", "SHOW", "") else f"{verb} {n}"


class PgWireServer:
    def __init__(self, driver, *, api_key: str | None = None, host: str = "127.0.0.1", port: int = 5433):
        self.driver = driver
        self.api_key = api_key
        self.host, self.port = host, port
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> int:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]
        self._running = True
        self._thread = threading.Thread(target=self._accept, name="ds-pg-wire", daemon=True)
        self._thread.start()
        log.info("pg-wire serving on %s:%s", self.host, self.port)
        return self.port

    def stop(self) -> None:
        self._running = False
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass

    def _accept(self) -> None:
        while self._running:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                break
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    # ─── per-connection ───────────────────────────────────────────────────────────

    def _recv_exact(self, conn, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("client closed")
            buf += chunk
        return buf

    def _startup(self, conn) -> auth.Level | None:
        # First a length-prefixed message with no tag: could be SSLRequest (len 8, code 80877103) → 'N'.
        while True:
            length = struct.unpack("!I", self._recv_exact(conn, 4))[0]
            body = self._recv_exact(conn, length - 4)
            code = struct.unpack("!I", body[:4])[0]
            if code == 80877103:      # SSLRequest → we don't do TLS here (put a proxy in front)
                conn.sendall(b"N")
                continue
            if code == 80877102:      # CancelRequest → ignore
                return None
            break
        params, rest = {}, body[4:]
        items = rest.split(b"\x00")
        for i in range(0, len(items) - 1, 2):
            if items[i]:
                params[items[i].decode(errors="replace")] = items[i + 1].decode(errors="replace")

        con = getattr(self.driver, "db", None)
        if not auth.auth_configured(con, self.api_key):
            conn.sendall(_auth_ok())          # open mode → no password
            return auth.Level.FULL
        conn.sendall(_auth_cleartext())        # ask for the API key as the password
        tag = self._recv_exact(conn, 1)
        if tag != b"p":
            return None
        plen = struct.unpack("!I", self._recv_exact(conn, 4))[0]
        pw = self._recv_exact(conn, plen - 4).rstrip(b"\x00").decode(errors="replace")
        level = auth.level_for_key(con, self.api_key, pw)
        if level is not None:
            conn.sendall(_auth_ok())          # password accepted
        return level

    def _run(self, level: auth.Level, sql: str, params=None):
        from .serving import _cache
        sandboxed = level < auth.Level.FULL
        cur = _cache(self.driver).connection(self.driver, sandboxed=sandboxed).cursor()
        cur.execute(sql, params or [])
        return cur

    def _send_result(self, conn, cur, sql: str) -> None:
        if cur.description:
            conn.sendall(_row_description(cur.description))
            rows = cur.fetchall()
            for r in rows:
                conn.sendall(_data_row(r))
            conn.sendall(_command_complete(_tag_for(sql, len(rows))))
        else:
            conn.sendall(_command_complete(_tag_for(sql, 0)))

    def _handle(self, conn) -> None:
        try:
            level = self._startup(conn)
            if level is None:
                conn.sendall(_error("authentication failed", "28000"))
                conn.close()
                return
            conn.sendall(_param_status("server_version", "14.0 (duckstring)"))
            conn.sendall(_param_status("client_encoding", "UTF8"))
            conn.sendall(_param_status("DateStyle", "ISO, MDY"))
            conn.sendall(_msg(b"K", struct.pack("!II", 0, 0)))  # BackendKeyData (unused)
            conn.sendall(_ready())

            statements: dict[str, tuple[str, int]] = {}   # name → (sql, nparams)
            portals: dict[str, tuple[str, list]] = {}     # portal → (sql, params)
            err = False  # in the extended flow, skip messages until the next Sync after an error
            while self._running:
                tag = self._recv_exact(conn, 1)
                length = struct.unpack("!I", self._recv_exact(conn, 4))[0]
                body = self._recv_exact(conn, length - 4)
                if tag == b"S":                                   # Sync → end of an extended batch
                    err = False
                    conn.sendall(_ready())
                    continue
                if err:
                    continue
                if tag == b"Q":                                   # simple query
                    sql = body.split(b"\x00", 1)[0].decode("utf-8", "replace")
                    for stmt in [s for s in _split(sql) if s.strip()] or [""]:
                        try:
                            self._send_result(conn, self._run(level, stmt), stmt)
                        except Exception as exc:
                            conn.sendall(_error(str(exc)))
                            break
                    conn.sendall(_ready())
                elif tag == b"P":                                 # Parse: name\0 query\0 int16 nparams…
                    name, q, tail = body.split(b"\x00", 2)
                    nparams = struct.unpack("!H", tail[:2])[0] if len(tail) >= 2 else 0
                    statements[name.decode()] = (q.decode("utf-8", "replace"), nparams)
                    conn.sendall(_msg(b"1", b""))
                elif tag == b"B":                                 # Bind
                    portal, stmt, params = _parse_bind(body)
                    portals[portal] = (statements.get(stmt, ("", 0))[0], params)
                    conn.sendall(_msg(b"2", b""))
                elif tag == b"D":                                 # Describe (kind byte + name)
                    kind, name = body[:1], body[1:].split(b"\x00", 1)[0].decode()
                    if kind == b"S":
                        sql, nparams = statements.get(name, ("", 0))
                        params = [None] * nparams
                    else:
                        sql, params = portals.get(name, ("", []))
                    try:                                          # execute for the shape (rows fetched at E)
                        cur = self._run(level, sql, params)
                        if kind == b"S":
                            conn.sendall(_msg(b"t", struct.pack("!H", len(params))))  # ParameterDescription
                        conn.sendall(_row_description(cur.description) if cur.description else _msg(b"n", b""))
                    except Exception as exc:
                        conn.sendall(_error(str(exc)))
                        err = True
                elif tag == b"E":                                 # Execute
                    portal = body.split(b"\x00", 1)[0].decode()
                    sql, params = portals.get(portal, ("", []))
                    try:
                        self._send_result(conn, self._run(level, sql, params), sql)
                    except Exception as exc:
                        conn.sendall(_error(str(exc)))
                        err = True
                elif tag == b"X":                                 # Terminate
                    break
                # H (Flush), C (Close) → ignored
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass


def _split(sql: str) -> list[str]:
    return [s for s in sql.split(";")]


def _parse_bind(body: bytes) -> tuple[str, str, list]:
    portal, rest = body.split(b"\x00", 1)
    stmt, rest = rest.split(b"\x00", 1)
    (nfmt,) = struct.unpack("!H", rest[:2])
    rest = rest[2 + 2 * nfmt:]                    # skip the format codes (we read params as text)
    (nparams,) = struct.unpack("!H", rest[:2])
    rest = rest[2:]
    params: list = []
    for _ in range(nparams):
        (ln,) = struct.unpack("!i", rest[:4])
        rest = rest[4:]
        if ln == -1:
            params.append(None)
        else:
            params.append(rest[:ln].decode("utf-8", "replace"))
            rest = rest[ln:]
    return portal.decode(), stmt.decode(), params
