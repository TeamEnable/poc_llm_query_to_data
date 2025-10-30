from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Iterable, Dict, List, runtime_checkable, Optional, Mapping, Any
import csv
import sqlite3
import os
from pathlib import Path

Row = Dict[str, Any]  # e.g., {"country": "...", "capital": "..."}


@runtime_checkable
class Sink(Protocol):
    """A minimal streaming writer interface used by the app."""

    def open(self) -> None: ...
    def write(self, rows: Iterable[Row]) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> bool: ...


class SinkCM:
    """Context-manager mixin for sinks.

    Concrete sinks should subclass this (e.g., `class CsvSink(SinkCM): ...`).
    """

    def __enter__(self):  # type: ignore[override]
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):  # type: ignore[override]
        self.close()
        return False


# ----------------------------
# CSV sink
# ----------------------------
class CsvSink(SinkCM):
    """Write rows to a CSV file.

    Args:
        path: Destination CSV path.
        headers: Column order to write. Extra keys in rows are ignored; missing keys become empty.
        newline: Line terminator to use when writing.
        encoding: File encoding (defaults to utf-8).
        append: If True, append to file and only write header if file did not exist.
    """

    def __init__(
        self,
        path: str,
        headers: List[str],
        newline: str = "\n",
        encoding: str = "utf-8",
        append: bool = False,
    ):
        self.path = path
        self.headers = headers
        self.newline = newline
        self.encoding = encoding
        self.append = append
        self._fh: Optional[Any] = None
        self._writer: Optional[csv.DictWriter] = None

    def open(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        mode = "a" if self.append else "w"
        file_existed = Path(self.path).exists()
        self._fh = open(self.path, mode, newline="", encoding=self.encoding)
        self._writer = csv.DictWriter(
            self._fh, fieldnames=self.headers, lineterminator=self.newline
        )
        # Write header on create or when not appending
        if not self.append or not file_existed:
            self._writer.writeheader()

    def write(self, rows: Iterable[Row]) -> None:
        assert self._writer is not None, "CsvSink not opened"
        for r in rows:
            # normalize to declared headers only
            safe = {h: r.get(h, "") for h in self.headers}
            self._writer.writerow(safe)

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
            self._writer = None


# ----------------------------
# SQLite sink
# ----------------------------

def _qident(name: str) -> str:
    """Quote an identifier for SQLite with double quotes.
    Very small helper; does not allow embedded quotes to avoid SQLi on identifiers.
    """
    if '"' in name:
        raise ValueError("Identifier may not contain double quotes")
    return f'"{name}"'


class SqliteSink(SinkCM):
    """Write rows into a SQLite table, creating it if needed.

    Args:
        db_path: Path to the .sqlite/.db file (will be created if missing).
        table: Target table name.
        columns: Ordered list of columns to write. If None, inferred from `schema` ordering.
        schema: Optional explicit mapping of column -> SQLite type (e.g., {"country": "TEXT"}).
                If omitted, all declared `columns` will be created as TEXT.
        if_not_exists: Use CREATE TABLE IF NOT EXISTS.
        replace_table: If True, DROP TABLE IF EXISTS before creating it (overrides if_not_exists behavior).
        journal_wal: If True, set PRAGMA journal_mode=WAL for better concurrent reads.
        batch_size: Number of rows to buffer before executing `executemany`.
        ...

    Notes:
        - Unknown keys in incoming rows are ignored; missing keys are inserted as NULL.
        - All values are bound parameters; no string formatting for data happens here.
    """

    def __init__(
        self,
        db_path: str,
        table: str,
        *,
        columns: Optional[List[str]] = None,
        schema: Optional[Mapping[str, str]] = None,
        if_not_exists: bool = True,
        replace_table: bool = False,
        journal_wal: bool = True,
        batch_size: int = 500,
        upsert_keys: Optional[List[str]] = None,
        upsert_update: Union[List[str], Literal["all", "none"]] = "all",
    ):
        if schema is None and columns is None:
            raise ValueError("Provide at least one of `columns` or `schema`.")
        if columns is None and schema is not None:
            columns = list(schema.keys())
        assert columns is not None
        self.db_path = db_path
        self.table = table
        self.columns = columns
        self.schema = dict(schema) if schema is not None else {c: "TEXT" for c in columns}
        self.if_not_exists = if_not_exists
        self.replace_table = replace_table
        self.journal_wal = journal_wal
        self.batch_size = batch_size
        self._conn: Optional[sqlite3.Connection] = None
        self._pending: List[List[Any]] = []
        # upsert config
        self.upsert_keys = list(upsert_keys) if upsert_keys else None
        self.upsert_update = upsert_update
        if self.upsert_keys:
            missing = [k for k in self.upsert_keys if k not in self.columns]
            if missing:
                raise ValueError(f"upsert_keys not in columns: {missing}")

    # context manager sugar provided by Sink protocol defaults

    def open(self) -> None:
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA foreign_keys = ON")
        if self.journal_wal:
            try:
                self._conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass

        cur = self._conn.cursor()
        if self.replace_table:
            cur.execute(f"DROP TABLE IF EXISTS {_qident(self.table)}")

        cols_sql = ", ".join([f"{_qident(k)} {v}" for k, v in self.schema.items()])
        ine = "IF NOT EXISTS " if self.if_not_exists and not self.replace_table else ""
        cur.execute(f"CREATE TABLE {ine}{_qident(self.table)} ({cols_sql})")
        self._conn.commit()

        # Ensure UNIQUE index for upserts
        if self.upsert_keys:
            idx_name = f"{self.table}__uniq__{'__'.join(self.upsert_keys)}"
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {_qident(idx_name)} ON {_qident(self.table)} (" + ", ".join(_qident(k) for k in self.upsert_keys) + ")"
            )
            self._conn.commit()

        placeholders = ", ".join(["?" for _ in self.columns])
        cols_list = ", ".join([_qident(c) for c in self.columns])
        if not self.upsert_keys:
            self._insert_sql = f"INSERT INTO {_qident(self.table)} ({cols_list}) VALUES ({placeholders})"
        else:
            non_keys = [c for c in self.columns if c not in self.upsert_keys]
            if isinstance(self.upsert_update, list):
                update_cols = [c for c in self.upsert_update if c in non_keys]
            elif self.upsert_update == "all":
                update_cols = non_keys
            else:
                update_cols = []
            conflict = ", ".join(_qident(k) for k in self.upsert_keys)
            if update_cols:
                set_clause = ", ".join(f"{_qident(c)}=excluded.{_qident(c)}" for c in update_cols)
                self._insert_sql = (
                    f"INSERT INTO {_qident(self.table)} ({cols_list}) VALUES ({placeholders}) "
                    f"ON CONFLICT({conflict}) DO UPDATE SET {set_clause}"
                )
            else:
                self._insert_sql = (
                    f"INSERT INTO {_qident(self.table)} ({cols_list}) VALUES ({placeholders}) "
                    f"ON CONFLICT({conflict}) DO NOTHING"
                )
                
    def _flush(self) -> None:
        if not self._pending:
            return
        assert self._conn is not None
        self._conn.executemany(self._insert_sql, self._pending)
        self._conn.commit()
        self._pending.clear()

    def write(self, rows: Iterable[Row]) -> None:
        assert self._conn is not None, "SqliteSink not opened"
        for r in rows:
            params = [r.get(col, None) for col in self.columns]
            self._pending.append(params)
            if len(self._pending) >= self.batch_size:
                self._flush()

    def close(self) -> None:
        try:
            self._flush()
        finally:
            if self._conn is not None:
                self._conn.close()
                self._conn = None


class NullSink(SinkCM):
    """Useful for dry-runs/benchmarks."""

    def open(self) -> None:  # pragma: no cover - trivial
        pass

    def write(self, rows: Iterable[Row]) -> None:  # pragma: no cover - trivial
        pass

    def close(self) -> None:  # pragma: no cover - trivial
        pass
