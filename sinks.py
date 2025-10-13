from __future__ import annotations
from typing import Protocol, Iterable, Dict, List, runtime_checkable, Optional
import csv
import sqlite3
import os

Row = Dict[str, str]  # e.g., {"country": "...", "capital": "..."}


@runtime_checkable
class Sink(Protocol):
    def open(self) -> None: ...
    def write(self, rows: Iterable[Row]) -> None: ...
    def close(self) -> None: ...


class CsvSink:
    def __init__(self, path: str, headers: List[str], newline: str = "\n"):
        self.path = path
        self.headers = headers
        self.newline = newline
        self._fh = None
        self._writer = None

    def open(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._fh = open(self.path, "w", newline="")
        self._writer = csv.DictWriter(
            self._fh, fieldnames=self.headers, lineterminator=self.newline
        )
        self._writer.writeheader()

    def write(self, rows: Iterable[Row]) -> None:
        assert self._writer is not None, "CsvSink not opened"
        for r in rows:
            self._writer.writerow(r)

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
            self._writer = None


class SQLiteSink:
    def __init__(
        self,
        db_path: str,
        table: str,
        schema: Dict[str, str],
        if_not_exists: bool = True,
    ):
        """
        schema: mapping of column -> SQL type (e.g., {"country": "TEXT", "capital": "TEXT"})
        """
        self.db_path = db_path
        self.table = table
        self.schema = schema
        self.if_not_exists = if_not_exists
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        self._conn = sqlite3.connect(self.db_path)
        cols = ", ".join([f'"{k}" {v}' for k, v in self.schema.items()])
        ine = "IF NOT EXISTS " if self.if_not_exists else ""
        self._conn.execute(f"CREATE TABLE {ine}{self.table} ({cols})")
        self._conn.commit()

    def write(self, rows: Iterable[Row]) -> None:
        assert self._conn is not None, "SQLiteSink not opened"
        cols = list(self.schema.keys())
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT INTO {self.table} ({','.join(cols)}) VALUES ({placeholders})"
        values = [[r.get(c) for c in cols] for r in rows]
        self._conn.executemany(sql, values)
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


class NullSink:
    """Useful for dry-runs/benchmarks."""

    def open(self) -> None:
        pass

    def write(self, rows: Iterable[Row]) -> None:
        pass

    def close(self) -> None:
        pass
