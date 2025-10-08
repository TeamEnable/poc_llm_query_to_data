import csv
import os
import sqlite3
import pytest

import sinks


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def test_csvsink_writes_header_and_rows(tmp_path):
    out = tmp_path / "nested" / "dir" / "out.csv"
    headers = ["colA", "colB", "colC"]
    rows = [
        {"colA": "A", "colB": "x", "colC": "u"},
        {"colA": "B", "colB": "y", "colC": "v"},
    ]

    sink = sinks.CsvSink(str(out), headers=headers, newline="\n")
    sink.open()
    try:
        sink.write(rows)
    finally:
        sink.close()

    assert os.path.exists(out)
    content = read_csv(out)
    assert content[0] == headers
    assert content[1:] == [["A", "x", "u"], ["B", "y", "v"]]


def test_csvsink_write_before_open_raises(tmp_path):
    out = tmp_path / "out.csv"
    sink = sinks.CsvSink(str(out), headers=["h1", "h2"])
    with pytest.raises(AssertionError) as exc:
        sink.write([{"h1": "a", "h2": "b"}])
    assert "CsvSink not opened" in str(exc.value)


def test_sqlitesink_inserts_rows_and_creates_table(tmp_path):
    db_path = tmp_path / "t.db"
    table = "records"
    schema = {"colA": "TEXT", "colB": "TEXT", "colC": "TEXT"}
    rows = [
        {"colA": "A", "colB": "x", "colC": "u"},
        {"colA": "B", "colB": "y", "colC": "v"},
    ]

    sink = sinks.SQLiteSink(
        str(db_path), table=table, schema=schema, if_not_exists=True
    )
    sink.open()
    try:
        sink.write(rows)
    finally:
        sink.close()

    # Verify with a fresh connection
    con = sqlite3.connect(str(db_path))
    try:
        # table exists?
        cur = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        assert cur.fetchone() is not None

        # row count
        cnt = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert cnt == 2

        # content matches column order defined in schema
        fetched = con.execute(
            f"SELECT colA, colB, colC FROM {table} ORDER BY colA"
        ).fetchall()
        assert fetched == [("A", "x", "u"), ("B", "y", "v")]
    finally:
        con.close()


def test_sqlitesink_write_before_open_raises(tmp_path):
    db_path = tmp_path / "x.db"
    sink = sinks.SQLiteSink(str(db_path), table="t", schema={"a": "TEXT"})
    with pytest.raises(AssertionError) as exc:
        sink.write([{"a": "v"}])
    assert "SQLiteSink not opened" in str(exc.value)


def test_nullsink_noop(tmp_path):
    s = sinks.NullSink()
    s.open()
    s.write([{"anything": "goes"}])
    s.close()
    # Nothing to assert—just ensure no exceptions
