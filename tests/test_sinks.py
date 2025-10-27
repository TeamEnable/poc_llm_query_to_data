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


def test_sqlite_sink_creates_table_and_inserts_rows(tmp_path):
    db_path = tmp_path / "test.sqlite"
    table = "countries"
    columns = ["country", "capital"]

    rows = [
        {"country": "France", "capital": "Paris"},
        {"country": "Spain"},  # capital omitted on purpose -> should become NULL
    ]

    # Act: write rows
    with sinks.SqliteSink(str(db_path), table=table, columns=columns) as s:
        s.write(rows)

    # Assert: table exists, rows present, values correct
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # 1) table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    assert cur.fetchone() == (table,)

    # 2) row count
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    assert cur.fetchone()[0] == 2

    # 3) values (order by country for determinism)
    cur.execute(f"SELECT country, capital FROM {table} ORDER BY country ASC")
    data = cur.fetchall()
    assert data == [
        ("France", "Paris"),
        ("Spain", None),  # missing key -> NULL
    ]

    con.close()


def test_sqlitesink_write_before_open_raises(tmp_path):
    db_path = tmp_path / "x.db"
    sink = sinks.SqliteSink(str(db_path), table="t", schema={"a": "TEXT"})
    with pytest.raises(AssertionError) as exc:
        sink.write([{"a": "v"}])
    assert "SqliteSink not opened" in str(exc.value)


def test_nullsink_noop(tmp_path):
    s = sinks.NullSink()
    s.open()
    s.write([{"anything": "goes"}])
    s.close()
    # Nothing to assert—just ensure no exceptions
