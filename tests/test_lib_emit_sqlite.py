import sqlite3
import pandas as pd
from lib import emit_df
from sinks import SqliteSink


def test_emit_sqlite_creates_table_and_inserts(tmp_path):
    df = pd.DataFrame([{"country": "France", "capital": "Paris"}, {"country": "Spain"}])

    status = emit_df(
        df,
        sink=SqliteSink(
            tmp_path / "demo.sqlite",
            "countries",
            columns=["country", "capital"],
            replace_table=True,
        ),
    )
    assert "Success" in status

    with sqlite3.connect(tmp_path / "demo.sqlite") as con:
        cur = con.cursor()
        cur.execute("SELECT country, capital FROM countries ORDER BY country")
        assert cur.fetchall() == [("France", "Paris"), ("Spain", None)]
