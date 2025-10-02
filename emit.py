from typing import List, Dict, Iterable
from sinks import Sink, Row

HEADERS = ["country", "capital"]

def csv_rows_to_dicts(matrix: List[List[str]]) -> List[Row]:
    """
    Convert parsed CSV matrix (including header row) into list[dict].
    Assumes header matches HEADERS (already validated in your validator).
    """
    header = matrix[0]
    rows = matrix[1:]
    return [dict(zip(header, r)) for r in rows]

def emit(rows: Iterable[Row], sink: Sink) -> None:
    sink.open()
    try:
        sink.write(rows)
    finally:
        sink.close()
