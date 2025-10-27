from validator_csv import parse_and_validate
from typing import List, Iterable, Optional, Dict, Any
import pandas as pd

from sinks import Sink, Row, CsvSink, SqliteSink

from openai import OpenAI


RETRY_LIMIT = 2

CORRECTION_TEMPLATE = """Your previous output was invalid for these reasons:
{errors}

Re-emit the result as CSV ONLY, in one single ```csv fenced block,
with the CSV file specs previously provided,
RFC4180 quoting. No commentary.
Here is your previous output to fix, do NOT add explanations:

{original}
"""

# You need to set your OpenAI API key
# export OPENAI_API_KEY=sk-...

try:
    _client = OpenAI()
except Exception as e:
    print(e)


def build_system_prompt(headers: list[str], row_count: int | None = None) -> str:
    lines = [
        "You are a data emitter. Return ONLY valid CSV inside one single ```csv fenced block.",
        f"Header MUST be exactly: {','.join(headers)}",
        "Use RFC4180 quoting rules: quote fields that contain commas or quotes; escape quotes by doubling them.",
    ]
    if row_count is not None:
        lines.append(f"Return exactly {row_count} data rows, in addition to the header row.")

    return "\n".join(lines)


def call_llm(messages: list[dict]) -> str:
    """
    messages: [{"role": "system"|"user"|"assistant", "content": str}, ...]
    returns: assistant text (str)
    """
    try:
        resp = _client.chat.completions.create(
            model="gpt-4o-mini",  # good cost/latency for CSV emission
            messages=messages,
            temperature=0,
            top_p=1,
            max_tokens=1500,
            # seed=42,  # improves reproducibility across runs
        )
        return resp.choices[0].message.content or ""
    except Exception as e:
        # Let the driver handle retries/logging
        raise RuntimeError(f"OpenAI API error: {e}")


def csv_rows_to_dicts(matrix: List[List[str]]) -> List[Row]:
    """
    Convert parsed CSV matrix (including header row) into list[dict].
    Assumes header matches headers (already validated in your validator).
    """
    header = matrix[0]
    rows = matrix[1:]
    return [dict(zip(header, r)) for r in rows]


def emit(rows: Iterable[Row], sink: Sink) -> None:
    try:
        with sink:
            sink.write(rows)
        return "Success: {type(sink)}"
    except Exception as e:
        return f"Failure: {e}"


def emit_df(df, sink: Sink) -> None:
    return emit(df.to_dict(orient="records"), sink)


# def _apply_schema_projection(df: pd.DataFrame, schema_fields: list[str]) -> tuple[pd.DataFrame, dict]:
#     out_cols, missing = {}, []
#     for name in schema_fields:
#         if name in df.columns:
#             out_cols[name] = df[name]
#         else:
#             out_cols[name] = pd.Series([pd.NA] * len(df))  # <-- was ""
#             missing.append(name)

#     dropped = [c for c in df.columns if c not in schema_fields]
#     info = {
#         "kept": len(schema_fields) - len(missing),
#         "added_empty": len(missing),
#         "dropped": len(dropped),
#         "missing_fields": missing,
#         "dropped_fields": dropped,
#     }
#     return pd.DataFrame(out_cols), info

# -----------------------
# Helpers for projection
# -----------------------

def _normalize_df_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace on column names."""
    df = df.copy()
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    return df

def _project_dataframe(df: pd.DataFrame, target_fields: list[str]) -> pd.DataFrame:
    """
    Return a frame with exactly target_fields in that order.
    Missing fields are added with None; extra fields are dropped.
    """
    df = df.copy()
    df_cols = set(df.columns)
    data = {}
    for f in target_fields:
        if f in df_cols:
            data[f] = df[f]
        else:
            data[f] = pd.Series([None] * len(df), index=df.index)
    return pd.DataFrame(data, index=df.index)

def _projection_report(df_before: pd.DataFrame, df_after: pd.DataFrame, target_fields: list[str]) -> Dict[str, Any]:
    before_cols = [str(c) for c in df_before.columns]
    after_cols  = [str(c) for c in df_after.columns]
    kept = [c for c in after_cols if c in before_cols]
    added_empty = [c for c in after_cols if c not in before_cols]
    dropped = [c for c in before_cols if c not in after_cols]
    return {
        "target": target_fields,
        "kept": kept,
        "added_empty": added_empty,
        "dropped": dropped,
        "missing_fields": added_empty,
        "dropped_fields": dropped,
    }

def _records_for_sink(df: pd.DataFrame, sink_kind: str) -> list[dict]:
    """
    Convert to list-of-dicts with proper null handling:
      - CSV: NaN -> "" (empty string)
      - SQLite: NaN -> None
    """
    if sink_kind == "sqlite":
        return df.where(pd.notna(df), None).to_dict(orient="records")
    # CSV
    return df.where(pd.notna(df), "").to_dict(orient="records")


# -----------------------
# Orchestrator
# -----------------------

def run_once(
    prompt: str,
    columns: list[str],
    row_count: int | None = None,
    output: str = "out/data.csv",
    schema_fields: Optional[list[str]] = None,
    **sink_kwargs,
):
    """
    Updated flow:
      1) Get LLM reply, validate, convert to dict rows.
      2) Build DataFrame directly from dict_rows (no CSV round-trip).
      3) Normalize headers and project to target fields (columns > schema_fields > produced).
      4) Persist via the selected sink (CSV default, SQLite optional) with sink-appropriate null handling.
      5) Return (df, info)
    """
    success: bool = False
    info: Dict[str, Any] = {}
    df: pd.DataFrame | None = None

    system_prompt = build_system_prompt(columns, row_count)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt},
    ]

    for attempt in range(RETRY_LIMIT + 1):
        reply = call_llm(messages)
        errors, data = parse_and_validate(reply, columns, row_count)
        if not errors:
            # 1) LLM CSV text -> dict rows
            dict_rows = csv_rows_to_dicts(data)

            # 2) Build DF directly (respect requested order from `columns`)
            df_raw = pd.DataFrame(dict_rows)
            df_raw = _normalize_df_headers(df_raw)

            # Decide target fields: explicit `columns` > `schema_fields` > current df columns
            if columns:
                target_fields = [c.strip() for c in columns]
            elif schema_fields:
                target_fields = [c.strip() for c in schema_fields]
            else:
                target_fields = list(df_raw.columns)

            # 3) Project
            df_proj = _project_dataframe(df_raw, target_fields)
            info["projection"] = _projection_report(df_raw, df_proj, target_fields)

            # 4) Persist via selected sink
            sink_kind = sink_kwargs.get("sink", "csv")
            if sink_kind == "sqlite":
                db_path = sink_kwargs.get("sqlite_db") or str(Path(output).with_suffix(".sqlite"))
                table = sink_kwargs.get("sqlite_table") or (Path(output).stem or "data")
                replace = bool(sink_kwargs.get("sqlite_replace", False))
                sink = SqliteSink(db_path, table, columns=target_fields, replace_table=replace)
            else:
                sink = CsvSink(path=output, headers=target_fields)

            rows_to_emit = _records_for_sink(df_proj, sink_kind)
            print("Ready to persist data received from LLM")
            status = emit(rows_to_emit, sink)

            if "Success" in status:
                success = True
                df = df_proj
                break
            elif "Failure" in status:
                raise RuntimeError("Error while persisting the data")

        # retry with correction
        correction = CORRECTION_TEMPLATE.format(
            errors="\n".join(f"- {e}" for e in errors),
            original=reply,
        )
        messages.append({"role": "assistant", "content": reply})
        messages.append({"role": "user", "content": correction})

    if not success or df is None:
        raise RuntimeError(f"Validation failed after {RETRY_LIMIT + 1} attempts.")

    # Return projected DF (already persisted) + info for logs
    return df.reset_index(drop=True), info
