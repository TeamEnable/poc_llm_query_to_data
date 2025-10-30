# app.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, List

import gradio as gr
import pandas as pd

from lib import run_once  # your updated run_once that accepts sort_by + sink kwargs


def _parse_cols(csv_text: str | None) -> list[str]:
    if not csv_text:
        return []
    return [c.strip() for c in csv_text.split(",") if c.strip()]


def _parse_schema_json(text: str | None) -> Optional[list[str]]:
    """
    Accepts a JSON array of field names (e.g., ["country","capital"]).
    Returns None if empty.
    """
    if not text or not text.strip():
        return None
    import json
    try:
        data = json.loads(text)
        if not isinstance(data, list) or not all(isinstance(x, str) for x in data):
            raise ValueError("Schema JSON must be an array of strings.")
        return [x.strip() for x in data]
    except Exception as e:
        raise gr.Error(f"Invalid schema JSON: {e}") from e


def _status_for_sink(
    sink: str,
    csv_out: str,
    sqlite_db: str | None,
    sqlite_table: str | None,
    row_count: int,
) -> str:
    if sink == "sqlite":
        # mirror lib defaults if not provided
        db = sqlite_db or str(Path(csv_out).with_suffix(".sqlite"))
        table = sqlite_table or (Path(csv_out).stem or "data")
        return f"SQLite: {db} — table '{table}' with {row_count} rows"
    return f"CSV written to: {csv_out}"


def build_ui() -> gr.Blocks:
    with gr.Blocks(title="Prompt → Table (CSV / SQLite)") as demo:
        gr.Markdown("# Prompt → Table\nCSV by default; optionally persist to SQLite.")

        with gr.Row():
            prompt_in = gr.Textbox(
                label="Prompt",
                placeholder="e.g., List 20 EU countries and their capitals.",
                lines=3,
            )

        with gr.Row():
            cols_csv = gr.Textbox(
                label="Columns (comma-separated)",
                placeholder="country, capital",
                value="country, capital",
            )
            rows_in = gr.Number(label="Rows", value=20, precision=0)
            sort_by_in = gr.Textbox(
                label="Sort by (optional)",
                placeholder="e.g., country",
                value="",
            )

        with gr.Row():
            schema_json_in = gr.Textbox(
                label="Schema (JSON array, optional)",
                placeholder='["country", "capital"]',
            )

        gr.Markdown("### Output sink")

        with gr.Row():
            sink_choice = gr.Radio(
                choices=["csv", "sqlite"],
                value="csv",
                label="Sink",
            )
            csv_out_path = gr.Textbox(
                label="CSV output path",
                value="out/data.csv",
                placeholder="out/data.csv",
            )

        with gr.Row(visible=False) as sqlite_opts:
            sqlite_db = gr.Textbox(
                label="SQLite DB path",
                value="out/data.sqlite",
                placeholder="out/data.sqlite",
            )
            sqlite_table = gr.Textbox(
                label="SQLite table name",
                value="data",
                placeholder="data",
            )
            sqlite_replace = gr.Checkbox(
                label="Replace table (DROP & CREATE)",
                value=False,
            )

        with gr.Row():
            gen_btn = gr.Button("Generate", variant="primary")
            status = gr.Markdown(visible=False)

        tbl_out = gr.Dataframe(
            headers=["country", "capital"],
            row_count=10,
            visible=False,
            label="Preview (first 10 rows)",
        )

        # Toggle SQLite options visibility
        def _toggle_sqlite(sink: str):
            return gr.update(visible=(sink == "sqlite"))

        sink_choice.change(_toggle_sqlite, inputs=[sink_choice], outputs=[sqlite_opts])

        # Core handler
        def on_generate(
            prompt: str,
            columns_csv: str,
            rows: int | float,
            sort_by: str | None,
            schema_json: str | None,
            sink: str,
            csv_out: str,
            db_path: str | None,
            table: str | None,
            replace_tbl: bool,
        ):
            if not prompt or not str(prompt).strip():
                raise gr.Error("Please provide a prompt.")
            try:
                n_rows = int(rows) if rows is not None else 20
            except Exception:
                n_rows = 20

            columns = _parse_cols(columns_csv)
            schema_fields = _parse_schema_json(schema_json)

            # small optim: pass schema through as columns, if no columns provided
            if not columns and schema_fields:
                columns = schema_fields[:]

            sort_by_val = (sort_by or "").strip() or None

            # Build sink kwargs only when SQLite is selected (keeps CSV path authoritative)
            sink_kwargs = {}
            if sink == "sqlite":
                sink_kwargs = {
                    "sink": "sqlite",
                    "sqlite_db": db_path or None,
                    "sqlite_table": table or None,
                    "sqlite_replace": bool(replace_tbl),
                }

            # Delegate to lib.run_once (now with sort_by)
            df, info = run_once(
                prompt=prompt,
                columns=columns,
                row_count=n_rows,
                sort_by=sort_by_val,
                output=csv_out or "out/data.csv",
                schema_fields=schema_fields,
                **sink_kwargs,
            )

            # Status message
            msg_bits: List[str] = []
            msg_bits.append(
                _status_for_sink(
                    sink=sink,
                    csv_out=csv_out or "out/data.csv",
                    sqlite_db=db_path,
                    sqlite_table=table,
                    row_count=len(df),
                )
            )

            # Projection info (if present)
            proj = (info or {}).get("projection")
            if proj:
                kept = proj.get("kept", [])
                added = proj.get("added_empty", [])
                dropped = proj.get("dropped", [])
                msg_bits.append(
                    f"Projection → kept: {kept} | added(NULL): {added} | dropped: {dropped}"
                )

            # Return a 10-row preview for UX
            preview = df.head(10)
            return gr.update(visible=True, value="\n\n".join(msg_bits)), gr.update(
                visible=True, value=preview
            )

        gen_btn.click(
            fn=on_generate,
            inputs=[
                prompt_in,
                cols_csv,
                rows_in,
                sort_by_in,
                schema_json_in,
                sink_choice,
                csv_out_path,
                sqlite_db,
                sqlite_table,
                sqlite_replace,
            ],
            outputs=[status, tbl_out],
        )

    return demo


if __name__ == "__main__":
    ui = build_ui()
    ui.launch()
