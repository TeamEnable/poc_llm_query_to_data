# app.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, List

import gradio as gr

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
    docx_path: str | None = None,
) -> str:
    if sink == "sqlite":
        # mirror lib defaults if not provided
        db = sqlite_db or str(Path(csv_out).with_suffix(".sqlite"))
        table = sqlite_table or (Path(csv_out).stem or "data")
        return f"SQLite: {db} — table '{table}' with {row_count} rows"
    elif sink == "docx":
        path = docx_path or str(Path(csv_out).with_suffix(".docx"))
        return f"DOCX written to: {path} (rows: {row_count})"
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
                choices=["csv", "sqlite", "docx"],
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

        # --- UPSERT options (SQLite only) ---
        with gr.Row(visible=False) as sqlite_upsert_opts:
            upsert_keys_in = gr.Textbox(
                label="UPSERT keys (comma-separated)",
                placeholder="e.g., iso2 or country,iso2",
                value="",
            )
            upsert_policy = gr.Dropdown(
                label="UPSERT update policy",
                choices=["all", "none", "only listed columns"],
                value="all",
            )
        with gr.Row(visible=False) as sqlite_upsert_cols_row:
            upsert_cols_in = gr.Textbox(
                label="Columns to update on conflict (comma-separated)",
                placeholder="e.g., capital,continent",
                value="",
            )

        # Docx options row (hidden until docx selected)
        with gr.Row(visible=False) as docx_opts:
            docx_path = gr.Textbox(label="DOCX path", value="out/data.docx")
            docx_title = gr.Textbox(label="DOCX title", value="Generated Data")

        # Generate button
        with gr.Row():
            gen_btn = gr.Button("Generate", variant="primary")
            status = gr.Markdown(visible=False)

        tbl_out = gr.Dataframe(
            headers=["country", "capital"],
            row_count=10,
            visible=False,
            label="Preview (first 10 rows)",
        )

        # Toggle SQLite / DOCX options visibility
        def _toggle_sqlite(sink: str):
            is_sqlite = (sink == "sqlite")
            is_docx = (sink == "docx")
            return (
                gr.update(visible=is_sqlite),          # sqlite_opts
                gr.update(visible=is_sqlite),          # sqlite_upsert_opts (if present)
                gr.update(visible=False),              # sqlite_upsert_cols_row
                gr.update(visible=is_docx),            # docx_opts
             )

        sink_choice.change(
            _toggle_sqlite,
            inputs=[sink_choice],
            outputs=[sqlite_opts, sqlite_upsert_opts, sqlite_upsert_cols_row, docx_opts],
        )

        def _toggle_upsert_cols(policy: str):
            return gr.update(visible=(policy == "only listed columns"))

        upsert_policy.change(
            _toggle_upsert_cols,
            inputs=[upsert_policy],
            outputs=[sqlite_upsert_cols_row],
        )

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
            upsert_keys_text: str,
            upsert_policy_val: str,
            upsert_cols_text: str,
            docx_path_val: str | None,
            docx_title_val: str | None,
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
                # Start by parsing UPSERT keys
                keys = [
                    c.strip() for c in (upsert_keys_text or "").split(",") if c.strip()
                ]
                # Parse UPSERT policy
                if upsert_policy_val == "all":
                    update_spec = "all"
                elif upsert_policy_val == "none":
                    update_spec = "none"
                else:
                    update_spec = [
                        c.strip()
                        for c in (upsert_cols_text or "").split(",")
                        if c.strip()
                    ]

                sink_kwargs = {
                    "sink": "sqlite",
                    "sqlite_db": db_path or None,
                    "sqlite_table": table or None,
                    "sqlite_replace": bool(replace_tbl),
                    "sqlite_upsert_keys": keys or None,
                    "sqlite_upsert_update": update_spec,  # "all" | "none" | list[str]
                }

            if sink == "docx":
                sink_kwargs = {
                    "sink": "docx",
                    "docx_path": docx_path_val or None,
                    "docx_title": docx_title_val or None,
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
            if sink == "sqlite" and (keys or upsert_policy_val != "all"):
                msg_bits.append(
                    f"UPSERT → keys: {keys or '—'} | policy: {upsert_policy_val if upsert_policy_val != 'only listed columns' else upsert_cols_text}"
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
                upsert_keys_in,
                upsert_policy,
                upsert_cols_in,
                docx_path,
                docx_title,
            ],
            outputs=[status, tbl_out],
        )

    return demo


if __name__ == "__main__":
    ui = build_ui()
    ui.launch()
