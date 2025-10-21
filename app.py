from typing import List, Optional
import json
import gradio as gr
import pandas as pd

# --- helper: same behavior as CLI ---
def parse_schema_json(text: str | None) -> list[str] | None:
    if not text or not text.strip():
        return None
    raw = json.loads(text)
    if isinstance(raw, list):
        fields = [str(x) for x in raw]
    elif isinstance(raw, dict) and isinstance(raw.get("fields"), list):
        fields = [str(x) for x in raw["fields"]]
    else:
        raise ValueError("Schema must be a list or an object with a 'fields' array.")
    # de-dupe, preserve order
    seen, uniq = set(), []
    for f in fields:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    return uniq

# =============================================================
# 1) PROMPT ➜ TABLE
# =============================================================
def _core_run_once(
    prompt: str, columns: List[str], rows: int, sort_by: Optional[str],
    schema_fields: Optional[List[str]] = None,          # NEW: pass-through
) -> pd.DataFrame:
    """
    Adapter to the core run_once function.
    Loads a dataframe once the data has been written...
    """
    from lib import run_once

    # If no sort_by provided, pick first hint/schema column if available
    if not sort_by and (columns or schema_fields):
        sort_by = (columns or schema_fields)[0]

    # HINT: if no columns provided but schema exists, use schema as hint
    columns_hint = columns or (schema_fields or [])

    res = run_once(
        prompt=prompt,
        columns=columns_hint,
        row_count=rows,
        schema_fields=schema_fields,
    )

    return res[0]


def load_df(file_obj) -> pd.DataFrame:
    file_obj.seek(0)
    return pd.read_csv(file_obj)

# =============================================================
# Gradio UI
# =============================================================
with gr.Blocks(title="Demo UX — Prompt") as demo:
    gr.Markdown("# Demo UX — Prompt to Table")

    with gr.Tabs():
        with gr.TabItem("Prompt → Table"):
            gr.Markdown("Use your LLM-powered runner to generate a small table from a prompt.")
            prompt_in = gr.Textbox(label="Prompt", placeholder="e.g. Produce the list of countries and their capitals.")
            cols_csv = gr.Textbox(label="Columns (comma-separated)", placeholder="e.g. country, capital")
            schema_json_in = gr.Textbox(                     # NEW
                label="Schema JSON (optional, same as CLI)",
                placeholder='e.g. ["country","capital","iso2","continent"] or {"fields":[...] }',
                lines=4,
            )
            with gr.Row():
                rows_in = gr.Number(label="Rows", value=20, precision=0)
                sort_by_in = gr.Textbox(label="Sort by (optional)", placeholder="column name")
            gen_btn = gr.Button("Generate Table")
            tbl_out = gr.Dataframe(label="Preview")
            status1 = gr.Markdown(visible=False)  # write status

            def on_generate(prompt: str, cols: str, rows: float, sort_by: str, schema_text: str):
                # parse inputs
                columns = [c.strip() for c in (cols or "").split(",") if c.strip()]
                try:
                    schema_fields = parse_schema_json(schema_text)
                except Exception as e:
                    return None, gr.update(visible=True, value=f"❌ Invalid schema JSON: {e}")

                # guardrail: if both empty, still allowed (generator decides)
                df = _core_run_once(
                    prompt=prompt or "",
                    columns=columns,
                    rows=int(rows or 0),
                    sort_by=(sort_by or None),
                    schema_fields=schema_fields,     # NEW: pass-through to core
                )
                if df is None or not isinstance(df, pd.DataFrame):
                    return None, gr.update(visible=True, value="❌ No data produced or run failed.")

                # UI mirrors CLI semantics: preview 10 rows, note schema usage
                msg_bits = [f"Previewing first 10 rows. Total rows: {len(df)}"]
                if schema_fields:
                    msg_bits.append(f"Schema applied ({len(schema_fields)} fields). Missing fields are NULL-filled.")
                return df.head(10), gr.update(visible=True, value=" — ".join(msg_bits))

            gen_btn.click(
                on_generate,
                inputs=[prompt_in, cols_csv, rows_in, sort_by_in, schema_json_in],   # NEW: include schema textbox
                outputs=[tbl_out, status1],
            )

if __name__ == "__main__":
    demo.launch()
