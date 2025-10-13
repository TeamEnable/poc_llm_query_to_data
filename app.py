from typing import List, Optional

import gradio as gr
import pandas as pd

# =============================================================
# 1) PROMPT ➜ TABLE
# =============================================================


def _core_run_once(
    prompt: str, columns: List[str], rows: int, sort_by: Optional[str]
) -> pd.DataFrame:
    """Adapter to the core run_once function.
    Loads a dataframe once the data has been written...
    """
    from lib import run_once

    if not sort_by:
        sort_by = columns[0]
    res = run_once(prompt=prompt, columns=columns, row_count=rows)
    if res.startswith("OUTPUT OK"):
        with open("out/data.csv") as f:
            df = load_df(f)
            if sort_by in df.columns:
                df = df.sort_values(by=sort_by)
            return df.reset_index(drop=True)


def load_df(file_obj) -> pd.DataFrame:
    file_obj.seek(0)
    return pd.read_csv(file_obj)


# =============================================================
# Gradio UI
# =============================================================
with gr.Blocks(title="Demo UX — Prompt") as demo:
    gr.Markdown("# Demo UX — Prompt to Table")

    with gr.Tabs():
        # ------------------ TAB 1: PROMPT ➜ TABLE ------------------
        with gr.TabItem("Prompt → Table"):
            gr.Markdown(
                "Use your LLM-powered runner to generate a small table from a prompt."
            )
            prompt_in = gr.Textbox(
                label="Prompt",
                placeholder="e.g. Produce the list of countries and their capitals.",
            )
            cols_csv = gr.Textbox(
                label="Columns (comma-separated)", placeholder="e.g. country, capital"
            )
            with gr.Row():
                rows_in = gr.Number(label="Rows", value=20, precision=0)
                sort_by_in = gr.Textbox(
                    label="Sort by (optional)", placeholder="column name"
                )
            gen_btn = gr.Button("Generate Table")
            tbl_out = gr.Dataframe(label="Preview")
            status1 = gr.Markdown(visible=False)  # write status

            def on_generate(prompt: str, cols: str, rows: float, sort_by: str):
                columns = [c.strip() for c in (cols or "").split(",") if c.strip()]
                n_rows = int(rows or 0)
                df = _core_run_once(
                    prompt=prompt or "",
                    columns=columns,
                    rows=n_rows,
                    sort_by=(sort_by or None),
                )
                return df.head(10), gr.update(
                    visible=True,
                    value=f"Previewing first 10 rows. Total rows: {len(df)}",
                )

            gen_btn.click(
                on_generate,
                inputs=[prompt_in, cols_csv, rows_in, sort_by_in],
                outputs=[tbl_out, status1],
            )


if __name__ == "__main__":
    demo.launch()
