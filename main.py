from __future__ import annotations

import os, sys, time, subprocess, textwrap

from validator_csv import parse_and_validate
from emit import csv_rows_to_dicts, emit
from sinks import CsvSink

from typing import Literal
from pathlib import Path
import typer

from openai import OpenAI


RETRY_LIMIT = 2

# SYSTEM_PROMPT = """You are a data emitter. Return ONLY valid CSV inside one single ```csv fenced block.
# You will be given specific details about the CSV file, such as the header to use and the way to sort the data.
# Use RFC4180 quoting rules: quote fields that contain commas or quotes; escape quotes by doubling them.
# No commentary, no notes, no extra fences, no trailing text.
# """

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


def build_system_prompt(headers: list[str],
                        row_count: int | None = None,
                        sort_by: str | None = None) -> str:
    if not sort_by:
        sort_by = headers[0]
    
    lines = [
        "You are a data emitter. Return ONLY valid CSV inside one single ```csv fenced block.",
        f"Header MUST be exactly: {','.join(headers)}",
        "Use RFC4180 quoting rules: quote fields that contain commas or quotes; escape quotes by doubling them.",
        f"SORT rows alphabetically by {sort_by} (A→Z).",
    ]
    if row_count is not None:
        lines.append(f"Exactly {row_count} data rows (no more, no less).")
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


def run_once(prompt: str,
             headers: list[str],
             row_count: int | None = None,
             sort_by: str | None = None,
             output: str = "out/data.csv") -> str:
    """ """
    system_prompt = build_system_prompt(headers, row_count, sort_by)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt},
    ]

    for attempt in range(RETRY_LIMIT + 1):
        reply = call_llm(messages)
        errors, data = parse_and_validate(reply, headers, sort_by, row_count)
        if not errors:
            dict_rows = csv_rows_to_dicts(data)
            # print(dict_rows)
            # actual write happens with emit() -> the right sink
            emit(
                dict_rows,
                CsvSink(path=output, headers=headers),
            )
            return f"OUTPUT OK TO {output}"

        # build correction message and retry
        # prepare correction
        correction = CORRECTION_TEMPLATE.format(
            errors="\n".join(f"- {e}" for e in errors),
            original=reply,
        )
        messages.append({"role": "assistant", "content": reply})
        messages.append({"role": "user", "content": correction})

    # if still failing
    raise RuntimeError(f"Validation failed after {RETRY_LIMIT + 1} attempts.")


app = typer.Typer(help="LLM → Structured Data — CLI")

OutputFormat = Literal["csv"]  # keep only CSV for now


@app.command("run")
def cli_run(
    prompt: str = typer.Argument(
        ..., help="User prompt for gathering/generating data."
    ),
    columns: list[str] = typer.Option(
        "--columns", "--col", help="Name of column to use for the tabular data.",
    ),
    output: Path = typer.Option(
        "out/data.csv", "--output", "-o", help="Destination file path."
    ),
    row_count: int = typer.Option(
        20, "--rows", "-f", help="Number of rows."
    ),
    sort_by: str = typer.Option(
        "", "--sort-by", "-f", help="Name of column to sort by."
    ),
    format: OutputFormat = typer.Option(
        "csv", "--format", "-f", help="Output format (currently only CSV)."
    ),
) -> None:
    """
    Calls the existing run_once() using the provided prompt and writes raw CSV to --output.
    Example: python main.py "Produce the list of countries and their capitals." --col "country" --col "capital" --sort-by "country" --rows 25
    """
    if not sort_by:
        sort_by = columns[0]
    
    msg = f"▶ Running with prompt: {prompt!r} - Output details: {output}; {columns}"
    if sort_by == columns[0]:
        msg = f"{msg}, sort by 1st column"
    else:
        msg = f"{msg}, sort by column '{sort_by}'"
    typer.echo(msg, err=True)

    try:
        status = run_once(prompt, headers=columns, row_count=row_count, sort_by=sort_by, output=output)
    except Exception as e:
        typer.echo(f"❌ Generation failed: {e}", err=True)
        raise typer.Exit(code=2)

    typer.echo(f"✅ {status}")


if __name__ == "__main__":
    app()
