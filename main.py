from pathlib import Path
import json
import typer
from typing import Literal, Optional

from lib import run_once


def _parse_schema(schema_json: Optional[str], schema_file: Optional[Path]) -> list[str] | None:
    if not schema_json and not schema_file:
        return None
    if schema_json and schema_file:
        raise typer.BadParameter("Use only one of --schema-json or --schema-file.")
    raw = json.loads(Path(schema_file).read_text("utf-8")) if schema_file else json.loads(schema_json)
    if isinstance(raw, list):
        fields = [str(x) for x in raw]
    elif isinstance(raw, dict) and isinstance(raw.get("fields"), list):
        fields = [str(x) for x in raw["fields"]]
    else:
        raise typer.BadParameter("Schema must be a list or {\"fields\": [...]}.")

    seen, uniq = set(), []
    for f in fields:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    return uniq


app = typer.Typer(help="LLM → Structured Data — CLI")

OutputFormat = Literal["csv"]  # keep only CSV for now


@app.command("run")
def cli_run(
    prompt: str = typer.Argument(
        ..., help="User prompt for gathering/generating data."
    ),
    col: list[str] = typer.Option(None, "--col", help="Name of column to use for the tabular data."),
    output: Path = typer.Option(
        "out/data.csv", "--output", "-o", help="Destination file path."
    ),
    row_count: int = typer.Option(20, "--rows", "-f", help="Number of rows."),
    sort_by: str = typer.Option(
        "", "--sort-by", "-f", help="Name of column to sort by."
    ),
    format: OutputFormat = typer.Option(
        "csv", "--format", "-f", help="Output format (currently only CSV)."
    ),
    schema_json: Optional[str] = typer.Option(None, "--schema-json"),
    schema_file: Optional[Path] = typer.Option(None, "--schema-file"),
) -> None:
    """
    Calls the existing run_once() using the provided prompt and writes raw CSV to --output.
    Example: python main.py "Produce the list of countries and their capitals." --col "country" --col "capital" --sort-by "country" --rows 25
    """
    schema_fields = _parse_schema(schema_json, schema_file)

    # columns = generator hint; if no columns but we do have a schema, use schema as hint
    columns_hint = col or (schema_fields or [])

    if not sort_by:
        sort_by = columns_hint[0]

    msg = f"▶ Running with prompt: {prompt!r} - Output details: {output}; {columns_hint}"
    if sort_by == columns_hint[0]:
        msg = f"{msg}, we will sort by 1st column"
    else:
        msg = f"{msg}, we will sort by column '{sort_by}'"
    typer.echo(msg, err=True)

    try:
        res = run_once(
            prompt, 
            columns=columns_hint,
            row_count=row_count,
            output=output,
            schema_fields=schema_fields
        )
    except Exception as e:
        typer.echo(f"❌ Generation failed: {e}", err=True)
        raise typer.Exit(code=2)

    typer.echo(f"OUTPUT ✅: {res[1]}")
    typer.echo(res[0].head())


if __name__ == "__main__":
    app()
