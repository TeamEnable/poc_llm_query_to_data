
from pathlib import Path
import typer
from typing import Literal

from lib import run_once


app = typer.Typer(help="LLM → Structured Data — CLI")

OutputFormat = Literal["csv"]  # keep only CSV for now


@app.command("run")
def cli_run(
    prompt: str = typer.Argument(
        ..., help="User prompt for gathering/generating data."
    ),
    columns: list[str] = typer.Option(
        "--columns",
        "--col",
        help="Name of column to use for the tabular data.",
    ),
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
        status = run_once(
            prompt, headers=columns, row_count=row_count, sort_by=sort_by, output=output
        )
    except Exception as e:
        typer.echo(f"❌ Generation failed: {e}", err=True)
        raise typer.Exit(code=2)

    typer.echo(f"✅ {status}")


if __name__ == "__main__":
    app()
