from validator_csv import parse_and_validate
from typing import List, Iterable
from sinks import Sink, Row, CsvSink


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


def csv_rows_to_dicts(matrix: List[List[str]]) -> List[Row]:
    """
    Convert parsed CSV matrix (including header row) into list[dict].
    Assumes header matches headers (already validated in your validator).
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


def run_once(
    prompt: str,
    columns: list[str],
    row_count: int | None = None,
    output: str = "out/data.csv",
) -> str:
    """ """
    system_prompt = build_system_prompt(columns, row_count)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt},
    ]

    for attempt in range(RETRY_LIMIT + 1):
        reply = call_llm(messages)
        errors, data = parse_and_validate(reply, columns, row_count)
        if not errors:
            dict_rows = csv_rows_to_dicts(data)
            # print(dict_rows)
            # actual write happens with emit() -> the right sink
            emit(
                dict_rows,
                CsvSink(path=output, headers=columns),
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
