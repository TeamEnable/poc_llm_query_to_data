import os, sys, time, subprocess, textwrap

from validator_csv import parse_and_validate
from emit import csv_rows_to_dicts, emit
from sinks import CsvSink

from openai import OpenAI


RETRY_LIMIT = 2

SYSTEM_PROMPT = """You are a data emitter. Return ONLY valid CSV inside one single ```csv fenced block.
Header MUST be exactly: country,capital
Exactly 20 data rows (no more, no less).
Sort rows alphabetically by country (A→Z).
Use RFC4180 quoting rules: quote fields that contain commas or quotes; escape quotes by doubling them.
No commentary, no notes, no extra fences, no trailing text.
"""

USER_PROMPT = "Produce the list of countries and their capitals. Limit to the first 20 alphabetically by country."

CORRECTION_TEMPLATE = """Your previous output was invalid for these reasons:
{errors}

Re-emit the result as CSV ONLY, in one single ```csv fenced block,
with header exactly `country,capital`, exactly 20 rows, sorted by country (A→Z),
RFC4180 quoting. No commentary.
Here is your previous output to fix, do NOT add explanations:

{original}
"""

# You need to set your OpenAI API key
# export OPENAI_API_KEY=sk-...


_client = OpenAI()


def call_llm(messages: list[dict]) -> str:
    """
    messages: [{"role": "system"|"user"|"assistant", "content": str}, ...]
    returns: assistant text (str)
    """
    try:
        resp = _client.chat.completions.create(
            model="gpt-4o-mini",      # good cost/latency for CSV emission
            messages=messages,
            temperature=0,
            top_p=1,
            max_tokens=1500,
            seed=42,                  # improves reproducibility across runs
        )
        return resp.choices[0].message.content or ""
    except Exception as e:
        # Let the driver handle retries/logging
        raise RuntimeError(f"OpenAI API error: {e}")


# def validate_with_subprocess(payload: str) -> tuple[bool, list[str]]:
#     p = subprocess.Popen(
#         [sys.executable, "validator_csv.py"],
#         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
#         text=True
#     )
#     out, err = p.communicate(payload, timeout=30)
#     ok = (p.returncode == 0)
#     errors = []
#     if not ok:
#         # first line is "INVALID", subsequent lines are errors
#         lines = [ln for ln in out.splitlines() if ln.strip()]
#         if len(lines) >= 2:
#             errors = lines[1:]
#         else:
#             errors = ["unknown validation error"]
#     return ok, errors

def run_once():
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT},
    ]

    for attempt in range(RETRY_LIMIT + 1):
        reply = call_llm(messages)
        errors, data = parse_and_validate(reply)
        if not errors:
            dict_rows = csv_rows_to_dicts(data)
            emit(dict_rows, CsvSink(path="out/countries.csv", headers=["country","capital"]))
            return "OUTPUT OK"

        # build correction message and retry
        # prepare correction
        correction = CORRECTION_TEMPLATE.format(
            errors="\n".join(f"- {e}" for e in errors),
            original=reply,
        )
        messages.append({"role": "assistant", "content": reply})
        messages.append({"role": "user", "content": correction})
        
    # if still failing
    raise RuntimeError(f"Validation failed after {RETRY_LIMIT+1} attempts.")


if __name__ == "__main__":
    try:
        result = run_once()
        print(result)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
