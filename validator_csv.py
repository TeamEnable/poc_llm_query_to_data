import csv, io, sys, re

HEADER = ["country", "capital"]
ROW_COUNT = 20

def extract_code_fence(text: str) -> str:
    """
    Extracts content from a single ```csv ... ``` block if present.
    If no fence, returns the original text.
    """
    m = re.search(r"```csv\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else text.strip()

def read_csv_strict(csv_text: str):
    # RFC4180-ish parsing via Python's csv—handles quotes and commas
    data = list(csv.reader(io.StringIO(csv_text)))
    return data

def validate(data: list[list[str]]) -> list[str]:
    errs = []
    if not data:
        return ["empty CSV"]
    header = data[0]
    rows = data[1:]

    if header != HEADER:
        errs.append(f"header mismatch: expected {HEADER}, got {header}")

    if len(rows) != ROW_COUNT:
        errs.append(f"row count mismatch: expected {ROW_COUNT}, got {len(rows)}")

    # shape check
    for i, row in enumerate(rows, start=2):  # 1-based header, so rows start at line 2
        if len(row) != 2:
            errs.append(f"line {i}: expected 2 columns, got {len(row)} ({row})")

    # alphabetical check on country
    countries = [r[0] for r in rows if len(r) == 2]
    if countries != sorted(countries, key=lambda s: s):
        errs.append("rows not sorted A→Z by country")

    # minimal UTF-8 sanity (this will raise if invalid when coming from bytes; here assume str OK)
    return errs

def parse_and_validate(text: str):
    inner = extract_code_fence(text)
    data = read_csv_strict(inner)
    errors = validate(data)
    return errors, data

if __name__ == "__main__":
    raw = sys.stdin.read()
    errs, data = parse_and_validate(raw)
    if errs:
        print("INVALID\n" + "\n".join(errs))
        sys.exit(1)
    else:
        print("VALID")
        # Optional: print normalized CSV to stdout
        out = io.StringIO()
        w = csv.writer(out, lineterminator="\n")
        for row in data:
            w.writerow(row)
        print(out.getvalue())
