import csv, io, sys, re


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


def validate(data: list[list[str]], headers: list[str], sort_by: str, row_count: int) -> list[str]:
    errs = []
    if not data:
        return ["empty CSV"]
    header = data[0]
    rows = data[1:]

    # 1st check
    if header != headers:
        errs.append(f"header mismatch: expected {headers}, got {header}")

    # 2nd check
    if len(rows) != row_count:
        errs.append(f"row count mismatch: expected {row_count}, got {len(rows)}")

    expected_cols = len(header)
    for i, row in enumerate(rows, start=2):  # data rows start at line 2
        if len(row) != expected_cols:
            errs.append(
                f"line {i}: expected {expected_cols} columns, got {len(row)} ({row})"
            )

    # alphabetical check on sort_by column (generic)
    sort_by_index = headers.index(sort_by)
    sort_by_col = [r[sort_by_index] for r in rows if len(r) == expected_cols]
    if sort_by_col != sorted(sort_by_col, key=lambda s: s):
        errs.append("rows not sorted A→Z by first column")
    print(errs)
    return errs


def parse_and_validate(text: str, headers: list[str], sort_by: str, row_count: int):
    inner = extract_code_fence(text)
    data = read_csv_strict(inner)
    errors = validate(data, headers, sort_by, row_count)
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
