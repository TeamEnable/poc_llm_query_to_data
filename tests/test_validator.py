import pytest
import validator_csv as validator


def make_data(header, rows):
    return [header, *rows]


@pytest.fixture
def defaults(monkeypatch):
    monkeypatch.setattr(validator, "HEADER", ["c1", "c2", "c3"], raising=False)
    monkeypatch.setattr(validator, "ROW_COUNT", 3, raising=False)


def test_empty_input_returns_error():
    assert validator.validate([]) == ["empty CSV"]


def test_header_mismatch(defaults):
    data = make_data(
        ["C1", "C2", "C3"], [["a", "x", "u"], ["b", "y", "v"], ["c", "z", "w"]]
    )
    errs = validator.validate(data)
    assert any("header mismatch" in e for e in errs)


def test_row_count_mismatch(defaults):
    data = make_data(
        ["c1", "c2", "c3"], [["a", "x", "u"], ["b", "y", "v"]]
    )  # only 2 rows
    errs = validator.validate(data)
    assert any("row count mismatch" in e for e in errs)


def test_shape_error_reports_line_number(defaults):
    # Wrong shape in second data row: 4 columns instead of 3
    data = make_data(
        ["c1", "c2", "c3"],
        [
            ["a", "x", "u"],
            ["b", "y", "v", "extra"],  # line 3 (header is line 1)
            ["c", "z", "w"],
        ],
    )
    errs = validator.validate(data)
    assert any("expected 3 columns" in e for e in errs)
    assert any("line 3:" in e for e in errs)


def test_unsorted_first_column_error(defaults):
    data = make_data(
        ["c1", "c2", "c3"],
        [
            ["c", "x", "u"],
            ["a", "y", "v"],
            ["b", "z", "w"],
        ],
    )
    errs = validator.validate(data)
    assert "rows not sorted A→Z by first column" in errs


def test_happy_path_no_errors(defaults):
    data = make_data(
        ["c1", "c2", "c3"],
        [
            ["a", "x", "u"],
            ["b", "y", "v"],
            ["c", "z", "w"],
        ],
    )
    assert validator.validate(data) == []


# Specifically test a 2-column case
def test_two_column_dataset(monkeypatch):
    monkeypatch.setattr(validator, "HEADER", ["left", "right"], raising=False)
    monkeypatch.setattr(validator, "ROW_COUNT", 2, raising=False)
    data = make_data(["left", "right"], [["a", "1"], ["b", "2"]])
    assert validator.validate(data) == []
