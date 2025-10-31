import validator_csv as validator


def make_data(header, rows):
    return [header, *rows]


def test_empty_input_returns_error():
    assert validator.validate([], [], 1) == ["empty CSV"]


def test_header_mismatch():
    data = make_data(
        ["c1", "c2", "c3"], [["a", "x", "u"], ["b", "y", "v"], ["c", "z", "w"]]
    )
    errs = validator.validate(data, headers=["c1", "c2", "c4"], row_count=3)
    assert any("header mismatch" in e for e in errs)


# def test_row_count_mismatch():
#     data = make_data(
#         ["c1", "c2", "c3"], [["a", "x", "u"], ["b", "y", "v"]]
#     )  # only 2 rows
#     errs = validator.validate(
#         data, headers=["c1", "c2", "c3"], row_count=3
#     )
#     assert any("row count mismatch" in e for e in errs)


def test_shape_error_reports_line_number():
    # Wrong shape in second data row: 4 columns instead of 3
    data = make_data(
        ["c1", "c2", "c3"],
        [
            ["a", "x", "u"],
            ["b", "y", "v", "extra"],  # line 3 (header is line 1)
            ["c", "z", "w"],
        ],
    )
    errs = validator.validate(data, headers=["c1", "c2", "c3"], row_count=3)
    assert any("expected 3 columns" in e for e in errs)
    assert any("line 3:" in e for e in errs)


def test_happy_path_no_errors():
    data = make_data(
        ["c1", "c2", "c3"],
        [
            ["a", "x", "u"],
            ["b", "y", "v"],
            ["c", "z", "w"],
        ],
    )
    assert validator.validate(data, headers=["c1", "c2", "c3"], row_count=3) == []
