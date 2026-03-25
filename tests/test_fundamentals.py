from pathlib import Path

from src.pandas_wrangling_demo.fundamentals import (
    build_dataframe_examples,
    build_numpy_examples,
    build_series_examples,
    run_fundamentals_demo,
)



def test_series_examples_match_slide_values() -> None:
    result = build_series_examples()

    assert result["value_a"] == -5
    assert result["positive"].to_dict() == {"d": 4, "b": 7, "c": 3}
    assert result["index"] == ["d", "b", "a", "c"]


def test_numpy_examples_create_series_and_mean() -> None:
    result = build_numpy_examples()

    assert result["array"] == [4, 7, -5, 3]
    assert abs(result["mean"] - 2.25) < 1e-9
    assert result["positive_values"].to_dict() == {"d": 4, "b": 7, "c": 3}



def test_dataframe_examples_match_slide_structure() -> None:
    result = build_dataframe_examples()
    dataframe = result["dataframe"]

    assert list(dataframe.columns) == ["estado", "ano", "pop"]
    assert tuple(result["shape"]) == (4, 3)
    assert result["first_row_label"]["estado"] == "SP"
    assert result["second_row_position"]["ano"] == 2021



def test_run_fundamentals_demo_creates_outputs(tmp_path: Path) -> None:
    result = run_fundamentals_demo(tmp_path)
    output_dir = tmp_path / "outputs" / "fundamentals"

    assert output_dir.exists()
    assert "numpy" in result["sections"]
    assert "series" in result["sections"]
    assert "time_series" in result["sections"]

    expected_files = [
        "fundamentals_report.txt",
        "dataframe_example.csv",
        "cleaning_example.csv",
        "groupby_example.csv",
        "merge_example.csv",
        "time_series_monthly.csv",
        "population_by_state.png",
        "monthly_sales.png",
        "students.csv",
        "students.json",
        "students.db",
    ]

    for filename in expected_files:
        assert (output_dir / filename).exists(), filename
