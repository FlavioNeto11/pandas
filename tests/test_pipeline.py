from pathlib import Path

import pandas as pd

from src.pandas_wrangling_demo.pipeline import run_pipeline



def test_run_pipeline_creates_expected_outputs(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path)
    output_dir = tmp_path / "outputs"

    assert result["row_count"] == 10
    assert result["total_revenue"] > 0
    assert output_dir.exists()

    expected_files = [
        "inspection_summary.txt",
        "cleaned_sales.csv",
        "enriched_sales.csv",
        "sales_by_category.csv",
        "sales_pivot_region_segment.csv",
        "monthly_revenue.png",
    ]

    for filename in expected_files:
        assert (output_dir / filename).exists()

    enriched_sales = pd.read_csv(output_dir / "enriched_sales.csv")
    assert "realized_revenue" in enriched_sales.columns
    assert enriched_sales["order_id"].nunique() == 10


def test_run_pipeline_stream_mode_matches_default(tmp_path: Path) -> None:
    default_result = run_pipeline(tmp_path)
    stream_result = run_pipeline(tmp_path, stream=True, chunk_size=2)

    assert stream_result["row_count"] == default_result["row_count"]
    assert abs(stream_result["total_revenue"] - default_result["total_revenue"]) < 1e-9

    output_dir = tmp_path / "outputs"
    enriched_sales = pd.read_csv(output_dir / "enriched_sales.csv")
    assert enriched_sales["order_id"].nunique() == 10
    assert (tmp_path / "data" / "raw" / "customers.jsonl").exists()
