# pyright: reportMissingImports=false, reportMissingModuleSource=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false
from __future__ import annotations

import io
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from .data_setup import bootstrap_demo_environment


pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 120)



def run_pipeline(base_dir: Path) -> dict[str, object]:
    paths = bootstrap_demo_environment(base_dir)
    sales_frames = [
        pd.read_csv(paths["sales_jan_path"]),
        pd.read_csv(paths["sales_feb_path"]),
    ]
    customers_df = pd.read_json(paths["customers_path"])

    with sqlite3.connect(paths["database_path"]) as connection:
        returns_df = pd.read_sql_query("SELECT order_id, returned_qty, return_reason FROM returns", connection)

    raw_sales_df = pd.concat(sales_frames, ignore_index=True)
    _write_inspection_report(raw_sales_df, customers_df, returns_df, paths["output_dir"])

    cleaned_sales_df = _clean_sales(raw_sales_df)
    enriched_sales_df = _enrich_sales(cleaned_sales_df, customers_df, returns_df)

    sales_by_category = (
        enriched_sales_df.groupby(["category", "region"], as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_quantity=("quantity", "sum"),
            total_revenue=("realized_revenue", "sum"),
        )
        .sort_values(["category", "total_revenue"], ascending=[True, False])
    )

    sales_pivot = pd.pivot_table(
        enriched_sales_df,
        index="region",
        columns="segment",
        values="realized_revenue",
        aggfunc="sum",
        fill_value=0,
    )

    _write_outputs(
        cleaned_sales_df=cleaned_sales_df,
        enriched_sales_df=enriched_sales_df,
        sales_by_category=sales_by_category,
        sales_pivot=sales_pivot,
        output_dir=paths["output_dir"],
    )
    _plot_monthly_revenue(enriched_sales_df, paths["output_dir"])

    return {
        "row_count": int(len(enriched_sales_df)),
        "total_revenue": float(enriched_sales_df["realized_revenue"].sum()),
        "output_dir": str(paths["output_dir"]),
    }



def _clean_sales(sales_df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = sales_df.copy()
    cleaned_df = cleaned_df.drop_duplicates()
    cleaned_df["order_date"] = pd.to_datetime(cleaned_df["order_date"], errors="coerce")

    numeric_columns = ["customer_id", "quantity", "unit_price", "discount"]
    for column in numeric_columns:
        cleaned_df[column] = pd.to_numeric(cleaned_df[column], errors="coerce")

    cleaned_df["discount"] = cleaned_df["discount"].fillna(0).clip(lower=0, upper=0.30)
    cleaned_df["region"] = cleaned_df["region"].fillna("Nao Informado")
    cleaned_df["product"] = cleaned_df["product"].str.strip()
    cleaned_df["category"] = cleaned_df["category"].str.strip()

    cleaned_df = cleaned_df.dropna(subset=["order_id", "customer_id", "order_date", "quantity", "unit_price"])
    cleaned_df["customer_id"] = cleaned_df["customer_id"].astype(int)
    cleaned_df["quantity"] = cleaned_df["quantity"].astype(int)
    cleaned_df["order_id"] = cleaned_df["order_id"].astype(int)
    cleaned_df["order_month"] = cleaned_df["order_date"].dt.to_period("M").astype(str)
    cleaned_df["gross_revenue"] = cleaned_df["quantity"] * cleaned_df["unit_price"]
    cleaned_df["net_revenue"] = cleaned_df["gross_revenue"] * (1 - cleaned_df["discount"])
    return cleaned_df.sort_values(["order_date", "order_id"]).reset_index(drop=True)



def _enrich_sales(
    cleaned_sales_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    returns_df: pd.DataFrame,
) -> pd.DataFrame:
    enriched_df = cleaned_sales_df.merge(customers_df, on="customer_id", how="left")
    enriched_df = enriched_df.merge(returns_df, on="order_id", how="left")
    enriched_df["returned_qty"] = enriched_df["returned_qty"].fillna(0).astype(int)
    enriched_df["return_reason"] = enriched_df["return_reason"].fillna("Sem devolucao")
    enriched_df["returned_value"] = (
        enriched_df["returned_qty"] * enriched_df["unit_price"] * (1 - enriched_df["discount"])
    )
    enriched_df["realized_revenue"] = enriched_df["net_revenue"] - enriched_df["returned_value"]
    enriched_df["average_ticket"] = enriched_df["realized_revenue"] / enriched_df["quantity"]
    return enriched_df



def _write_inspection_report(
    sales_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    report_buffer = io.StringIO()

    def append_section(title: str, dataframe: pd.DataFrame) -> None:
        report_buffer.write(f"=== {title} ===\n")
        report_buffer.write("HEAD\n")
        report_buffer.write(dataframe.head().to_string())
        report_buffer.write("\n\nINFO\n")
        info_buffer = io.StringIO()
        dataframe.info(buf=info_buffer)
        report_buffer.write(info_buffer.getvalue())
        report_buffer.write("\nDESCRIBE\n")
        report_buffer.write(dataframe.describe(include="all").to_string())
        report_buffer.write("\n\n")

    append_section("VENDAS CONSOLIDADAS", sales_df)
    append_section("CLIENTES", customers_df)
    append_section("DEVOLUCOES", returns_df)

    (output_dir / "inspection_summary.txt").write_text(report_buffer.getvalue(), encoding="utf-8")



def _write_outputs(
    cleaned_sales_df: pd.DataFrame,
    enriched_sales_df: pd.DataFrame,
    sales_by_category: pd.DataFrame,
    sales_pivot: pd.DataFrame,
    output_dir: Path,
) -> None:
    cleaned_sales_df.to_csv(output_dir / "cleaned_sales.csv", index=False)
    enriched_sales_df.to_csv(output_dir / "enriched_sales.csv", index=False)
    sales_by_category.to_csv(output_dir / "sales_by_category.csv", index=False)
    sales_pivot.to_csv(output_dir / "sales_pivot_region_segment.csv")



def _plot_monthly_revenue(enriched_sales_df: pd.DataFrame, output_dir: Path) -> None:
    revenue_by_month = (
        enriched_sales_df.groupby("order_month", as_index=False)
        .agg(realized_revenue=("realized_revenue", "sum"))
        .sort_values("order_month")
    )

    plt.figure(figsize=(8, 4))
    plt.plot(revenue_by_month["order_month"], revenue_by_month["realized_revenue"], marker="o", linewidth=2)
    plt.title("Receita realizada por mês")
    plt.xlabel("Mês")
    plt.ylabel("Receita")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / "monthly_revenue.png", dpi=150)
    plt.close()
