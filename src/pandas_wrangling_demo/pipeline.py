# pyright: reportMissingImports=false, reportMissingModuleSource=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false
from __future__ import annotations

import io
import sqlite3
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path

import matplotlib
import pandas as pd

from .data_setup import bootstrap_demo_environment


matplotlib.use("Agg")
import matplotlib.pyplot as plt


pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 120)



def run_pipeline(
    base_dir: Path,
    stream: bool = False,
    chunk_size: int = 1000,
    stream_workers: int = 1,
    in_flight_tasks: int = 0,
) -> dict[str, object]:
    paths = bootstrap_demo_environment(base_dir)
    if stream:
        return _run_pipeline_streaming(
            paths,
            chunk_size=chunk_size,
            stream_workers=stream_workers,
            in_flight_tasks=in_flight_tasks,
        )

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


def _run_pipeline_streaming(
    paths: dict[str, Path],
    chunk_size: int,
    stream_workers: int,
    in_flight_tasks: int,
) -> dict[str, object]:
    if stream_workers <= 1:
        return _run_pipeline_streaming_sequential(paths, chunk_size=chunk_size)
    return _run_pipeline_streaming_threaded(
        paths,
        chunk_size=chunk_size,
        stream_workers=stream_workers,
        in_flight_tasks=in_flight_tasks,
    )


def _run_pipeline_streaming_sequential(paths: dict[str, Path], chunk_size: int) -> dict[str, object]:
    customers_df = _read_customers_streaming(paths, chunk_size)
    with sqlite3.connect(paths["database_path"]) as connection:
        returns_df = pd.read_sql_query("SELECT order_id, returned_qty, return_reason FROM returns", connection)

    output_dir = paths["output_dir"]
    cleaned_output = output_dir / "cleaned_sales.csv"
    enriched_output = output_dir / "enriched_sales.csv"
    _initialize_streaming_outputs(cleaned_output, enriched_output)

    row_count = 0
    total_revenue = 0.0
    first_chunk = True
    inspection_sample: pd.DataFrame | None = None

    aggregate_db_path = output_dir / "stream_aggregate_temp.db"
    _safe_unlink(aggregate_db_path)

    with sqlite3.connect(aggregate_db_path) as aggregate_connection:
        _initialize_distinct_orders_store(aggregate_connection)
        _initialize_stream_aggregate_store(aggregate_connection)

        for sales_chunk in _iter_sales_chunks(paths, chunk_size):
            if inspection_sample is None:
                inspection_sample = sales_chunk.head(5)

            cleaned_chunk = _clean_sales(sales_chunk)
            if cleaned_chunk.empty:
                continue

            enriched_chunk = _enrich_sales(cleaned_chunk, customers_df, returns_df)

            cleaned_chunk.to_csv(cleaned_output, mode="a", index=False, header=first_chunk)
            enriched_chunk.to_csv(enriched_output, mode="a", index=False, header=first_chunk)
            first_chunk = False

            row_count += int(len(enriched_chunk))
            total_revenue += float(enriched_chunk["realized_revenue"].sum())
            _accumulate_streaming_chunk_to_sqlite(aggregate_connection, enriched_chunk)
            _store_distinct_orders(aggregate_connection, enriched_chunk)

        sales_by_category = _read_sales_by_category_from_sqlite(aggregate_connection)
        pivot_source = _read_pivot_source_from_sqlite(aggregate_connection)
        monthly_df = _read_monthly_from_sqlite(aggregate_connection)

    _safe_unlink(aggregate_db_path)

    _write_streaming_inspection_report(inspection_sample, customers_df, returns_df, output_dir)

    if not sales_by_category.empty:
        sales_by_category = sales_by_category.sort_values(
            ["category", "total_revenue"], ascending=[True, False]
        )
    sales_by_category.to_csv(output_dir / "sales_by_category.csv", index=False)

    if pivot_source.empty:
        sales_pivot = pd.DataFrame()
    else:
        sales_pivot = pd.pivot_table(
            pivot_source,
            index="region",
            columns="segment",
            values="realized_revenue",
            aggfunc="sum",
            fill_value=0,
        )
    sales_pivot.to_csv(output_dir / "sales_pivot_region_segment.csv")

    if not monthly_df.empty:
        monthly_df = monthly_df.sort_values("order_month")
    _plot_monthly_revenue_from_monthly(monthly_df, output_dir)

    return {
        "row_count": int(row_count),
        "total_revenue": float(total_revenue),
        "output_dir": str(output_dir),
    }


def _run_pipeline_streaming_threaded(
    paths: dict[str, Path],
    chunk_size: int,
    stream_workers: int,
    in_flight_tasks: int,
) -> dict[str, object]:
    customers_df = _read_customers_streaming(paths, chunk_size)
    with sqlite3.connect(paths["database_path"]) as connection:
        returns_df = pd.read_sql_query("SELECT order_id, returned_qty, return_reason FROM returns", connection)

    output_dir = paths["output_dir"]
    cleaned_output = output_dir / "cleaned_sales.csv"
    enriched_output = output_dir / "enriched_sales.csv"
    _initialize_streaming_outputs(cleaned_output, enriched_output)

    row_count = 0
    total_revenue = 0.0
    first_chunk = True
    inspection_sample: pd.DataFrame | None = None

    aggregate_db_path = output_dir / "stream_aggregate_temp.db"
    _safe_unlink(aggregate_db_path)

    pending_limit = in_flight_tasks if in_flight_tasks > 0 else max(2, stream_workers * 2)

    with sqlite3.connect(aggregate_db_path) as aggregate_connection:
        _initialize_distinct_orders_store(aggregate_connection)
        _initialize_stream_aggregate_store(aggregate_connection)

        chunk_iterator = iter(enumerate(_iter_sales_chunks(paths, chunk_size)))
        running_futures: dict[Future[tuple[pd.DataFrame, pd.DataFrame]], int] = {}
        completed_chunks: dict[int, tuple[pd.DataFrame, pd.DataFrame]] = {}
        next_chunk_to_write = 0

        with ThreadPoolExecutor(max_workers=stream_workers) as executor:
            while len(running_futures) < pending_limit:
                try:
                    chunk_index, sales_chunk = next(chunk_iterator)
                except StopIteration:
                    break

                if inspection_sample is None:
                    inspection_sample = sales_chunk.head(5)

                future = executor.submit(_prepare_streaming_chunk, sales_chunk, customers_df, returns_df)
                running_futures[future] = chunk_index

            while running_futures:
                done_futures, _ = wait(set(running_futures.keys()), return_when=FIRST_COMPLETED)

                for done_future in done_futures:
                    chunk_index = running_futures.pop(done_future)
                    completed_chunks[chunk_index] = done_future.result()

                while next_chunk_to_write in completed_chunks:
                    cleaned_chunk, enriched_chunk = completed_chunks.pop(next_chunk_to_write)
                    next_chunk_to_write += 1

                    if cleaned_chunk.empty:
                        continue

                    cleaned_chunk.to_csv(cleaned_output, mode="a", index=False, header=first_chunk)
                    enriched_chunk.to_csv(enriched_output, mode="a", index=False, header=first_chunk)
                    first_chunk = False

                    row_count += int(len(enriched_chunk))
                    total_revenue += float(enriched_chunk["realized_revenue"].sum())
                    _accumulate_streaming_chunk_to_sqlite(aggregate_connection, enriched_chunk)
                    _store_distinct_orders(aggregate_connection, enriched_chunk)

                while len(running_futures) < pending_limit:
                    try:
                        chunk_index, sales_chunk = next(chunk_iterator)
                    except StopIteration:
                        break

                    if inspection_sample is None:
                        inspection_sample = sales_chunk.head(5)

                    future = executor.submit(_prepare_streaming_chunk, sales_chunk, customers_df, returns_df)
                    running_futures[future] = chunk_index

        sales_by_category = _read_sales_by_category_from_sqlite(aggregate_connection)
        pivot_source = _read_pivot_source_from_sqlite(aggregate_connection)
        monthly_df = _read_monthly_from_sqlite(aggregate_connection)

    _safe_unlink(aggregate_db_path)

    if inspection_sample is None:
        for chunk in _iter_sales_chunks(paths, chunk_size):
            inspection_sample = chunk.head(5)
            break

    _write_streaming_inspection_report(inspection_sample, customers_df, returns_df, output_dir)

    if not sales_by_category.empty:
        sales_by_category = sales_by_category.sort_values(
            ["category", "total_revenue"], ascending=[True, False]
        )
    sales_by_category.to_csv(output_dir / "sales_by_category.csv", index=False)

    if pivot_source.empty:
        sales_pivot = pd.DataFrame()
    else:
        sales_pivot = pd.pivot_table(
            pivot_source,
            index="region",
            columns="segment",
            values="realized_revenue",
            aggfunc="sum",
            fill_value=0,
        )
    sales_pivot.to_csv(output_dir / "sales_pivot_region_segment.csv")

    if not monthly_df.empty:
        monthly_df = monthly_df.sort_values("order_month")
    _plot_monthly_revenue_from_monthly(monthly_df, output_dir)

    return {
        "row_count": int(row_count),
        "total_revenue": float(total_revenue),
        "output_dir": str(output_dir),
    }


def _prepare_streaming_chunk(
    sales_chunk: pd.DataFrame,
    customers_df: pd.DataFrame,
    returns_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cleaned_chunk = _clean_sales(sales_chunk)
    if cleaned_chunk.empty:
        return cleaned_chunk, cleaned_chunk
    enriched_chunk = _enrich_sales(cleaned_chunk, customers_df, returns_df)
    return cleaned_chunk, enriched_chunk


def _iter_sales_chunks(paths: dict[str, Path], chunk_size: int):
    sales_files = [paths["sales_jan_path"], paths["sales_feb_path"]]
    for sales_file in sales_files:
        for chunk in pd.read_csv(sales_file, chunksize=chunk_size):
            yield chunk


def _read_customers_streaming(paths: dict[str, Path], chunk_size: int) -> pd.DataFrame:
    customers_jsonl_path = paths["customers_jsonl_path"]
    customer_chunks: list[pd.DataFrame] = []
    stream_chunk_size = max(1, int(chunk_size))

    for chunk in pd.read_json(customers_jsonl_path, lines=True, chunksize=stream_chunk_size):
        customer_chunks.append(chunk)

    if not customer_chunks:
        return pd.DataFrame(columns=["customer_id", "customer_name", "segment", "city"])

    return pd.concat(customer_chunks, ignore_index=True)


def _initialize_streaming_outputs(cleaned_output: Path, enriched_output: Path) -> None:
    _safe_unlink(cleaned_output)
    _safe_unlink(enriched_output)


def _safe_unlink(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except PermissionError:
        return


def _initialize_stream_aggregate_store(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS totals_by_category_region (
            category TEXT NOT NULL,
            region TEXT NOT NULL,
            total_quantity REAL NOT NULL,
            total_revenue REAL NOT NULL,
            PRIMARY KEY (category, region)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pivot_totals (
            region TEXT NOT NULL,
            segment TEXT NOT NULL,
            total_revenue REAL NOT NULL,
            PRIMARY KEY (region, segment)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_totals (
            order_month TEXT NOT NULL PRIMARY KEY,
            total_revenue REAL NOT NULL
        )
        """
    )
    connection.commit()


def _accumulate_streaming_chunk_to_sqlite(
    connection: sqlite3.Connection,
    enriched_chunk: pd.DataFrame,
) -> None:
    group_chunk = (
        enriched_chunk.groupby(["category", "region"], as_index=False)
        .agg(total_quantity=("quantity", "sum"), total_revenue=("realized_revenue", "sum"))
    )
    group_rows = [
        (str(row.category), str(row.region), float(row.total_quantity), float(row.total_revenue))
        for row in group_chunk.itertuples(index=False)
    ]

    pivot_chunk = (
        enriched_chunk.groupby(["region", "segment"], as_index=False)
        .agg(total_revenue=("realized_revenue", "sum"))
    )
    pivot_rows = [
        (str(row.region), str(row.segment), float(row.total_revenue))
        for row in pivot_chunk.itertuples(index=False)
    ]

    monthly_chunk = (
        enriched_chunk.groupby("order_month", as_index=False)
        .agg(total_revenue=("realized_revenue", "sum"))
    )
    monthly_rows = [
        (str(row.order_month), float(row.total_revenue))
        for row in monthly_chunk.itertuples(index=False)
    ]

    cursor = connection.cursor()
    cursor.executemany(
        """
        INSERT INTO totals_by_category_region (category, region, total_quantity, total_revenue)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(category, region) DO UPDATE SET
            total_quantity = totals_by_category_region.total_quantity + excluded.total_quantity,
            total_revenue = totals_by_category_region.total_revenue + excluded.total_revenue
        """,
        group_rows,
    )
    cursor.executemany(
        """
        INSERT INTO pivot_totals (region, segment, total_revenue)
        VALUES (?, ?, ?)
        ON CONFLICT(region, segment) DO UPDATE SET
            total_revenue = pivot_totals.total_revenue + excluded.total_revenue
        """,
        pivot_rows,
    )
    cursor.executemany(
        """
        INSERT INTO monthly_totals (order_month, total_revenue)
        VALUES (?, ?)
        ON CONFLICT(order_month) DO UPDATE SET
            total_revenue = monthly_totals.total_revenue + excluded.total_revenue
        """,
        monthly_rows,
    )
    connection.commit()


def _initialize_distinct_orders_store(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS distinct_orders (
            category TEXT NOT NULL,
            region TEXT NOT NULL,
            order_id INTEGER NOT NULL,
            PRIMARY KEY (category, region, order_id)
        )
        """
    )
    connection.commit()


def _store_distinct_orders(connection: sqlite3.Connection, enriched_chunk: pd.DataFrame) -> None:
    distinct_rows = (
        enriched_chunk[["category", "region", "order_id"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    cursor = connection.cursor()
    cursor.executemany(
        "INSERT OR IGNORE INTO distinct_orders (category, region, order_id) VALUES (?, ?, ?)",
        [(str(category), str(region), int(order_id)) for category, region, order_id in distinct_rows],
    )
    connection.commit()


def _read_total_orders_by_group(connection: sqlite3.Connection) -> dict[tuple[str, str], int]:
    cursor = connection.cursor()
    rows = cursor.execute(
        """
        SELECT category, region, COUNT(*) AS total_orders
        FROM distinct_orders
        GROUP BY category, region
        """
    ).fetchall()
    return {(str(category), str(region)): int(total_orders) for category, region, total_orders in rows}


def _read_sales_by_category_from_sqlite(connection: sqlite3.Connection) -> pd.DataFrame:
    query = """
        SELECT
            t.category,
            t.region,
            COALESCE(d.total_orders, 0) AS total_orders,
            t.total_quantity,
            t.total_revenue
        FROM totals_by_category_region t
        LEFT JOIN (
            SELECT category, region, COUNT(*) AS total_orders
            FROM distinct_orders
            GROUP BY category, region
        ) d
            ON d.category = t.category
           AND d.region = t.region
    """
    return pd.read_sql_query(query, connection)


def _read_pivot_source_from_sqlite(connection: sqlite3.Connection) -> pd.DataFrame:
    query = """
        SELECT region, segment, total_revenue AS realized_revenue
        FROM pivot_totals
    """
    return pd.read_sql_query(query, connection)


def _read_monthly_from_sqlite(connection: sqlite3.Connection) -> pd.DataFrame:
    query = """
        SELECT order_month, total_revenue AS realized_revenue
        FROM monthly_totals
    """
    return pd.read_sql_query(query, connection)



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


def _write_streaming_inspection_report(
    sample_sales_df: pd.DataFrame | None,
    customers_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    report_buffer = io.StringIO()
    report_buffer.write("=== VENDAS CONSOLIDADAS (MODO STREAMING) ===\n")

    if sample_sales_df is None or sample_sales_df.empty:
        report_buffer.write("Nenhuma linha de vendas encontrada.\n\n")
    else:
        report_buffer.write("HEAD (amostra do primeiro chunk)\n")
        report_buffer.write(sample_sales_df.to_string())
        report_buffer.write("\n\nINFO\n")
        info_buffer = io.StringIO()
        sample_sales_df.info(buf=info_buffer)
        report_buffer.write(info_buffer.getvalue())
        report_buffer.write("\nDESCRIBE\n")
        report_buffer.write(sample_sales_df.describe(include="all").to_string())
        report_buffer.write("\n\n")

    report_buffer.write("=== CLIENTES ===\n")
    report_buffer.write(customers_df.head().to_string())
    report_buffer.write("\n\n=== DEVOLUCOES ===\n")
    report_buffer.write(returns_df.head().to_string())
    report_buffer.write("\n")

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


def _plot_monthly_revenue_from_monthly(monthly_df: pd.DataFrame, output_dir: Path) -> None:
    plt.figure(figsize=(8, 4))
    if not monthly_df.empty:
        plt.plot(monthly_df["order_month"], monthly_df["realized_revenue"], marker="o", linewidth=2)
    plt.title("Receita realizada por mês")
    plt.xlabel("Mês")
    plt.ylabel("Receita")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / "monthly_revenue.png", dpi=150)
    plt.close()
