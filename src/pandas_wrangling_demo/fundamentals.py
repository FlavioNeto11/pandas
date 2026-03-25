from __future__ import annotations

import io
import sqlite3
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# pyright: reportMissingImports=false, reportMissingModuleSource=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false


def run_fundamentals_demo(base_dir: Path) -> dict[str, Any]:
    output_dir = base_dir / "outputs" / "fundamentals"
    output_dir.mkdir(parents=True, exist_ok=True)

    sections: dict[str, Any] = {
        "numpy": build_numpy_examples(),
        "series": build_series_examples(),
        "dataframe": build_dataframe_examples(),
        "io": build_io_examples(output_dir),
        "cleaning": build_cleaning_examples(),
        "groupby": build_groupby_examples(),
        "merge_reshape": build_merge_reshape_examples(),
        "time_series": build_time_series_examples(),
    }

    _write_text_report(sections, output_dir)
    _write_structured_outputs(sections, output_dir)
    _create_plots(sections, output_dir)

    return {
        "sections": list(sections.keys()),
        "output_dir": str(output_dir),
    }



def build_numpy_examples() -> dict[str, Any]:
    array = np.array([4, 7, -5, 3])
    series_from_array = pd.Series(array, index=["d", "b", "a", "c"])

    return {
        "array": array.tolist(),
        "series_from_array": series_from_array,
        "mean": float(array.mean()),
        "positive_values": series_from_array[series_from_array > 0],
    }



def build_series_examples() -> dict[str, Any]:
    default_series = pd.Series([4, 7, -5, 3])
    labeled_series = pd.Series([4, 7, -5, 3], index=["d", "b", "a", "c"])
    positive_values = labeled_series[labeled_series > 0]

    return {
        "default": default_series,
        "labeled": labeled_series,
        "value_a": labeled_series["a"],
        "positive": positive_values,
        "index": labeled_series.index.tolist(),
        "values": labeled_series.values.tolist(),
    }



def build_dataframe_examples() -> dict[str, Any]:
    dados = {
        "estado": ["SP", "SP", "RJ", "MG"],
        "ano": [2020, 2021, 2021, 2020],
        "pop": [44.0, 44.5, 17.4, 21.0],
    }
    dataframe = pd.DataFrame(dados)

    return {
        "dataframe": dataframe,
        "estado_column": dataframe["estado"],
        "first_row_label": dataframe.loc[0],
        "second_row_position": dataframe.iloc[1],
        "columns": dataframe.columns.tolist(),
        "shape": dataframe.shape,
    }



def build_io_examples(output_dir: Path) -> dict[str, Any]:
    source_df = pd.DataFrame(
        {
            "aluno": ["Ana", "Bruno", "Carla"],
            "nota": [9.0, 7.5, 8.8],
            "turma": ["A", "A", "B"],
        }
    )
    csv_path = output_dir / "students.csv"
    json_path = output_dir / "students.json"
    db_path = output_dir / "students.db"

    source_df.to_csv(csv_path, index=False)
    source_df.to_json(json_path, orient="records", indent=2, force_ascii=False)

    with sqlite3.connect(db_path) as connection:
        source_df.to_sql("students", connection, if_exists="replace", index=False)
        sql_df = pd.read_sql_query("SELECT aluno, nota, turma FROM students", connection)

    csv_df = pd.read_csv(csv_path)
    json_df = pd.read_json(json_path)

    return {
        "source": source_df,
        "csv": csv_df,
        "json": json_df,
        "sql": sql_df,
        "files": [str(csv_path), str(json_path), str(db_path)],
    }



def build_cleaning_examples() -> dict[str, Any]:
    dirty_df = pd.DataFrame(
        {
            "produto": ["Notebook", "Mouse", "Mouse", "Teclado ", None],
            "preco": [3500, "80", "80", 150, 200],
            "desconto": [0.05, None, None, 0.0, 0.1],
            "categoria": ["Eletronicos", "Eletronicos", "Eletronicos", "Eletronicos", "Acessorios"],
        }
    )

    cleaned_df = dirty_df.copy()
    cleaned_df["produto"] = cleaned_df["produto"].fillna("Desconhecido").str.strip()
    cleaned_df["preco"] = pd.to_numeric(cleaned_df["preco"], errors="coerce")
    cleaned_df["desconto"] = cleaned_df["desconto"].fillna(0)
    cleaned_df = cleaned_df.drop_duplicates()
    cleaned_df["preco_final"] = cleaned_df["preco"] * (1 - cleaned_df["desconto"])

    return {
        "before": dirty_df,
        "after": cleaned_df,
        "missing_before": dirty_df.isna().sum().to_dict(),
        "missing_after": cleaned_df.isna().sum().to_dict(),
    }



def build_groupby_examples() -> dict[str, Any]:
    sales_df = pd.DataFrame(
        {
            "categoria": ["Eletronicos", "Eletronicos", "Moveis", "Moveis", "Casa"],
            "regiao": ["Sudeste", "Sul", "Sudeste", "Nordeste", "Sul"],
            "valor": [3500, 160, 900, 1760, 360],
            "quantidade": [1, 2, 1, 2, 3],
        }
    )

    grouped = (
        sales_df.groupby(["categoria", "regiao"], as_index=False)
        .agg(total_valor=("valor", "sum"), total_quantidade=("quantidade", "sum"))
        .sort_values(["categoria", "total_valor"], ascending=[True, False])
    )

    pivot = pd.pivot_table(
        sales_df,
        index="regiao",
        columns="categoria",
        values="valor",
        aggfunc="sum",
        fill_value=0,
    )

    return {
        "sales": sales_df,
        "grouped": grouped,
        "pivot": pivot,
    }



def build_merge_reshape_examples() -> dict[str, Any]:
    population_df = pd.DataFrame(
        {
            "estado": ["SP", "RJ", "MG"],
            "pop_milhoes": [44.5, 17.4, 21.0],
        }
    )
    gdp_df = pd.DataFrame(
        {
            "estado": ["SP", "RJ", "MG"],
            "pib_bilhoes": [3200, 950, 720],
        }
    )
    wide_sales_df = pd.DataFrame(
        {
            "mes": ["2026-01", "2026-02"],
            "sudeste": [7000, 3550],
            "sul": [160, 1760],
        }
    )

    merged = population_df.merge(gdp_df, on="estado", how="inner")
    melted = wide_sales_df.melt(id_vars="mes", var_name="regiao", value_name="faturamento")
    reshaped = melted.pivot(index="mes", columns="regiao", values="faturamento")

    return {
        "merged": merged,
        "melted": melted,
        "reshaped": reshaped,
    }



def build_time_series_examples() -> dict[str, Any]:
    time_df = pd.DataFrame(
        {
            "data": pd.date_range("2026-01-01", periods=8, freq="15D"),
            "vendas": [120, 150, 140, 180, 220, 210, 240, 260],
        }
    )
    time_df["media_movel_2"] = time_df["vendas"].rolling(window=2).mean()

    monthly = (
        time_df.set_index("data")
        .resample("ME")
        .agg(vendas_total=("vendas", "sum"), media_movel_final=("media_movel_2", "last"))
        .reset_index()
    )

    return {
        "daily": time_df,
        "monthly": monthly,
    }



def _write_text_report(sections: dict[str, Any], output_dir: Path) -> None:
    report = io.StringIO()

    for section_name, payload in sections.items():
        report.write(f"=== {section_name.upper()} ===\n")
        for key, value in payload.items():
            report.write(f"-- {key} --\n")
            if isinstance(value, pd.DataFrame | pd.Series):
                report.write(f"{value.to_string()}\n\n")
            else:
                report.write(f"{value}\n\n")

    (output_dir / "fundamentals_report.txt").write_text(report.getvalue(), encoding="utf-8")



def _write_structured_outputs(sections: dict[str, Any], output_dir: Path) -> None:
    sections["dataframe"]["dataframe"].to_csv(output_dir / "dataframe_example.csv", index=False)
    sections["cleaning"]["after"].to_csv(output_dir / "cleaning_example.csv", index=False)
    sections["groupby"]["grouped"].to_csv(output_dir / "groupby_example.csv", index=False)
    sections["merge_reshape"]["merged"].to_csv(output_dir / "merge_example.csv", index=False)
    sections["time_series"]["monthly"].to_csv(output_dir / "time_series_monthly.csv", index=False)



def _create_plots(sections: dict[str, Any], output_dir: Path) -> None:
    dataframe_df = sections["dataframe"]["dataframe"]
    time_monthly_df = sections["time_series"]["monthly"]

    plt.figure(figsize=(7, 4))
    plt.bar(dataframe_df["estado"], dataframe_df["pop"], color="#2f5aa8")
    plt.title("População por estado")
    plt.xlabel("Estado")
    plt.ylabel("População (milhões)")
    plt.tight_layout()
    plt.savefig(output_dir / "population_by_state.png", dpi=150)
    plt.close()

    plt.figure(figsize=(7, 4))
    plt.plot(time_monthly_df["data"], time_monthly_df["vendas_total"], marker="o", color="#2f5aa8")
    plt.title("Vendas mensais")
    plt.xlabel("Data")
    plt.ylabel("Total de vendas")
    plt.tight_layout()
    plt.savefig(output_dir / "monthly_sales.png", dpi=150)
    plt.close()
