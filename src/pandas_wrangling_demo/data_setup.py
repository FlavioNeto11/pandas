from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

SALES_JAN = """order_id,customer_id,product,category,region,order_date,quantity,unit_price,discount
1001,1,Notebook,Eletronicos,Sudeste,2026-01-05,1,3500,0.05
1002,2,Mouse,Eletronicos,Sul,2026-01-08,2,80,
1003,3,Cadeira,Moveis,Nordeste,2026-01-10,1,900,0.10
1004,4,Luminaria,Casa,Norte,2026-01-12,3,120,0.00
1005,5,Notebook,Eletronicos,Sudeste,2026-01-12,1,3500,0.05
1005,5,Notebook,Eletronicos,Sudeste,2026-01-12,1,3500,0.05
"""

SALES_FEB = """order_id,customer_id,product,category,region,order_date,quantity,unit_price,discount
1006,1,Teclado,Eletronicos,Sudeste,2026-02-02,1,150,0.00
1007,2,Cadeira,Moveis,Sul,2026-02-06,2,880,0.15
1008,6,Aspirador,Casa,Centro-Oeste,2026-02-09,1,650,0.07
1009,7,Mouse,Eletronicos,Nordeste,2026-02-11,3,75,0.02
1010,8,Notebook,Eletronicos,,2026-02-15,1,3400,0.04
"""

CUSTOMERS: list[dict[str, Any]] = [
    {"customer_id": 1, "customer_name": "Ana", "segment": "Corporativo", "city": "Sao Paulo"},
    {"customer_id": 2, "customer_name": "Bruno", "segment": "Varejo", "city": "Curitiba"},
    {"customer_id": 3, "customer_name": "Carla", "segment": "Corporativo", "city": "Recife"},
    {"customer_id": 4, "customer_name": "Diego", "segment": "Varejo", "city": "Manaus"},
    {"customer_id": 5, "customer_name": "Erika", "segment": "Enterprise", "city": "Campinas"},
    {"customer_id": 6, "customer_name": "Fabio", "segment": "Varejo", "city": "Goiania"},
    {"customer_id": 7, "customer_name": "Gi", "segment": "Corporativo", "city": "Salvador"},
    {"customer_id": 8, "customer_name": "Helena", "segment": "Enterprise", "city": "Belo Horizonte"},
]

RETURNS: list[tuple[int, int, str]] = [
    (1002, 1, "Defeito"),
    (1007, 1, "Arrependimento"),
    (1009, 1, "Atraso na entrega"),
]


def bootstrap_demo_environment(base_dir: Path) -> dict[str, Path]:
    data_dir = base_dir / "data"
    raw_dir = data_dir / "raw"
    output_dir = base_dir / "outputs"
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    sales_jan_path = raw_dir / "sales_jan.csv"
    sales_feb_path = raw_dir / "sales_feb.csv"
    customers_path = raw_dir / "customers.json"
    database_path = data_dir / "warehouse.db"

    if not sales_jan_path.exists():
        sales_jan_path.write_text(SALES_JAN, encoding="utf-8")

    if not sales_feb_path.exists():
        sales_feb_path.write_text(SALES_FEB, encoding="utf-8")

    if not customers_path.exists():
        customers_path.write_text(json.dumps(CUSTOMERS, ensure_ascii=False, indent=2), encoding="utf-8")

    _ensure_returns_database(database_path)

    return {
        "raw_dir": raw_dir,
        "output_dir": output_dir,
        "database_path": database_path,
        "sales_jan_path": sales_jan_path,
        "sales_feb_path": sales_feb_path,
        "customers_path": customers_path,
    }



def _ensure_returns_database(database_path: Path) -> None:
    connection = sqlite3.connect(database_path)
    try:
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS returns")
        cursor.execute(
            """
            CREATE TABLE returns (
                order_id INTEGER PRIMARY KEY,
                returned_qty INTEGER NOT NULL,
                return_reason TEXT NOT NULL
            )
            """
        )
        cursor.executemany(
            "INSERT INTO returns (order_id, returned_qty, return_reason) VALUES (?, ?, ?)",
            RETURNS,
        )
        connection.commit()
    finally:
        connection.close()
