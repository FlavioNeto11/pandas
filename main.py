from pathlib import Path

from src.pandas_wrangling_demo.pipeline import run_pipeline


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    result = run_pipeline(base_dir)
    print("Pipeline executada com sucesso.")
    print(f"Linhas processadas: {result['row_count']}")
    print(f"Receita total: {result['total_revenue']:.2f}")
    print(f"Arquivos gerados em: {result['output_dir']}")
