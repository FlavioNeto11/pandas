from pathlib import Path

from src.pandas_wrangling_demo.fundamentals import run_fundamentals_demo


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    result = run_fundamentals_demo(base_dir)
    print("Exemplos fundamentais executados com sucesso.")
    print(f"Seções geradas: {', '.join(result['sections'])}")
    print(f"Arquivos gerados em: {result['output_dir']}")
