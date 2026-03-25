import argparse
from pathlib import Path

from src.pandas_wrangling_demo.pipeline import run_pipeline


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa a pipeline de data wrangling.")
    parser.add_argument("--stream", action="store_true", help="Ativa leitura de CSV em chunks (streaming).")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Quantidade de linhas por chunk no modo streaming.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Quantidade de threads de processamento no modo streaming.",
    )
    parser.add_argument(
        "--in-flight",
        type=int,
        default=0,
        help="Máximo de chunks em processamento simultâneo (0 = automático).",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    result = run_pipeline(
        base_dir,
        stream=args.stream,
        chunk_size=args.chunk_size,
        stream_workers=args.workers,
        in_flight_tasks=args.in_flight,
    )
    print("Pipeline executada com sucesso.")
    print(f"Modo streaming: {args.stream}")
    if args.stream:
        print(f"Workers: {args.workers}")
        print(f"In-flight: {args.in_flight if args.in_flight > 0 else 'auto'}")
    print(f"Linhas processadas: {result['row_count']}")
    print(f"Receita total: {result['total_revenue']:.2f}")
    print(f"Arquivos gerados em: {result['output_dir']}")
