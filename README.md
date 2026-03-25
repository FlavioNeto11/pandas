# Projeto de Data Wrangling com Pandas

Este projeto demonstra um fluxo macro de data wrangling com `pandas`, inspirado na arquitetura da imagem:

1. Leitura de múltiplas fontes: `CSV`, `JSON` e `SQL`
2. Inspeção com `head`, `info` e `describe`
3. Limpeza de dados (`NA`, tipos e duplicatas)
4. Transformações e criação de colunas
5. Junções com `merge` e `concat`
6. Agregações com `groupby` e `pivot_table`
7. Geração de saídas em relatórios, tabelas e gráfico

Também inclui exemplos fundamentais inspirados nos slides da aula:

- `NumPy` + `pandas`
- `Series` com índice padrão e personalizado
- `DataFrame` com seleção por coluna, `loc` e `iloc`
- leitura e escrita em `CSV`, `JSON` e `SQL`
- limpeza de dados
- `groupby`, `pivot_table`, `merge`, `melt` e `pivot`
- séries temporais e visualização

## Estrutura

```text
.
├── data
│   ├── raw
│   │   ├── customers.json
│   │   ├── sales_jan.csv
│   │   └── sales_feb.csv
│   └── warehouse.db
├── outputs
├── src
│   └── pandas_wrangling_demo
│       ├── __init__.py
│       ├── data_setup.py
│       └── pipeline.py
├── tests
│   ├── test_fundamentals.py
│   └── test_pipeline.py
├── examples_main.py
├── main.py
├── pandas_aula_completa.ipynb
└── requirements.txt
```

## Como executar

No PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python .\main.py
python .\main.py --stream --chunk-size 2
python .\main.py --stream --chunk-size 2 --workers 4 --in-flight 8
python .\examples_main.py
```

No modo `--stream`, a pipeline lê arquivos `CSV` e `JSONL` por chunks para reduzir uso de memória.
Com `--workers > 1`, o processamento dos chunks usa `ThreadPoolExecutor` com escrita ordenada de saída.
O parâmetro `--in-flight` limita quantos chunks podem ficar em processamento simultâneo (`0` usa valor automático).
No modo `--stream`, as agregações incrementais (incluindo `total_orders` distintos) usam uma base SQLite temporária em disco ao invés de manter estruturas grandes em memória.

### Abrir no VS Code / Jupyter

```powershell
code .\pandas_aula_completa.ipynb
```

## Saídas geradas

Após a execução, o projeto gera em `outputs/`:

- `inspection_summary.txt`
- `cleaned_sales.csv`
- `enriched_sales.csv`
- `sales_by_category.csv`
- `sales_pivot_region_segment.csv`
- `monthly_revenue.png`

Após executar `python .\examples_main.py`, também gera em `outputs/fundamentals/`:

- `fundamentals_report.txt`
- `dataframe_example.csv`
- `cleaning_example.csv`
- `groupby_example.csv`
- `merge_example.csv`
- `time_series_monthly.csv`
- `population_by_state.png`
- `monthly_sales.png`

## Notebook

O arquivo `pandas_aula_completa.ipynb` organiza todo o conteúdo do projeto em células de markdown e código, cobrindo:

- configuração do ambiente
- fundamentos de `NumPy` e `pandas`
- `Series` e `DataFrame`
- I/O com `CSV`, `JSON` e `SQL`
- limpeza e transformação
- `groupby`, `pivot_table`, `merge`, `melt` e `pivot`
- séries temporais, gráficos e pipeline final

## Testes

```powershell
pytest
```
