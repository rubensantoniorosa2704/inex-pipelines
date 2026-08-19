"""
pipelines/censo/silver.py — Silver do Censo da Educação Superior (arquivo IES).

Lê o CSV bruto extraído pelo inex-ingest e produz dois Parquets silver por ano:
  - silver/censo_ies/{ano}.parquet     → atributos da IES (para dim + hist)
  - silver/censo_fact/{ano}.parquet    → métricas da IES (docentes, técnicos)

Uso:
  python -m pipelines.censo.silver --year 2023
  python -m pipelines.censo.silver --year 2009-2023
"""

import sys
from pathlib import Path

import click
import polars as pl

from pipelines.censo.schema import (
    FACT_COLUMN_MAP,
    IES_COLUMN_MAP,
    IES_REQUIRED_COLUMNS,
    IES_SILVER_SCHEMA,
)
from shared.io import read_csv_inep, write_parquet
from shared.paths import bronze_dir, silver_path
from shared.validate import assert_no_nulls, assert_not_empty, assert_required_columns
from shared.years import parse_years


def _find_ies_csv(year: int) -> Path:
    """
    Localiza o arquivo CSV de IES dentro do diretório bronze do ano.

    Suporte: 2009 em diante.
    Anos anteriores (1995–2008) têm schema completamente diferente — sem CO_IES,
    separador pipe em vez de ponto-e-vírgula, colunas sem padronização — e não
    são compatíveis com este pipeline.
    """
    if year < 2009:
        raise ValueError(
            f"Ano {year} não suportado: o Censo anterior a 2009 não possui CO_IES "
            "e tem schema incompatível com este pipeline."
        )

    base = bronze_dir("censo-superior", year)
    if not base.exists():
        raise FileNotFoundError(
            f"Diretório bronze não encontrado: {base}\n"
            f"Execute: inex-ingest ingest --dataset censo-superior --year {year}"
        )

    candidates = list(base.rglob("*IES*.CSV")) + list(base.rglob("*IES*.csv"))

    if not candidates:
        raise FileNotFoundError(
            f"Arquivo CSV de IES não encontrado em {base}\n"
            f"Arquivos disponíveis: {list(base.rglob('*.CSV'))}"
        )

    # Preferência: arquivo que contém o ano no nome
    for c in candidates:
        if str(year) in c.name:
            return c

    return candidates[0]


def _rename_columns(df: pl.DataFrame, column_map: dict[str, str]) -> pl.DataFrame:
    """Renomeia apenas as colunas presentes no DataFrame (ignora ausentes)."""
    existing = {k: v for k, v in column_map.items() if k in df.columns}
    return df.rename(existing)


def _cast_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Aplica os tipos canônicos às colunas presentes."""
    casts = [
        pl.col(col).cast(dtype)
        for col, dtype in schema.items()
        if col in df.columns
    ]
    return df.with_columns(casts) if casts else df


def _is_stale(csv_path: Path, year: int) -> bool:
    """
    Retorna True se o silver precisa ser reprocessado:
    - Parquet ainda não existe, ou
    - bronze foi modificado depois do silver existente
    """
    silver_ies = silver_path("censo_ies", year)
    silver_fact = silver_path("censo_fact", year)

    if not silver_ies.exists() or not silver_fact.exists():
        return True

    bronze_mtime = csv_path.stat().st_mtime
    return bronze_mtime > silver_ies.stat().st_mtime


def process_year(year: int, verbose: bool = False, force: bool = False) -> None:
    """Processa um único ano do Censo: lê bronze, separa IES e fatos, salva silver."""
    csv_path = _find_ies_csv(year)

    if not force and not _is_stale(csv_path, year):
        if verbose:
            print(f"[{year}] silver já atualizado, pulando (use --force para reprocessar)")
        else:
            print(f"~ {year} (já atualizado)")
        return

    if verbose:
        print(f"[{year}] lendo {csv_path}")
    raw = read_csv_inep(csv_path)

    if verbose:
        print(f"[{year}] {raw.shape[0]} linhas, {raw.shape[1]} colunas")

    # --- Silver IES (atributos dimensionais) ---
    ies_cols = [c for c in IES_COLUMN_MAP if c in raw.columns]
    df_ies = (
        raw.select(ies_cols)
        .pipe(_rename_columns, IES_COLUMN_MAP)
        .pipe(_cast_schema, IES_SILVER_SCHEMA)
    )

    # tp_rede só existe a partir de 2023; para anos anteriores, deriva de tp_categoria_adm
    # 1=Pública Federal, 2=Pública Estadual, 3=Pública Municipal → rede 1 (Pública)
    # demais → rede 2 (Privada)
    if "tp_rede" not in df_ies.columns and "tp_categoria_adm" in df_ies.columns:
        df_ies = df_ies.with_columns(
            pl.when(pl.col("tp_categoria_adm") <= 3)
            .then(pl.lit(1))
            .otherwise(pl.lit(2))
            .cast(pl.Int32)
            .alias("tp_rede")
        )

    # Validações
    assert_not_empty(df_ies, label=f"censo_ies/{year}")
    assert_required_columns(df_ies, IES_REQUIRED_COLUMNS, label=f"censo_ies/{year}")
    assert_no_nulls(df_ies, ["co_ies", "ano"], label=f"censo_ies/{year}")

    out_ies = silver_path("censo_ies", year)
    write_parquet(df_ies, out_ies)

    if verbose:
        print(f"[{year}] silver IES → {out_ies} ({df_ies.shape[0]} linhas)")

    # --- Silver Fatos (métricas de docentes e técnicos) ---
    fact_cols = [c for c in FACT_COLUMN_MAP if c in raw.columns]
    df_fact = (
        raw.select(fact_cols)
        .pipe(_rename_columns, FACT_COLUMN_MAP)
        .with_columns([
            pl.col("co_ies").cast(pl.Int64),
            pl.col("ano").cast(pl.Int32),
        ])
    )

    assert_not_empty(df_fact, label=f"censo_fact/{year}")
    assert_no_nulls(df_fact, ["co_ies", "ano"], label=f"censo_fact/{year}")

    out_fact = silver_path("censo_fact", year)
    write_parquet(df_fact, out_fact)

    if verbose:
        print(f"[{year}] silver fatos → {out_fact} ({df_fact.shape[0]} linhas)")

    print(f"✓ {year}")


@click.command()
@click.option("--year", required=True, help="Ano ou intervalo: 2023 ou 2009-2023")
@click.option("--force", is_flag=True, help="Reprocessa mesmo que o silver já esteja atualizado")
@click.option("--verbose", is_flag=True, help="Log detalhado")
def main(year: str, force: bool, verbose: bool) -> None:
    """Gera o silver do Censo da Educação Superior (arquivo IES)."""
    years = parse_years(year)
    errors = []

    for y in years:
        try:
            process_year(y, verbose=verbose, force=force)
        except FileNotFoundError as e:
            print(f"✗ {y}: {e}", file=sys.stderr)
            errors.append(y)
        except Exception as e:
            print(f"✗ {y}: erro inesperado — {e}", file=sys.stderr)
            errors.append(y)

    if errors:
        print(f"\nFalhou em {len(errors)} ano(s): {errors}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
