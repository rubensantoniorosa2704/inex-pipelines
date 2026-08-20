"""
pipelines/censo/silver_cursos.py — Silver do Censo: arquivo de Cursos.

Lê o CSV bruto extraído pelo inex-ingest e produz:
  silver/censo_cursos/{ano}.parquet

Cobertura: 2009–2024 (anos com co_curso estável).

Uso:
  python -m pipelines.censo.silver_cursos --year 2023
  python -m pipelines.censo.silver_cursos --year 2009-2024
"""

import sys
from pathlib import Path

import click
import polars as pl

from pipelines.censo.schema_cursos import (
    CURSO_COLUMN_MAP,
    CURSO_REQUIRED_COLUMNS,
    CURSO_SILVER_SCHEMA,
)
from pipelines.censo.silver import _rename_columns, _cast_schema
from shared.io import read_csv_inep, write_parquet
from shared.paths import bronze_dir, silver_path
from shared.validate import assert_no_nulls, assert_not_empty, assert_required_columns
from shared.years import parse_years


def _find_cursos_csv(year: int) -> Path:
    """Localiza o CSV de cadastro de cursos no bronze do ano."""
    if year < 2009:
        raise ValueError(
            f"Ano {year} não suportado: o Censo anterior a 2009 não possui CO_CURSO "
            "com chave estável compatível com este pipeline."
        )

    base = bronze_dir("censo-superior", year)
    if not base.exists():
        raise FileNotFoundError(
            f"Diretório bronze não encontrado: {base}\n"
            f"Execute: inex-ingest ingest --dataset censo-superior --year {year}"
        )

    candidates = (
        list(base.rglob("*CURSOS*.CSV")) +
        list(base.rglob("*CURSOS*.csv")) +
        list(base.rglob("*cursos*.CSV")) +
        list(base.rglob("*cursos*.csv"))
    )

    if not candidates:
        raise FileNotFoundError(
            f"Arquivo CSV de cursos não encontrado em {base}\n"
            f"Arquivos disponíveis: {list(base.rglob('*.CSV'))}"
        )

    for c in candidates:
        if str(year) in c.name:
            return c

    return candidates[0]


def _is_stale(csv_path: Path, year: int) -> bool:
    out = silver_path("censo_cursos", year)
    if not out.exists():
        return True
    return csv_path.stat().st_mtime > out.stat().st_mtime


def process_year(year: int, verbose: bool = False, force: bool = False) -> None:
    """Processa um único ano do censo de cursos."""
    csv_path = _find_cursos_csv(year)

    if not force and not _is_stale(csv_path, year):
        if verbose:
            print(f"[{year}] silver já atualizado, pulando (use --force para reprocessar)")
        else:
            print(f"~ {year} (já atualizado)")
        return

    if verbose:
        safe_path = str(csv_path).encode("utf-8", "replace").decode("utf-8")
        print(f"[{year}] lendo {safe_path}")

    raw = read_csv_inep(csv_path)

    if verbose:
        print(f"[{year}] {raw.shape[0]} linhas, {raw.shape[1]} colunas")

    # Seleciona apenas colunas mapeadas que existem no arquivo
    cols_present = [c for c in CURSO_COLUMN_MAP if c in raw.columns]
    df = (
        raw.select(cols_present)
        .pipe(_rename_columns, CURSO_COLUMN_MAP)
        .pipe(_cast_schema, CURSO_SILVER_SCHEMA)
    )

    assert_not_empty(df, f"censo_cursos/{year}")
    assert_required_columns(df, CURSO_REQUIRED_COLUMNS, f"censo_cursos/{year}")
    assert_no_nulls(df, ["co_ies", "co_curso", "ano"], f"censo_cursos/{year}")

    out = silver_path("censo_cursos", year)
    write_parquet(df, out)

    print(f"✓ {year} ({df.shape[0]:,} cursos, {df.shape[1]} colunas)")
    if verbose:
        print(df.head(3))


@click.command()
@click.option("--year", required=True, help="Ano ou intervalo: 2023 ou 2009-2024")
@click.option("--force", is_flag=True, help="Reprocessa mesmo se silver já atualizado")
@click.option("--verbose", is_flag=True, help="Log detalhado")
def main(year: str, force: bool, verbose: bool) -> None:
    """Gera o silver do Censo — arquivo de Cursos."""
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
            if verbose:
                import traceback
                traceback.print_exc()
            errors.append(y)

    if errors:
        print(f"\nFalhou em {len(errors)} ano(s): {errors}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
