"""
pipelines/enade/silver_microdados.py — Silver dos Microdados do ENADE.

Lê os arquivos bronze extraídos pelo inex-ingest e produz um Parquet silver
por ano com uma linha por estudante inscrito:
  silver/enade_microdados/{ano}.parquet

O silver preserva a granularidade individual (um estudante por linha) e é
insumo para o gold, que agrega por (co_ies, co_curso, ano).

Arquivos usados do bronze:
  microdados{ano}_arq1.txt — dados do curso (CO_IES fica aqui)
  microdados{ano}_arq3.txt — notas e presença

Join: (NU_ANO, CO_CURSO) — chave comum entre os dois arquivos.

Uso:
  python -m pipelines.enade.silver_microdados --year 2022
  python -m pipelines.enade.silver_microdados --year 2017-2022
"""

import sys
from pathlib import Path

import click
import polars as pl

from pipelines.enade.schema_microdados import (
    ARQ1_COLUMN_MAP,
    ARQ3_COLUMN_MAP,
    REQUIRED_COLUMNS,
    SILVER_SCHEMA,
)
from shared.io import read_csv_inep, write_parquet
from shared.paths import bronze_dir, silver_path
from shared.validate import assert_no_nulls, assert_not_empty, assert_required_columns
from shared.years import parse_years

# Anos sem aplicação do ENADE
YEARS_SKIPPED = {2020}


def _find_file(year: int, pattern: str) -> Path:
    """
    Localiza um arquivo bronze por padrão glob dentro do diretório do ano.
    Busca recursivamente para lidar com subdiretórios arbitrários do inex-ingest.
    """
    base = bronze_dir("enade-microdados", year)
    if not base.exists():
        raise FileNotFoundError(
            f"Diretório bronze não encontrado: {base}\n"
            f"Execute: inex-ingest ingest --dataset enade-microdados --year {year}"
        )
    candidates = list(base.rglob(pattern))
    if not candidates:
        raise FileNotFoundError(
            f"Arquivo '{pattern}' não encontrado em {base}"
        )
    return candidates[0]


def _find_arq1(year: int) -> Path:
    """Localiza o arquivo com dados do curso (contém CO_IES)."""
    return _find_file(year, f"microdados{year}_arq1.txt")


def _find_arq3(year: int) -> Path:
    """Localiza o arquivo com notas e presença (contém NT_GER)."""
    return _find_file(year, f"microdados{year}_arq3.txt")


def _rename_and_select(df: pl.DataFrame, column_map: dict[str, str]) -> pl.DataFrame:
    """Seleciona e renomeia apenas as colunas presentes no mapeamento."""
    existing = {k: v for k, v in column_map.items() if k in df.columns}
    return df.select(list(existing.keys())).rename(existing)


def _cast_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica os tipos canônicos.
    Notas chegam como String no CSV (ex: '42.5') — cast para Float32.
    Valores não numéricos viram nulo (strict=False).
    """
    casts = [
        pl.col(col).cast(dtype, strict=False)
        for col, dtype in SILVER_SCHEMA.items()
        if col in df.columns
    ]
    return df.with_columns(casts) if casts else df


def _is_stale(year: int, arq1_path: Path, arq3_path: Path) -> bool:
    """Retorna True se o silver precisa ser reprocessado."""
    out = silver_path("enade_microdados", year)
    if not out.exists():
        return True
    silver_mtime = out.stat().st_mtime
    return (
        arq1_path.stat().st_mtime > silver_mtime
        or arq3_path.stat().st_mtime > silver_mtime
    )


def process_year(year: int, verbose: bool = False, force: bool = False) -> None:
    """Processa um único ano dos microdados do ENADE."""
    arq1_path = _find_arq1(year)
    arq3_path = _find_arq3(year)

    if not force and not _is_stale(year, arq1_path, arq3_path):
        if verbose:
            print(f"[{year}] silver já atualizado, pulando (use --force para reprocessar)")
        else:
            print(f"~ {year} (já atualizado)")
        return

    if verbose:
        print(f"[{year}] lendo {arq1_path.name}")

    # arq1: dados do curso — uma linha por estudante inscrito
    arq1 = read_csv_inep(arq1_path).pipe(_rename_and_select, ARQ1_COLUMN_MAP)

    if verbose:
        print(f"[{year}] {len(arq1)} inscritos, {arq1['co_curso'].n_unique()} cursos")
        print(f"[{year}] lendo {arq3_path.name}")

    # arq3: notas e presença — mesma quantidade de linhas que arq1, mesma ordem
    arq3 = read_csv_inep(arq3_path).pipe(_rename_and_select, ARQ3_COLUMN_MAP)

    # Join por (ano, co_curso) — as duas tabelas têm exatamente as mesmas linhas
    # na mesma ordem, mas usamos join explícito para garantir consistência
    df = arq1.join(
        arq3.drop("ano"),  # ano já está em arq1
        on="co_curso",
        how="inner",
    ).pipe(_cast_schema)

    assert_not_empty(df, label=f"enade_microdados/{year}")
    assert_required_columns(df, REQUIRED_COLUMNS, label=f"enade_microdados/{year}")
    assert_no_nulls(df, ["co_ies", "ano", "co_curso"], label=f"enade_microdados/{year}")

    out = silver_path("enade_microdados", year)
    write_parquet(df, out)

    n_presentes = df.filter(pl.col("tp_pres") == 555).height
    print(f"✓ {year} ({len(df):,} inscritos, {n_presentes:,} presentes, "
          f"{df['co_curso'].n_unique()} cursos, {df['co_ies'].n_unique()} IES)")

    if verbose:
        print(df.head(3))


@click.command()
@click.option("--year", required=True, help="Ano ou intervalo: 2022 ou 2017-2022")
@click.option("--force", is_flag=True, help="Reprocessa mesmo que o silver já esteja atualizado")
@click.option("--verbose", is_flag=True, help="Log detalhado")
def main(year: str, force: bool, verbose: bool) -> None:
    """Gera o silver dos Microdados do ENADE (suporta 2017–2022)."""
    years = parse_years(year)
    errors = []

    for y in years:
        if y in YEARS_SKIPPED:
            print(f"~ {y} (ENADE não aplicado)")
            continue
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
