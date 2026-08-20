"""
pipelines/enade/silver_microdados.py — Silver dos Microdados do ENADE.

Lê os arquivos bronze (arq1 + arq3) e produz um Parquet silver por ano:
  silver/enade_microdados/{ano}.parquet

Granularidade: uma linha por estudante inscrito.
Colunas: metadados do curso (via arq1) + notas e vetores (via arq3).

Join: arq3 (estudantes) LEFT JOIN arq1 deduplicado (lookup de curso por CO_CURSO).

Cobertura: 2004–2023 (exceto 2020 — pandemia, ENADE não aplicado).

Uso:
  python -m pipelines.enade.silver_microdados --year 2004-2023
  python -m pipelines.enade.silver_microdados --year 2023 --force --verbose
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
from shared.io import write_parquet
from shared.paths import bronze_dir, silver_path
from shared.validate import assert_no_nulls, assert_not_empty, assert_required_columns
from shared.years import parse_years

# Anos sem aplicação do ENADE
YEARS_SKIPPED = {2020}


def _read_csv_all_str(path: Path) -> pl.DataFrame:
    """
    Lê CSV do INEP com TODAS as colunas como String.
    Necessário porque os vetores de acerto/gabarito (ex: '000090900...') são
    confundidos com inteiros pelo schema inference do Polars.
    O cast para tipos corretos é feito depois via _cast_schema.
    """
    return pl.read_csv(
        path,
        separator=";",
        encoding="latin1",
        null_values=["", "NA", "N/A", "nan"],
        infer_schema_length=0,
        quote_char='"',
    )


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
    return _find_file(year, f"microdados{year}_arq1.txt")


def _find_arq3(year: int) -> Path:
    return _find_file(year, f"microdados{year}_arq3.txt")


def _rename_and_select(df: pl.DataFrame, column_map: dict[str, str]) -> pl.DataFrame:
    """Seleciona e renomeia apenas as colunas presentes no mapeamento."""
    existing = {k: v for k, v in column_map.items() if k in df.columns}
    return df.select(list(existing.keys())).rename(existing)


def _cast_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica os tipos canônicos às colunas numéricas.
    Vetores e CO_RS_I* permanecem como String (Utf8).
    """
    casts = []
    for col, dtype in SILVER_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype, strict=False))
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

    # -------------------------------------------------------------------------
    # arq1: metadados do curso — deduplicar por CO_CURSO para lookup
    # Leitura com infer_schema_length=0 (tudo String) para evitar erros de
    # inferência — o cast é feito depois via _cast_schema.
    # -------------------------------------------------------------------------
    arq1_raw = _read_csv_all_str(arq1_path)
    arq1 = (
        arq1_raw
        .pipe(_rename_and_select, ARQ1_COLUMN_MAP)
        .unique(subset=["co_curso"])
    )

    if verbose:
        print(f"[{year}] arq1: {arq1_raw.height:,} linhas → {arq1.height:,} cursos únicos")
        print(f"[{year}] lendo {arq3_path.name}")

    # -------------------------------------------------------------------------
    # arq3: desempenho dos estudantes — uma linha por estudante
    # Leitura com infer_schema_length=0: vetores são strings longas que Polars
    # confunde com Int64 se as primeiras linhas forem só zeros.
    # -------------------------------------------------------------------------
    arq3 = _read_csv_all_str(arq3_path).pipe(_rename_and_select, ARQ3_COLUMN_MAP)

    if verbose:
        print(f"[{year}] arq3: {arq3.height:,} estudantes")

    # -------------------------------------------------------------------------
    # Join: arq3 (estudantes) LEFT JOIN arq1 (curso) por co_curso
    # -------------------------------------------------------------------------
    # Remover 'ano' de arq1 pois já existe em arq3; remover 'co_curso' duplicado
    arq1_cols = [c for c in arq1.columns if c not in ("ano", "co_curso")]
    df = arq3.join(
        arq1.select(["co_curso"] + arq1_cols),
        on="co_curso",
        how="left",
    )

    # Cast tipos numéricos
    df = _cast_schema(df)

    # Filtrar linhas onde co_ies ficou null após cast (ex: CO_CURSO não-numérico
    # como "DJ1" em 2014 que não existe em arq1 → co_ies null)
    n_before = df.height
    df = df.filter(pl.col("co_ies").is_not_null() & pl.col("co_curso").is_not_null())
    n_dropped = n_before - df.height
    if n_dropped > 0 and verbose:
        print(f"[{year}] {n_dropped} linhas removidas (co_ies/co_curso nulo após cast/join)")

    # -------------------------------------------------------------------------
    # Validações
    # -------------------------------------------------------------------------
    assert_not_empty(df, label=f"enade_microdados/{year}")
    assert_required_columns(df, REQUIRED_COLUMNS, label=f"enade_microdados/{year}")
    assert_no_nulls(df, ["co_ies", "ano", "co_curso"], label=f"enade_microdados/{year}")

    # -------------------------------------------------------------------------
    # Escrita
    # -------------------------------------------------------------------------
    out = silver_path("enade_microdados", year)
    write_parquet(df, out)

    n_presentes = df.filter(pl.col("tp_pres") == 555).height
    print(
        f"✓ {year} ({df.height:,} inscritos, {n_presentes:,} presentes, "
        f"{df['co_curso'].n_unique():,} cursos, {df['co_ies'].n_unique():,} IES)"
    )

    if verbose:
        print(df.head(3))


@click.command()
@click.option("--year", required=True, help="Ano ou intervalo: 2023 ou 2004-2023")
@click.option("--force", is_flag=True, help="Reprocessa mesmo que o silver já esteja atualizado")
@click.option("--verbose", is_flag=True, help="Log detalhado")
def main(year: str, force: bool, verbose: bool) -> None:
    """Gera o silver dos Microdados do ENADE (2004–2023)."""
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
