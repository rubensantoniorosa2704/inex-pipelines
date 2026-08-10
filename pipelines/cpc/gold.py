"""
pipelines/cpc/gold.py — Gold do CPC: fact_cpc.

Le todos os silvers disponiveis e produz:
  gold/fact_cpc.parquet -> uma linha por (co_ies, co_area, ano)

Chave primaria: (co_ies, co_area, ano).

Sobre co_curso:
  O CPC eh calculado pelo INEP por (IES, area, ano) — nao por curso individual.
  co_curso eh incluido como enriquecimento (recuperado a partir dos microdados
  do ENADE via shared.lookup_cocurso para 2007–2016). Nos ~6% de casos em que
  o lookup nao consegue resolver (ambiguidade — 2+ cursos da mesma area na
  mesma IES/municipio), co_curso recebe a sentinela -1 ("Curso nao
  identificado"). Isso significa que co_curso pode:
    - repetir dentro da PK canonica (quando -1 aparece 2+ vezes por area/IES/ano)
    - ser usado para joins com fact_idd / fact_censo_cursos apenas quando != -1

Uso:
  python -m pipelines.cpc.gold
"""

import sys

import click
import polars as pl

from shared.io import read_parquet, write_parquet
from shared.paths import SILVER_ROOT, gold_path
from shared.validate import assert_no_nulls, assert_not_empty


def build_fact_cpc() -> pl.DataFrame:
    """Empilha todos os silvers do CPC disponiveis, mantendo apenas colunas canonicas."""
    silver_dir = SILVER_ROOT / "cpc"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum silver encontrado em {silver_dir}\n"
            "Execute primeiro: python -m pipelines.cpc.silver --year <anos>"
        )

    frames = [read_parquet(f) for f in files]
    combined = pl.concat(frames, how="diagonal")

    # Mantem apenas colunas canonicas (minusculas, sem espacos ou caracteres especiais)
    canonical = [c for c in combined.columns if c == c.lower() and " " not in c]
    combined = combined.select(canonical)

    # Dedup pela PK canonica do CPC: (co_ies, co_area, ano)
    df = (
        combined
        .unique(subset=["co_ies", "co_area", "ano"], keep="first")
        .sort(["co_ies", "ano", "co_area"])
    )

    # Sentinela: co_curso nulo -> -1 ("Curso nao identificado")
    # Preserva registros em joins obrigatorios; consumidores devem filtrar
    # co_curso != -1 quando precisarem de curso identificado.
    if "co_curso" in df.columns:
        df = df.with_columns(pl.col("co_curso").fill_null(-1))

    print(f"Silver carregado: {len(files)} anos, {df.shape[0]} linhas")
    return df


@click.command()
@click.option("--verbose", is_flag=True)
def main(verbose: bool) -> None:
    """Gera fact_cpc a partir dos silvers do CPC."""
    try:
        df = build_fact_cpc()

        # Validacao da PK canonica
        assert_not_empty(df, "fact_cpc")
        assert_no_nulls(df, ["co_ies", "co_area", "ano"], "fact_cpc")

        pk = ["co_ies", "co_area", "ano"]
        uniq = df.select(pk).unique().height
        if uniq != df.height:
            raise ValueError(
                f"fact_cpc: PK {pk} nao eh unica "
                f"({df.height - uniq} duplicatas em {df.height} linhas)"
            )

        out = gold_path("fact_cpc")
        write_parquet(df, out)

        anos = sorted(df["ano"].unique().to_list())
        cursos_id = df.filter(pl.col("co_curso") != -1)["co_curso"].n_unique()
        sem_curso = int((df["co_curso"] == -1).sum())

        print(f"OK fact_cpc -> {out}")
        print(
            f"  {df.shape[0]:,} linhas | "
            f"{df['co_ies'].n_unique():,} IES | "
            f"{cursos_id:,} cursos identificados | "
            f"{sem_curso:,} com co_curso=-1 | "
            f"anos: {anos}"
        )

        if verbose:
            print(df.head())

    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
