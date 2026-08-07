"""
pipelines/cpc/gold.py — Gold do CPC: fact_cpc.

Lê todos os silvers disponíveis e produz:
  gold/fact_cpc.parquet → uma linha por (co_ies, co_curso, ano)

Pode ser cruzado com dim_ies via co_ies.

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
    """Empilha todos os silvers do CPC disponíveis, mantendo apenas colunas canônicas."""
    silver_dir = SILVER_ROOT / "cpc"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum silver encontrado em {silver_dir}\n"
            "Execute primeiro: python -m pipelines.cpc.silver --year <anos>"
        )

    frames = [read_parquet(f) for f in files]
    combined = pl.concat(frames, how="diagonal")

    # Mantém apenas colunas canônicas (minúsculas, sem espaços ou caracteres especiais)
    # Descarta colunas históricas extras que não fazem parte do schema padronizado
    canonical = [c for c in combined.columns if c == c.lower() and " " not in c]
    combined = combined.select(canonical)

    df = (
        combined
        .unique(subset=["co_ies", "co_area", "ano"], keep="first")
        .sort(["co_ies", "ano", "co_area"])
    )

    print(f"Silver carregado: {len(files)} anos, {df.shape[0]} linhas")
    return df


@click.command()
@click.option("--verbose", is_flag=True)
def main(verbose: bool) -> None:
    """Gera fact_cpc a partir dos silvers do CPC."""
    try:
        df = build_fact_cpc()

        assert_not_empty(df, "fact_cpc")
        assert_no_nulls(df, ["co_ies", "ano"], "fact_cpc")
        # co_curso é nulo em anos anteriores a 2017 — o INEP não publicava o código

        out = gold_path("fact_cpc")
        write_parquet(df, out)

        anos = sorted(df["ano"].unique().to_list())
        print(f"✓ fact_cpc → {out}")
        print(
            f"  {df.shape[0]:,} linhas | "
            f"{df['co_ies'].n_unique():,} IES | "
            f"{df['co_curso'].n_unique():,} cursos | "
            f"anos: {anos}"
        )

        if verbose:
            print(df.head())

    except Exception as e:
        print(f"✗ Erro: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
