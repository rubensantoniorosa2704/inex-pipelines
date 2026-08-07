"""
pipelines/idd/gold.py — Gold do IDD: fact_idd.

Lê todos os silvers disponíveis e produz:
  gold/fact_idd.parquet → uma linha por (co_ies, co_curso, ano)

Pode ser cruzado com dim_ies via co_ies.

Uso:
  python -m pipelines.idd.gold
"""

import sys

import click
import polars as pl

from shared.io import read_parquet, write_parquet
from shared.paths import SILVER_ROOT, gold_path
from shared.validate import assert_no_nulls, assert_not_empty


def build_fact_idd() -> pl.DataFrame:
    """Empilha todos os silvers do IDD disponíveis."""
    silver_dir = SILVER_ROOT / "idd"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum silver encontrado em {silver_dir}\n"
            "Execute primeiro: python -m pipelines.idd.silver --year <anos>"
        )

    frames = [read_parquet(f) for f in files]
    df = (
        pl.concat(frames, how="diagonal")
        .unique(subset=["co_ies", "co_curso", "ano"], keep="first")
        .sort(["co_ies", "ano", "co_curso"])
    )

    print(f"Silver carregado: {len(files)} anos, {df.shape[0]} linhas")
    return df


@click.command()
@click.option("--verbose", is_flag=True)
def main(verbose: bool) -> None:
    """Gera fact_idd a partir dos silvers do IDD."""
    try:
        df = build_fact_idd()

        assert_not_empty(df, "fact_idd")
        assert_no_nulls(df, ["co_ies", "co_curso", "ano"], "fact_idd")

        out = gold_path("fact_idd")
        write_parquet(df, out)

        anos = sorted(df["ano"].unique().to_list())
        print(f"✓ fact_idd → {out}")
        print(f"  {df.shape[0]:,} linhas | {df['co_ies'].n_unique():,} IES | {df['co_curso'].n_unique():,} cursos | anos: {anos}")

        if verbose:
            print(df.head())

    except Exception as e:
        print(f"✗ Erro: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
