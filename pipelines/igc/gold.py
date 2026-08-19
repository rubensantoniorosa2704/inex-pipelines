"""
pipelines/igc/gold.py — Gold do IGC: fact_igc.

Lê todos os silvers disponíveis e produz:
  gold/fact_igc.parquet → uma linha por (co_ies, ano)

Pode ser cruzado com dim_ies via co_ies.

Uso:
  python -m pipelines.igc.gold
"""

import sys

import click
import polars as pl

from shared.io import read_parquet, write_parquet
from shared.paths import SILVER_ROOT, gold_path
from shared.validate import assert_no_nulls, assert_not_empty


def build_fact_igc() -> pl.DataFrame:
    """Empilha todos os silvers do IGC disponíveis."""
    silver_dir = SILVER_ROOT / "igc"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum silver encontrado em {silver_dir}\n"
            "Execute primeiro: python -m pipelines.igc.silver --year <anos>"
        )

    frames = [read_parquet(f) for f in files]
    combined = pl.concat(frames, how="diagonal")

    # Mantém apenas colunas canônicas (minúsculas, sem espaços)
    canonical = [c for c in combined.columns if c == c.lower() and " " not in c]
    combined = combined.select(canonical)

    df = (
        combined
        .unique(subset=["co_ies", "ano"], keep="first")
        .sort(["co_ies", "ano"])
    )

    print(f"Silver carregado: {len(files)} anos, {df.shape[0]} linhas")
    return df


@click.command()
@click.option("--verbose", is_flag=True)
def main(verbose: bool) -> None:
    """Gera fact_igc a partir dos silvers do IGC."""
    try:
        df = build_fact_igc()

        assert_not_empty(df, "fact_igc")
        assert_no_nulls(df, ["co_ies", "ano"], "fact_igc")

        out = gold_path("fact_igc")
        write_parquet(df, out)

        anos = sorted(df["ano"].unique().to_list())
        print(f"✓ fact_igc → {out}")
        print(
            f"  {df.shape[0]:,} linhas | "
            f"{df['co_ies'].n_unique():,} IES | "
            f"anos: {anos}"
        )

        if verbose:
            print(df.head())

    except Exception as e:
        print(f"✗ Erro: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
