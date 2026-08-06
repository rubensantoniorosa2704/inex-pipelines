"""
pipelines/censo/gold.py — Gold do Censo: dim_ies, hist_ies e fact_censo_ies.

Lê todos os Parquets silver disponíveis e produz:
  - gold/dim_ies.parquet        → 1 linha por co_ies, estado mais recente
  - gold/hist_ies.parquet       → 1 linha por (co_ies, ano), histórico completo
  - gold/fact_censo_ies.parquet → métricas anuais de docentes e técnicos

Uso:
  python -m pipelines.censo.gold
"""

import sys

import click
import polars as pl

from pipelines.censo.schema import CATEGORIA_ADM, ORG_ACADEMICA, REDE
from shared.io import read_parquet, write_parquet
from shared.paths import SILVER_ROOT, gold_path
from shared.validate import assert_no_nulls, assert_not_empty, assert_required_columns

# Colunas que compõem o histórico — atributos que mudam ao longo do tempo
HIST_COLUMNS = [
    "co_ies",
    "ano",
    "no_ies",
    "sg_ies",
    "tp_categoria_adm",
    "tp_org_academica",
    "tp_rede",
    "no_mantenedora",
    "co_mantenedora",
    # Labels decodificados
    "categoria_adm",
    "org_academica",
    "rede",
]

# Colunas estáticas da IES — ficam apenas na dim
DIM_STATIC_COLUMNS = [
    "co_ies",
    "sg_uf",
    "no_uf",
    "co_uf",
    "co_municipio",
    "no_municipio",
    "co_regiao",
    "no_regiao",
    "in_capital",
    "co_mesorregiao",
    "no_mesorregiao",
    "co_microrregiao",
    "no_microrregiao",
]


def _decode_labels(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona colunas de texto decodificadas a partir dos tipos numéricos."""

    def replace_map(col: str, mapping: dict[int, str]) -> pl.Expr:
        keys = list(mapping.keys())
        values = list(mapping.values())
        return (
            pl.col(col)
            .replace(
                old=pl.Series(keys, dtype=pl.Int32),
                new=pl.Series(values, dtype=pl.String),
                default=None,
            )
            .alias(col.replace("tp_", "").replace("_adm", "_adm"))
        )

    exprs = []
    if "tp_categoria_adm" in df.columns:
        exprs.append(
            pl.col("tp_categoria_adm")
            .replace_strict(
                old=pl.Series(list(CATEGORIA_ADM.keys()), dtype=pl.Int32),
                new=pl.Series(list(CATEGORIA_ADM.values()), dtype=pl.String),
                default=None,
                return_dtype=pl.String,
            )
            .alias("categoria_adm")
        )
    if "tp_org_academica" in df.columns:
        exprs.append(
            pl.col("tp_org_academica")
            .replace_strict(
                old=pl.Series(list(ORG_ACADEMICA.keys()), dtype=pl.Int32),
                new=pl.Series(list(ORG_ACADEMICA.values()), dtype=pl.String),
                default=None,
                return_dtype=pl.String,
            )
            .alias("org_academica")
        )
    if "tp_rede" in df.columns:
        exprs.append(
            pl.col("tp_rede")
            .replace_strict(
                old=pl.Series(list(REDE.keys()), dtype=pl.Int32),
                new=pl.Series(list(REDE.values()), dtype=pl.String),
                default=None,
                return_dtype=pl.String,
            )
            .alias("rede")
        )

    return df.with_columns(exprs) if exprs else df


def _load_all_silver_ies() -> pl.DataFrame:
    """Carrega e empilha todos os Parquets silver de censo_ies disponíveis."""
    silver_dir = SILVER_ROOT / "censo_ies"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum silver encontrado em {silver_dir}\n"
            "Execute primeiro: python -m pipelines.censo.silver --year <anos>"
        )

    frames = [read_parquet(f) for f in files]
    combined = pl.concat(frames, how="diagonal")  # diagonal: tolera colunas ausentes entre anos

    print(f"Silver carregado: {len(files)} anos, {combined.shape[0]} linhas")
    return combined


def build_hist_ies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Constrói hist_ies: uma linha por (co_ies, ano) com os atributos que mudam.
    Remove duplicatas mantendo apenas um registro por (co_ies, ano).
    """
    df = _decode_labels(df)

    hist_cols = [c for c in HIST_COLUMNS if c in df.columns]
    return (
        df.select(hist_cols)
        .unique(subset=["co_ies", "ano"], keep="first")
        .sort(["co_ies", "ano"])
    )


def build_dim_ies(df: pl.DataFrame, hist: pl.DataFrame) -> pl.DataFrame:
    """
    Constrói dim_ies: uma linha por co_ies com o estado mais recente.

    Estratégia:
    1. Para atributos estáticos (localização): pega o registro do ano mais recente.
    2. Faz join com hist para trazer os atributos atuais (nome, categoria, etc.).
    """
    # Pega o ano mais recente de cada IES
    latest_year = (
        df.select(["co_ies", "ano"])
        .sort("ano", descending=True)
        .unique(subset=["co_ies"], keep="first")
        .rename({"ano": "ano_referencia"})
    )

    # Atributos estáticos do ano mais recente
    static_cols = [c for c in DIM_STATIC_COLUMNS if c in df.columns]
    static = (
        df.select(static_cols + ["ano"])
        .sort("ano", descending=True)
        .unique(subset=["co_ies"], keep="first")
        .drop("ano")
    )

    # Atributos dinâmicos do ano mais recente (via hist)
    dynamic_cols = [c for c in HIST_COLUMNS if c != "co_ies" and c != "ano"]
    dynamic = (
        hist.sort("ano", descending=True)
        .unique(subset=["co_ies"], keep="first")
        .select(["co_ies"] + [c for c in dynamic_cols if c in hist.columns])
        .rename({
            "no_ies": "no_ies_atual",
            "sg_ies": "sg_ies_atual",
            "tp_categoria_adm": "tp_categoria_adm_atual",
            "tp_org_academica": "tp_org_academica_atual",
            "tp_rede": "tp_rede_atual",
            "no_mantenedora": "no_mantenedora_atual",
            "co_mantenedora": "co_mantenedora_atual",
            "categoria_adm": "categoria_adm_atual",
            "org_academica": "org_academica_atual",
            "rede": "rede_atual",
        })
    )

    dim = (
        latest_year
        .join(static, on="co_ies", how="left")
        .join(dynamic, on="co_ies", how="left")
        .sort("co_ies")
    )

    return dim


def build_fact_censo_ies() -> pl.DataFrame:
    """Empilha todos os Parquets silver de métricas (docentes, técnicos)."""
    silver_dir = SILVER_ROOT / "censo_fact"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"Nenhum silver de fatos encontrado em {silver_dir}")

    frames = [read_parquet(f) for f in files]
    return (
        pl.concat(frames, how="diagonal")
        .unique(subset=["co_ies", "ano"], keep="first")
        .sort(["co_ies", "ano"])
    )


@click.command()
@click.option("--verbose", is_flag=True, help="Log detalhado")
def main(verbose: bool) -> None:
    """Gera dim_ies, hist_ies e fact_censo_ies a partir dos silvers do Censo."""
    try:
        df = _load_all_silver_ies()

        # --- hist_ies ---
        hist = build_hist_ies(df)
        assert_not_empty(hist, "hist_ies")
        assert_required_columns(hist, ["co_ies", "ano", "no_ies", "sg_ies"], "hist_ies")
        assert_no_nulls(hist, ["co_ies", "ano"], "hist_ies")

        out_hist = gold_path("hist_ies")
        write_parquet(hist, out_hist)
        print(f"✓ hist_ies → {out_hist} ({hist.shape[0]} linhas, {hist['co_ies'].n_unique()} IES)")

        if verbose:
            print(hist.head())

        # --- dim_ies ---
        dim = build_dim_ies(df, hist)
        assert_not_empty(dim, "dim_ies")
        assert_required_columns(dim, ["co_ies", "no_ies_atual", "sg_uf"], "dim_ies")
        assert_no_nulls(dim, ["co_ies"], "dim_ies")

        out_dim = gold_path("dim_ies")
        write_parquet(dim, out_dim)
        print(f"✓ dim_ies  → {out_dim} ({dim.shape[0]} IES)")

        if verbose:
            print(dim.head())

        # --- fact_censo_ies ---
        try:
            fact = build_fact_censo_ies()
            assert_not_empty(fact, "fact_censo_ies")
            assert_no_nulls(fact, ["co_ies", "ano"], "fact_censo_ies")

            out_fact = gold_path("fact_censo_ies")
            write_parquet(fact, out_fact)
            print(f"✓ fact_censo_ies → {out_fact} ({fact.shape[0]} linhas)")
        except FileNotFoundError as e:
            print(f"⚠ fact_censo_ies ignorado: {e}", file=sys.stderr)

    except Exception as e:
        print(f"✗ Erro: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
