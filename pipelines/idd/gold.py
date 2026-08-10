"""
pipelines/idd/gold.py — Gold do IDD: fact_idd.

Le todos os silvers disponiveis e produz:
  gold/fact_idd.parquet -> uma linha por (co_ies, co_curso, ano)

Pode ser cruzado com dim_ies via co_ies.

Normalizacoes defensivas aplicadas no gold:
  - in_cebas: converte "X"/"-" (formato bruto do INEP) para Boolean
    caso o silver ainda esteja com String.
  - in_sem_conceito: deriva de idd_faixa nula OR observacao preenchida
    caso o silver nao tenha a coluna.
  - observacao: coluna auxiliar do silver, dropada no gold.

Uso:
  python -m pipelines.idd.gold
"""

import sys

import click
import polars as pl

from shared.io import read_parquet, write_parquet
from shared.paths import SILVER_ROOT, gold_path
from shared.validate import assert_no_nulls, assert_not_empty


def _normalize_in_cebas(df: pl.DataFrame) -> pl.DataFrame:
    """
    Garante que in_cebas seja Boolean.
    Aceita formato bruto do INEP ("X" = True, "-" = False) e formato ja
    convertido (Boolean). Se a coluna nao existir, cria como null.
    """
    if "in_cebas" not in df.columns:
        return df.with_columns(pl.lit(None, dtype=pl.Boolean).alias("in_cebas"))

    dtype = df.schema["in_cebas"]
    if dtype == pl.Boolean:
        return df

    if dtype == pl.String:
        return df.with_columns(
            pl.when(pl.col("in_cebas") == "X")
            .then(pl.lit(True))
            .when(pl.col("in_cebas") == "-")
            .then(pl.lit(False))
            .otherwise(pl.lit(None, dtype=pl.Boolean))
            .alias("in_cebas")
        )

    # Outros tipos: cast defensivo
    return df.with_columns(pl.col("in_cebas").cast(pl.Boolean, strict=False))


def _derive_in_sem_conceito(df: pl.DataFrame) -> pl.DataFrame:
    """
    Deriva in_sem_conceito quando o silver nao a persistiu.

    Regra: um curso esta "sem conceito" quando:
      - idd_faixa eh nula (INEP nao publicou nota) OU
      - observacao eh preenchida (INEP explicita ausencia de dado)

    Se in_sem_conceito ja existe no silver, apenas garante Boolean.
    """
    if "in_sem_conceito" in df.columns:
        dtype = df.schema["in_sem_conceito"]
        if dtype != pl.Boolean:
            df = df.with_columns(pl.col("in_sem_conceito").cast(pl.Boolean, strict=False))
        return df

    faixa_null = pl.col("idd_faixa").is_null() if "idd_faixa" in df.columns else pl.lit(False)

    if "observacao" in df.columns:
        obs_str = pl.col("observacao").cast(pl.Utf8).str.strip_chars()
        obs_preenchida = obs_str.is_not_null() & (obs_str != "") & (obs_str != "None")
    else:
        obs_preenchida = pl.lit(False)

    return df.with_columns((faixa_null | obs_preenchida).alias("in_sem_conceito"))


def build_fact_idd() -> pl.DataFrame:
    """Empilha todos os silvers do IDD disponiveis."""
    silver_dir = SILVER_ROOT / "idd"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum silver encontrado em {silver_dir}\n"
            "Execute primeiro: python -m pipelines.idd.silver --year <anos>"
        )

    frames = [read_parquet(f) for f in files]
    df = pl.concat(frames, how="diagonal")

    # Normalizacoes defensivas
    df = _normalize_in_cebas(df)
    df = _derive_in_sem_conceito(df)

    # Dropa colunas internas que nao devem vazar para o consumo analitico
    if "observacao" in df.columns:
        df = df.drop("observacao")

    df = (
        df
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

        # Valida unicidade da PK
        pk = ["co_ies", "co_curso", "ano"]
        uniq = df.select(pk).unique().height
        if uniq != df.height:
            raise ValueError(
                f"fact_idd: PK {pk} nao eh unica "
                f"({df.height - uniq} duplicatas em {df.height} linhas)"
            )

        out = gold_path("fact_idd")
        write_parquet(df, out)

        anos = sorted(df["ano"].unique().to_list())
        n_sc = int(df["in_sem_conceito"].sum()) if "in_sem_conceito" in df.columns else 0
        n_cebas = int(df["in_cebas"].sum()) if "in_cebas" in df.columns else 0

        print(f"OK fact_idd -> {out}")
        print(
            f"  {df.shape[0]:,} linhas | "
            f"{df['co_ies'].n_unique():,} IES | "
            f"{df['co_curso'].n_unique():,} cursos | "
            f"anos: {anos}"
        )
        print(f"  sem conceito: {n_sc:,} | CEBAS: {n_cebas:,}")

        if verbose:
            print(df.head())

    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
