"""
pipelines/enade/gold_microdados.py — Gold dos Microdados do ENADE: fact_enade.

Lê todos os silvers disponíveis e agrega por (co_ies, co_curso, ano):
  gold/fact_enade.parquet

Métricas por curso:
  - Contagem de inscritos e presentes
  - Notas: média, mediana e desvio-padrão (geral, FG, CE)
  - Metadados do curso (grupo, modalidade, UF, região, categoria adm, org acad)

Chave: (co_ies, co_curso, ano)
Join com: dim_ies (co_ies), fact_censo_cursos (co_ies, co_curso, ano),
          fact_cpc (co_ies, co_curso, ano — 2017+)

Uso:
  python -m pipelines.enade.gold_microdados
  python -m pipelines.enade.gold_microdados --verbose
"""

import sys

import click
import polars as pl

from shared.io import read_parquet, write_parquet
from shared.paths import SILVER_ROOT, gold_path
from shared.validate import assert_no_nulls, assert_not_empty


# TP_PRES == 555 significa "presente com nota válida"
_PRESENTE = 555


def build_fact_enade() -> pl.DataFrame:
    """
    Empilha todos os silvers de enade_microdados e agrega por (co_ies, co_curso, ano).
    """
    silver_dir = SILVER_ROOT / "enade_microdados"
    files = sorted(silver_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum silver encontrado em {silver_dir}\n"
            "Execute primeiro: python -m pipelines.enade.silver_microdados --year <anos>"
        )

    frames = []
    for f in files:
        df = read_parquet(f)
        frames.append(df)

    combined = pl.concat(frames, how="diagonal")
    print(f"Silver carregado: {len(files)} anos, {combined.height:,} estudantes")

    # -------------------------------------------------------------------------
    # Metadados do curso (constantes por co_curso em cada ano)
    # Pegar a primeira ocorrência por (co_curso, ano) — são todos iguais.
    # -------------------------------------------------------------------------
    meta_cols = [
        "ano", "co_ies", "co_curso", "co_grupo",
        "tp_categoria_adm", "tp_org_academica", "tp_modalidade",
        "co_municipio", "co_uf", "co_regiao",
    ]
    meta_cols = [c for c in meta_cols if c in combined.columns]

    curso_meta = (
        combined
        .select(meta_cols)
        .unique(subset=["co_ies", "co_curso", "ano"], keep="first")
    )

    # -------------------------------------------------------------------------
    # Agregações por (co_ies, co_curso, ano)
    # -------------------------------------------------------------------------
    # Total de inscritos
    agg_inscritos = (
        combined
        .group_by(["co_ies", "co_curso", "ano"])
        .agg(pl.len().alias("qt_inscritos"))
    )

    # Métricas dos presentes (TP_PRES == 555)
    presentes = combined.filter(pl.col("tp_pres") == _PRESENTE)

    agg_notas = (
        presentes
        .group_by(["co_ies", "co_curso", "ano"])
        .agg([
            pl.len().alias("qt_presentes"),

            # Nota geral
            pl.col("nt_ger").mean().alias("nt_ger_media"),
            pl.col("nt_ger").median().alias("nt_ger_mediana"),
            pl.col("nt_ger").std().alias("nt_ger_dp"),

            # Formação Geral
            pl.col("nt_fg").mean().alias("nt_fg_media"),
            pl.col("nt_fg").median().alias("nt_fg_mediana"),
            pl.col("nt_fg").std().alias("nt_fg_dp"),

            # Componente Específico
            pl.col("nt_ce").mean().alias("nt_ce_media"),
            pl.col("nt_ce").median().alias("nt_ce_mediana"),
            pl.col("nt_ce").std().alias("nt_ce_dp"),

            # Objetiva FG e CE
            pl.col("nt_obj_fg").mean().alias("nt_obj_fg_media"),
            pl.col("nt_obj_ce").mean().alias("nt_obj_ce_media"),

            # Discursiva FG e CE
            pl.col("nt_dis_fg").mean().alias("nt_dis_fg_media"),
            pl.col("nt_dis_ce").mean().alias("nt_dis_ce_media"),
        ])
    )

    # -------------------------------------------------------------------------
    # Juntar tudo
    # -------------------------------------------------------------------------
    df = (
        curso_meta
        .join(agg_inscritos, on=["co_ies", "co_curso", "ano"], how="left")
        .join(agg_notas, on=["co_ies", "co_curso", "ano"], how="left")
    )

    # Cursos sem nenhum presente → qt_presentes = 0
    df = df.with_columns(pl.col("qt_presentes").fill_null(0))

    # Taxa de participação
    df = df.with_columns(
        (pl.col("qt_presentes") / pl.col("qt_inscritos")).alias("tx_participacao")
    )

    # -------------------------------------------------------------------------
    # Percentis por (co_grupo, ano) — posição relativa dentro da mesma área
    # Apenas cursos com presentes (nota válida)
    # -------------------------------------------------------------------------
    df = df.with_columns([
        pl.col("nt_ger_media")
        .rank("ordinal")
        .over(["co_grupo", "ano"])
        .alias("_rank_ger"),
        pl.col("nt_ger_media")
        .count()
        .over(["co_grupo", "ano"])
        .alias("_total_grupo"),
    ])

    # Percentil: posição relativa de 0 a 100 dentro do grupo/ano
    df = df.with_columns(
        pl.when(pl.col("_total_grupo") > 1)
        .then(
            ((pl.col("_rank_ger") - 1) / (pl.col("_total_grupo") - 1) * 100)
            .round(1)
        )
        .otherwise(pl.lit(50.0))  # curso único no grupo → percentil 50
        .alias("percentil_grupo")
    ).drop(["_rank_ger", "_total_grupo"])

    # Cast tipos finais
    df = df.with_columns([
        pl.col("qt_inscritos").cast(pl.Int32),
        pl.col("qt_presentes").cast(pl.Int32),
        pl.col("tx_participacao").cast(pl.Float32),
        pl.col("percentil_grupo").cast(pl.Float32),
        pl.col("nt_ger_media").cast(pl.Float32),
        pl.col("nt_ger_mediana").cast(pl.Float32),
        pl.col("nt_ger_dp").cast(pl.Float32),
        pl.col("nt_fg_media").cast(pl.Float32),
        pl.col("nt_fg_mediana").cast(pl.Float32),
        pl.col("nt_fg_dp").cast(pl.Float32),
        pl.col("nt_ce_media").cast(pl.Float32),
        pl.col("nt_ce_mediana").cast(pl.Float32),
        pl.col("nt_ce_dp").cast(pl.Float32),
        pl.col("nt_obj_fg_media").cast(pl.Float32),
        pl.col("nt_obj_ce_media").cast(pl.Float32),
        pl.col("nt_dis_fg_media").cast(pl.Float32),
        pl.col("nt_dis_ce_media").cast(pl.Float32),
    ])

    df = df.sort(["co_ies", "co_curso", "ano"])

    return df


@click.command()
@click.option("--verbose", is_flag=True)
def main(verbose: bool) -> None:
    """Gera fact_enade a partir dos silvers dos Microdados do ENADE."""
    try:
        df = build_fact_enade()

        assert_not_empty(df, "fact_enade")
        assert_no_nulls(df, ["co_ies", "co_curso", "ano"], "fact_enade")

        # Validar PK
        pk = ["co_ies", "co_curso", "ano"]
        uniq = df.select(pk).unique().height
        if uniq != df.height:
            raise ValueError(
                f"fact_enade: PK {pk} não é única "
                f"({df.height - uniq} duplicatas em {df.height} linhas)"
            )

        out = gold_path("fact_enade")
        write_parquet(df, out)

        anos = sorted(df["ano"].unique().to_list())
        print(f"✓ fact_enade → {out}")
        print(
            f"  {df.shape[0]:,} linhas | "
            f"{df['co_ies'].n_unique():,} IES | "
            f"{df['co_curso'].n_unique():,} cursos | "
            f"anos: {anos[0]}–{anos[-1]} ({len(anos)} anos)"
        )

        if verbose:
            print(df.head(5))
            print()
            # Amostra: média geral por ano
            por_ano = (
                df.filter(pl.col("qt_presentes") > 0)
                .group_by("ano")
                .agg([
                    pl.col("nt_ger_media").mean().alias("media_geral"),
                    pl.col("qt_presentes").sum().alias("total_presentes"),
                    pl.len().alias("cursos"),
                ])
                .sort("ano")
            )
            print("Resumo por ano:")
            print(por_ano)

    except Exception as e:
        print(f"✗ Erro: {e}", file=sys.stderr)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
