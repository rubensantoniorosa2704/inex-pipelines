"""
queries/cpc_idd_censo.py — Visão analítica por curso: CPC + IDD + contexto da IES.

Cruza fact_cpc, fact_idd e hist_ies para produzir uma visão consolidada
por (co_ies, co_curso, ano) com indicadores de qualidade e contexto institucional.

Uso:
  python queries/cpc_idd_censo.py
  python queries/cpc_idd_censo.py --year 2023
  python queries/cpc_idd_censo.py --year 2023 --uf SP
  python queries/cpc_idd_censo.py --year 2023 --rede publica
  python queries/cpc_idd_censo.py --output resultado.parquet
"""

import os
import sys
from pathlib import Path

import click
import duckdb


DATA_DIR = Path(os.environ.get("PIPELINES_DATA_DIR", "./data"))
GOLD = DATA_DIR / "gold"


def get_paths() -> dict[str, str]:
    paths = {
        "fact_cpc": str(GOLD / "fact_cpc.parquet"),
        "fact_idd": str(GOLD / "fact_idd.parquet"),
        "hist_ies": str(GOLD / "hist_ies.parquet"),
        "dim_ies": str(GOLD / "dim_ies.parquet"),
        "fact_censo_ies": str(GOLD / "fact_censo_ies.parquet"),
    }
    for name, path in paths.items():
        if not Path(path).exists():
            print(f"✗ Arquivo não encontrado: {path}", file=sys.stderr)
            print(f"  Execute primeiro os pipelines: censo, cpc e idd.", file=sys.stderr)
            sys.exit(1)
    return paths


def build_query(paths: dict[str, str], year: int | None, uf: str | None, rede: str | None) -> str:
    """
    Monta a query de cruzamento CPC + IDD + contexto IES.

    Granularidade: uma linha por (co_ies, co_curso, ano).

    Joins:
      - fact_cpc  LEFT JOIN fact_idd   ON (co_ies, co_curso, ano)
        → IDD é componente do CPC, sobreposição esperada de 100%
      - resultado LEFT JOIN hist_ies   ON (co_ies, ano)
        → nome atual, categoria, organização acadêmica
      - resultado LEFT JOIN dim_ies    ON co_ies
        → localização (UF, município, região)
      - resultado LEFT JOIN fact_censo_ies ON (co_ies, ano)
        → corpo docente no mesmo ano
    """
    where_clauses = []
    if year:
        where_clauses.append(f"cpc.ano = {year}")
    if uf:
        where_clauses.append(f"UPPER(dim.sg_uf) = '{uf.upper()}'")
    if rede:
        rede_val = 1 if rede.lower() in ("publica", "pública", "1") else 2
        where_clauses.append(f"hist.tp_rede = {rede_val}")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    return f"""
    SELECT
        -- Identificação
        cpc.ano,
        cpc.co_ies,
        COALESCE(hist.no_ies,    cpc.no_ies)   AS no_ies,
        COALESCE(hist.sg_ies,    cpc.sg_ies)   AS sg_ies,
        cpc.co_curso,
        cpc.area_avaliacao,
        cpc.grau_academico,
        cpc.modalidade,

        -- Localização (dim_ies)
        dim.sg_uf,
        dim.no_municipio,
        dim.no_regiao,

        -- Classificação institucional (hist_ies)
        hist.categoria_adm,
        hist.org_academica,
        hist.rede,

        -- Notas ENADE
        cpc.nota_bruta_fg,
        cpc.nota_pad_fg,
        cpc.nota_bruta_ce,
        cpc.nota_pad_ce,
        cpc.enade_continuo,

        -- IDD (via fact_idd — mais detalhado que o campo embutido no CPC)
        idd.qt_participantes_idd,
        idd.prop_participantes_idd,
        idd.nota_bruta_idd,
        idd.idd_continuo,
        idd.idd_faixa,

        -- Corpo docente (CPC)
        cpc.nota_bruta_mestres,
        cpc.nota_pad_mestres,
        cpc.nota_bruta_doutores,
        cpc.nota_pad_doutores,
        cpc.nota_bruta_regime,
        cpc.nota_pad_regime,

        -- Percepção discente (CPC)
        cpc.nota_bruta_org_didatica,
        cpc.nota_pad_org_didatica,
        cpc.nota_bruta_infra,
        cpc.nota_pad_infra,
        cpc.nota_bruta_oportunidade,
        cpc.nota_pad_oportunidade,

        -- CPC final
        cpc.cpc_continuo,
        cpc.cpc_faixa,

        -- Contexto do censo: docentes no mesmo ano
        censo.qt_doc_total,
        censo.qt_doc_exe,
        censo.qt_doc_exe_dout,
        censo.qt_doc_exe_mest,

        -- Participação
        cpc.qt_inscritos,
        cpc.qt_participantes,
        cpc.in_cebas

    FROM read_parquet('{paths["fact_cpc"]}')       AS cpc

    LEFT JOIN read_parquet('{paths["fact_idd"]}')  AS idd
        ON  cpc.co_ies   = idd.co_ies
        AND cpc.co_curso = idd.co_curso
        AND cpc.ano      = idd.ano

    LEFT JOIN read_parquet('{paths["hist_ies"]}')  AS hist
        ON  cpc.co_ies = hist.co_ies
        AND cpc.ano    = hist.ano

    LEFT JOIN read_parquet('{paths["dim_ies"]}')   AS dim
        ON  cpc.co_ies = dim.co_ies

    LEFT JOIN read_parquet('{paths["fact_censo_ies"]}') AS censo
        ON  cpc.co_ies = censo.co_ies
        AND cpc.ano    = censo.ano

    {where_sql}

    ORDER BY cpc.ano, cpc.cpc_faixa DESC, cpc.cpc_continuo DESC
    """


@click.command()
@click.option("--year",   type=int,  default=None, help="Filtrar por ano (ex: 2023)")
@click.option("--uf",     default=None, help="Filtrar por UF (ex: SP, MG)")
@click.option("--rede",   default=None, help="Filtrar por rede: publica ou privada")
@click.option("--output", default=None, help="Salvar resultado em Parquet (ex: resultado.parquet)")
@click.option("--limit",  type=int,  default=20, help="Linhas a exibir no terminal (padrão: 20)")
def main(year: int | None, uf: str | None, rede: str | None, output: str | None, limit: int) -> None:
    """Visão por curso: CPC + IDD + contexto da IES (censo)."""
    paths = get_paths()
    query = build_query(paths, year=year, uf=uf, rede=rede)

    con = duckdb.connect()
    df = con.execute(query).df()

    print(f"\n{'='*60}")
    print(f"Resultado: {len(df):,} cursos")
    if year:
        print(f"  Ano:  {year}")
    if uf:
        print(f"  UF:   {uf.upper()}")
    if rede:
        print(f"  Rede: {rede}")
    print(f"{'='*60}")

    # Estatísticas resumidas
    print("\n--- Distribuição CPC (faixa) ---")
    dist = (
        df.groupby("cpc_faixa")["co_curso"]
        .count()
        .sort_index()
        .rename("cursos")
    )
    print(dist.to_string())

    print(f"\n--- Médias gerais ---")
    for col in ["enade_continuo", "idd_continuo", "cpc_continuo"]:
        if col in df.columns:
            val = df[col].dropna().mean()
            print(f"  {col:<25}: {val:.3f}")

    print(f"\n--- Primeiras {limit} linhas ---")
    cols_display = [
        "ano", "sg_uf", "no_ies", "area_avaliacao",
        "enade_continuo", "idd_continuo", "cpc_continuo", "cpc_faixa",
        "rede", "qt_doc_total",
    ]
    cols_display = [c for c in cols_display if c in df.columns]
    print(df[cols_display].head(limit).to_string(index=False))

    if output:
        import polars as pl
        pl.from_pandas(df).write_parquet(output, compression="zstd")
        print(f"\n✓ Salvo em: {output} ({len(df):,} linhas)")


if __name__ == "__main__":
    main()
