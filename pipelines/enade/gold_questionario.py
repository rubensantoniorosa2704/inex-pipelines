"""
pipelines/enade/gold_questionario.py — Gold do Questionário do Estudante: fact_enade_perfil.

Lê os arquivos bronze de questionário (arq7-arq32 = QE_I01 a QE_I26) e agrega
por (co_ies, co_curso, ano):
  gold/fact_enade_perfil.parquet

Cada questão é agregada como distribuição percentual das respostas por curso.
Ex: QE_I08 (renda) → pct de alunos em cada faixa de renda.

Questões incluídas (perfil socioeconômico — presentes em todos os anos):
  QE_I01: Estado civil
  QE_I02: Cor/raça
  QE_I03: Nacionalidade
  QE_I04: Escolaridade do pai
  QE_I05: Escolaridade da mãe
  QE_I06: Moradia
  QE_I07: Pessoas na família
  QE_I08: Renda familiar
  QE_I09: Situação financeira
  QE_I10: Situação de trabalho
  QE_I11: Bolsa de estudos
  QE_I12: Auxílio permanência (2013+) / Bolsa trabalho (2004-2012)
  QE_I13: Bolsa acadêmica
  QE_I14: Intercâmbio / Atividades extracurriculares
  QE_I15: Ação afirmativa
  QE_I16: UF do ensino médio
  QE_I17: Tipo de escola (pública/privada)
  QE_I18: Modalidade do ensino médio
  QE_I19: Incentivo para graduação
  QE_I20: Grupos de apoio
  QE_I21: Família com ensino superior
  QE_I22: Livros lidos no ano
  QE_I23: Horas de estudo
  QE_I24: Idioma estrangeiro
  QE_I25: Motivo de escolha do curso
  QE_I26: Motivo de escolha da IES

Granularidade: (co_ies, co_curso, ano)
Cada questão vira colunas: qe_iXX_A, qe_iXX_B, ..., qe_iXX_respondentes

O join com arq1 (para obter CO_IES) usa CO_CURSO deduplicado, igual ao silver.

Uso:
  python -m pipelines.enade.gold_questionario
  python -m pipelines.enade.gold_questionario --verbose
"""

import sys
from pathlib import Path

import click
import polars as pl

from shared.io import write_parquet
from shared.paths import bronze_dir, gold_path
from shared.validate import assert_no_nulls, assert_not_empty

# Anos sem ENADE
YEARS_SKIPPED = {2020}

# Questões do perfil socioeconômico (QE_I01 a QE_I26)
QE_QUESTIONS = [f"QE_I{i:02d}" for i in range(1, 27)]

# Arquivo que contém cada QE (padrão: arq{N}.txt onde N = idx+7 para QE_I01-I26)
# QE_I01 → arq7, QE_I02 → arq8, ..., QE_I26 → arq32
QE_ARQ_OFFSET = 6  # QE_I{N} está em arq{N+6}


def _read_csv_all_str(path: Path) -> pl.DataFrame:
    """Lê CSV do INEP com todas as colunas como String."""
    return pl.read_csv(
        path,
        separator=";",
        encoding="latin1",
        null_values=["", "NA", "N/A", "nan"],
        infer_schema_length=0,
        quote_char='"',
    )


def _find_dados_dir(year: int) -> Path:
    """Encontra o diretório de dados dentro da estrutura do bronze."""
    base = bronze_dir("enade-microdados", year)
    if not base.exists():
        raise FileNotFoundError(f"Diretório bronze não encontrado: {base}")

    for root, dirs, files in sorted(base.rglob("*")).__class__.__mro__[0].__init__:
        pass  # won't work, use walk

    import os
    for root, dirs, files in os.walk(base):
        dirname = os.path.basename(root)
        if dirname in ('DADOS', '2.DADOS', '2. DADOS'):
            return Path(root)

    raise FileNotFoundError(f"Diretório DADOS não encontrado em {base}")


def _find_qe_file(year: int, qe_num: int, dados_dir: Path) -> Path | None:
    """Encontra o arquivo que contém QE_I{qe_num} para o ano dado."""
    arq_num = qe_num + QE_ARQ_OFFSET
    pattern = f"microdados{year}_arq{arq_num}.txt"
    path = dados_dir / pattern
    if path.exists():
        return path
    return None


def _find_dados_dir_walk(year: int) -> Path:
    """Encontra o diretório de dados (DADOS ou 2.DADOS) no bronze."""
    import os
    base = bronze_dir("enade-microdados", year)
    if not base.exists():
        raise FileNotFoundError(f"Diretório bronze não encontrado: {base}")
    for root, dirs, files in os.walk(base):
        dirname = os.path.basename(root)
        if dirname in ('DADOS', '2.DADOS', '2. DADOS'):
            return Path(root)
    raise FileNotFoundError(f"Diretório DADOS não encontrado em {base}")


def _aggregate_qe(df: pl.DataFrame, qe_col: str, co_curso_col: str = "CO_CURSO") -> pl.DataFrame:
    """
    Agrega uma questão QE por CO_CURSO.
    Produz colunas: {qe_lower}_A, {qe_lower}_B, ..., {qe_lower}_respondentes
    Cada coluna de resposta contém a PROPORÇÃO (0.0 a 1.0) daquela resposta no curso.
    """
    qe_lower = qe_col.lower()

    # Filtrar apenas respostas válidas (não nulas)
    valid = df.filter(pl.col(qe_col).is_not_null())

    if valid.height == 0:
        return pl.DataFrame()

    # Contar respondentes por curso
    respondentes = (
        valid.group_by(co_curso_col)
        .agg(pl.len().alias(f"{qe_lower}_respondentes"))
    )

    # Contar cada resposta por curso e pivotar
    counts = (
        valid.group_by([co_curso_col, qe_col])
        .agg(pl.len().alias("n"))
    )

    # Pivot: cada valor de resposta vira uma coluna
    pivoted = counts.pivot(
        on=qe_col,
        index=co_curso_col,
        values="n",
    ).fill_null(0)

    # Renomear colunas de resposta
    rename_map = {}
    for col in pivoted.columns:
        if col != co_curso_col:
            rename_map[col] = f"{qe_lower}_{col}"
    pivoted = pivoted.rename(rename_map)

    # Converter contagens em proporções
    resp_cols = [c for c in pivoted.columns if c != co_curso_col]
    pivoted = pivoted.join(respondentes, on=co_curso_col, how="left")

    for col in resp_cols:
        pivoted = pivoted.with_columns(
            (pl.col(col).cast(pl.Float32) / pl.col(f"{qe_lower}_respondentes").cast(pl.Float32))
            .alias(col)
        )

    return pivoted


def process_year(year: int, verbose: bool = False) -> pl.DataFrame | None:
    """Processa questionários de um ano e retorna DataFrame agregado por CO_CURSO."""
    dados_dir = _find_dados_dir_walk(year)

    # Ler arq1 para lookup CO_CURSO → CO_IES
    arq1_path = dados_dir / f"microdados{year}_arq1.txt"
    if not arq1_path.exists():
        raise FileNotFoundError(f"arq1 não encontrado em {dados_dir}")

    arq1 = _read_csv_all_str(arq1_path)
    # Deduplicar por CO_CURSO
    curso_lookup = arq1.select(["CO_CURSO", "CO_IES"]).unique(subset=["CO_CURSO"])

    if verbose:
        print(f"[{year}] {curso_lookup.height} cursos no lookup")

    # Processar cada QE
    all_qe = curso_lookup.clone()
    qe_count = 0

    for i in range(1, 27):
        qe_col = f"QE_I{i:02d}"
        qe_file = _find_qe_file(year, i, dados_dir)

        if qe_file is None:
            continue

        df = _read_csv_all_str(qe_file)

        if qe_col not in df.columns:
            continue

        agg = _aggregate_qe(df, qe_col)
        if agg.height == 0:
            continue

        all_qe = all_qe.join(agg, on="CO_CURSO", how="left")
        qe_count += 1

    if qe_count == 0:
        return None

    # Adicionar ano e renomear
    all_qe = all_qe.with_columns(pl.lit(year).alias("ano"))
    all_qe = all_qe.rename({"CO_CURSO": "co_curso", "CO_IES": "co_ies"})

    # Cast chaves
    all_qe = all_qe.with_columns([
        pl.col("ano").cast(pl.Int32),
        pl.col("co_curso").cast(pl.Int64, strict=False),
        pl.col("co_ies").cast(pl.Int64, strict=False),
    ])

    # Filtrar cursos inválidos
    all_qe = all_qe.filter(pl.col("co_curso").is_not_null() & pl.col("co_ies").is_not_null())

    if verbose:
        print(f"[{year}] {qe_count} questões processadas, {all_qe.height} cursos")

    return all_qe


def build_fact_enade_perfil(verbose: bool = False) -> pl.DataFrame:
    """Constrói fact_enade_perfil empilhando todos os anos."""
    frames = []

    for year in range(2004, 2024):
        if year in YEARS_SKIPPED:
            continue
        try:
            df = process_year(year, verbose=verbose)
            if df is not None:
                frames.append(df)
                print(f"✓ {year} ({df.height:,} cursos, {df.width} colunas)")
            else:
                print(f"~ {year} (sem dados QE)")
        except Exception as e:
            print(f"✗ {year}: {e}", file=sys.stderr)
            if verbose:
                import traceback
                traceback.print_exc()

    if not frames:
        raise ValueError("Nenhum ano processado com sucesso")

    combined = pl.concat(frames, how="diagonal")

    # Reordenar: chaves primeiro
    key_cols = ["ano", "co_ies", "co_curso"]
    other_cols = sorted([c for c in combined.columns if c not in key_cols])
    combined = combined.select(key_cols + other_cols)

    combined = combined.sort(["co_ies", "co_curso", "ano"])

    print(f"\nTotal: {combined.height:,} linhas, {combined.width} colunas")
    return combined


@click.command()
@click.option("--verbose", is_flag=True)
def main(verbose: bool) -> None:
    """Gera fact_enade_perfil a partir dos questionários dos Microdados do ENADE."""
    try:
        df = build_fact_enade_perfil(verbose=verbose)

        assert_not_empty(df, "fact_enade_perfil")
        assert_no_nulls(df, ["co_ies", "co_curso", "ano"], "fact_enade_perfil")

        out = gold_path("fact_enade_perfil")
        write_parquet(df, out)

        anos = sorted(df["ano"].unique().to_list())
        print(f"\n✓ fact_enade_perfil → {out}")
        print(
            f"  {df.shape[0]:,} linhas | "
            f"{df['co_ies'].n_unique():,} IES | "
            f"{df['co_curso'].n_unique():,} cursos | "
            f"anos: {anos[0]}–{anos[-1]} ({len(anos)} anos)"
        )

        if verbose:
            # Mostrar colunas disponíveis
            qe_resp_cols = [c for c in df.columns if c.startswith("qe_i")]
            print(f"  Colunas QE: {len(qe_resp_cols)}")
            print(f"  Primeiras: {qe_resp_cols[:10]}")

    except Exception as e:
        print(f"✗ Erro: {e}", file=sys.stderr)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
