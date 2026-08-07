"""
pipelines/censo/schema_cursos.py — Schema do Censo: arquivo de Cursos.

Baseado no dicionário oficial do INEP:
  Anexos/ANEXO I - Dicionário de Dados/dicionário_dados_educação_superior.xlsx
  Aba: cadastro_cursos
  Cobertura: 2009–2023 (202 variáveis no total)

Granularidade: uma linha por (co_ies, co_curso, ano).
Chave de join com dim_ies / hist_ies: co_ies.
Chave de join com fact_cpc / fact_idd: (co_ies, co_curso, ano).

## Seleção de colunas

O arquivo possui 202 variáveis. São importadas:
  - Identificação do curso (nome, código, área CINE, grau, modalidade)
  - Classificação da IES (organização, rede, categoria) — redundante com
    hist_ies, mas útil para joins diretos sem tabela auxiliar
  - Localização do curso (UF, município, região, capital)
  - Totalizadores de vagas, ingressantes, matrículas e concluintes
  - Cor/raça de ingressantes e concluintes (totais)
  - Origem escolar dos ingressantes (escola pública/privada)
  - Financiamento estudantil — totais (FIES + ProUni)
  - Apoio social, mobilidade acadêmica, deficiência — totais
  - Reserva de vagas — totais

Colunas deliberadamente excluídas:
  - Desagregações por turno (diurno/noturno) — representam múltiplos
    dos totalizadores sem agregar informação analítica nova
  - Detalhamento de formas de ingresso (vestibular, ENEM, etc.) — muito
    fragmentado; QT_ING já cobre o total
  - Desagregações por faixa etária — 8 colunas por indicador; ficam para
    análises específicas
  - Situação de vínculo (trancado, desvinculado, transferido) — derivados
    de matrícula; pouco usados na análise de qualidade
"""

import polars as pl

# ---------------------------------------------------------------------------
# Mapeamento: nome original INEP → nome canônico
# ---------------------------------------------------------------------------

CURSO_COLUMN_MAP: dict[str, str] = {
    # Ano
    "NU_ANO_CENSO": "ano",

    # Identificação da IES e curso
    "CO_IES": "co_ies",
    "CO_CURSO": "co_curso",
    "NO_CURSO": "no_curso",

    # Área CINE (Classificação Internacional Normalizada da Educação)
    "CO_CINE_ROTULO": "co_cine_rotulo",
    "NO_CINE_ROTULO": "no_cine_rotulo",
    "CO_CINE_AREA_GERAL": "co_cine_area_geral",
    "NO_CINE_AREA_GERAL": "no_cine_area_geral",
    "CO_CINE_AREA_ESPECIFICA": "co_cine_area_especifica",
    "NO_CINE_AREA_ESPECIFICA": "no_cine_area_especifica",
    "CO_CINE_AREA_DETALHADA": "co_cine_area_detalhada",
    "NO_CINE_AREA_DETALHADA": "no_cine_area_detalhada",

    # Características do curso
    "TP_GRAU_ACADEMICO": "tp_grau_academico",
    "TP_MODALIDADE_ENSINO": "tp_modalidade_ensino",
    "TP_NIVEL_ACADEMICO": "tp_nivel_academico",
    "IN_GRATUITO": "in_gratuito",

    # Classificação da IES (redundante com hist_ies, útil para joins diretos)
    "TP_ORGANIZACAO_ACADEMICA": "tp_org_academica",
    "TP_REDE": "tp_rede",
    "TP_CATEGORIA_ADMINISTRATIVA": "tp_categoria_adm",

    # Localização do curso
    "CO_REGIAO": "co_regiao",
    "NO_REGIAO": "no_regiao",
    "CO_UF": "co_uf",
    "SG_UF": "sg_uf",
    "NO_UF": "no_uf",
    "CO_MUNICIPIO": "co_municipio",
    "NO_MUNICIPIO": "no_municipio",
    "IN_CAPITAL": "in_capital",
    "TP_DIMENSAO": "tp_dimensao",

    # Vagas
    "QT_VG_TOTAL": "qt_vg_total",

    # Ingressantes — totais e por cor/raça e origem escolar
    "QT_ING": "qt_ing",
    "QT_ING_FEM": "qt_ing_fem",
    "QT_ING_MASC": "qt_ing_masc",
    "QT_ING_BRANCA": "qt_ing_branca",
    "QT_ING_PRETA": "qt_ing_preta",
    "QT_ING_PARDA": "qt_ing_parda",
    "QT_ING_AMARELA": "qt_ing_amarela",
    "QT_ING_INDIGENA": "qt_ing_indigena",
    "QT_ING_CORND": "qt_ing_cornd",
    "QT_ING_PROCESCPUBLICA": "qt_ing_esc_publica",
    "QT_ING_PROCESCPRIVADA": "qt_ing_esc_privada",
    "QT_ING_PROCNAOINFORMADA": "qt_ing_esc_nd",
    "QT_ING_RESERVA_VAGA": "qt_ing_reserva_vaga",
    "QT_ING_ENEM": "qt_ing_enem",

    # Matrículas — totais e cor/raça
    "QT_MAT": "qt_mat",
    "QT_MAT_FEM": "qt_mat_fem",
    "QT_MAT_MASC": "qt_mat_masc",
    "QT_MAT_BRANCA": "qt_mat_branca",
    "QT_MAT_PRETA": "qt_mat_preta",
    "QT_MAT_PARDA": "qt_mat_parda",
    "QT_MAT_AMARELA": "qt_mat_amarela",
    "QT_MAT_INDIGENA": "qt_mat_indigena",
    "QT_MAT_CORND": "qt_mat_cornd",

    # Concluintes — totais e cor/raça
    "QT_CONC": "qt_conc",
    "QT_CONC_FEM": "qt_conc_fem",
    "QT_CONC_MASC": "qt_conc_masc",
    "QT_CONC_BRANCA": "qt_conc_branca",
    "QT_CONC_PRETA": "qt_conc_preta",
    "QT_CONC_PARDA": "qt_conc_parda",
    "QT_CONC_AMARELA": "qt_conc_amarela",
    "QT_CONC_INDIGENA": "qt_conc_indigena",
    "QT_CONC_CORND": "qt_conc_cornd",

    # Financiamento estudantil — totais
    "QT_ING_FINANC": "qt_ing_financ",
    "QT_ING_FIES": "qt_ing_fies",
    "QT_ING_PROUNII": "qt_ing_prouni_int",
    "QT_ING_PROUNIP": "qt_ing_prouni_parc",
    "QT_MAT_FINANC": "qt_mat_financ",
    "QT_MAT_FIES": "qt_mat_fies",
    "QT_MAT_PROUNII": "qt_mat_prouni_int",
    "QT_MAT_PROUNIP": "qt_mat_prouni_parc",
    "QT_CONC_FINANC": "qt_conc_financ",

    # Apoio social e mobilidade
    "QT_MAT_APOIO_SOCIAL": "qt_mat_apoio_social",
    "QT_MAT_MOB_ACADEMICA": "qt_mat_mob_academica",

    # Deficiência
    "QT_ALUNO_DEFICIENTE": "qt_aluno_deficiente",
    "QT_MAT_DEFICIENTE": "qt_mat_deficiente",
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias no silver
# ---------------------------------------------------------------------------

CURSO_REQUIRED_COLUMNS: list[str] = [
    "ano",
    "co_ies",
    "co_curso",
    "no_curso",
    "tp_grau_academico",
    "tp_modalidade_ensino",
    "sg_uf",
    "qt_mat",
    "qt_ing",
    "qt_conc",
]

# ---------------------------------------------------------------------------
# Tipos canônicos do silver
# ---------------------------------------------------------------------------

CURSO_SILVER_SCHEMA: dict[str, pl.DataType] = {
    "ano": pl.Int32,
    "co_ies": pl.Int64,
    "co_curso": pl.Int64,
    "co_cine_rotulo": pl.String,
    "co_cine_area_geral": pl.String,
    "co_cine_area_especifica": pl.String,
    "co_cine_area_detalhada": pl.String,
    "tp_grau_academico": pl.Int32,
    "tp_modalidade_ensino": pl.Int32,
    "tp_nivel_academico": pl.Int32,
    "in_gratuito": pl.Int32,
    "tp_org_academica": pl.Int32,
    "tp_rede": pl.Int32,
    "tp_categoria_adm": pl.Int32,
    "co_regiao": pl.Int32,
    "co_uf": pl.Int32,
    "co_municipio": pl.Int64,
    "in_capital": pl.Int32,
    "tp_dimensao": pl.Int32,
    "qt_vg_total": pl.Int32,
    "qt_ing": pl.Int32,
    "qt_ing_fem": pl.Int32,
    "qt_ing_masc": pl.Int32,
    "qt_ing_branca": pl.Int32,
    "qt_ing_preta": pl.Int32,
    "qt_ing_parda": pl.Int32,
    "qt_ing_amarela": pl.Int32,
    "qt_ing_indigena": pl.Int32,
    "qt_ing_cornd": pl.Int32,
    "qt_ing_esc_publica": pl.Int32,
    "qt_ing_esc_privada": pl.Int32,
    "qt_ing_esc_nd": pl.Int32,
    "qt_ing_reserva_vaga": pl.Int32,
    "qt_ing_enem": pl.Int32,
    "qt_mat": pl.Int32,
    "qt_mat_fem": pl.Int32,
    "qt_mat_masc": pl.Int32,
    "qt_mat_branca": pl.Int32,
    "qt_mat_preta": pl.Int32,
    "qt_mat_parda": pl.Int32,
    "qt_mat_amarela": pl.Int32,
    "qt_mat_indigena": pl.Int32,
    "qt_mat_cornd": pl.Int32,
    "qt_conc": pl.Int32,
    "qt_conc_fem": pl.Int32,
    "qt_conc_masc": pl.Int32,
    "qt_conc_branca": pl.Int32,
    "qt_conc_preta": pl.Int32,
    "qt_conc_parda": pl.Int32,
    "qt_conc_amarela": pl.Int32,
    "qt_conc_indigena": pl.Int32,
    "qt_conc_cornd": pl.Int32,
    "qt_ing_financ": pl.Int32,
    "qt_ing_fies": pl.Int32,
    "qt_ing_prouni_int": pl.Int32,
    "qt_ing_prouni_parc": pl.Int32,
    "qt_mat_financ": pl.Int32,
    "qt_mat_fies": pl.Int32,
    "qt_mat_prouni_int": pl.Int32,
    "qt_mat_prouni_parc": pl.Int32,
    "qt_conc_financ": pl.Int32,
    "qt_mat_apoio_social": pl.Int32,
    "qt_mat_mob_academica": pl.Int32,
    "qt_aluno_deficiente": pl.Int32,
    "qt_mat_deficiente": pl.Int32,
}

# ---------------------------------------------------------------------------
# Decodificadores — baseados no dicionário oficial
# ---------------------------------------------------------------------------

GRAU_ACADEMICO: dict[int, str] = {
    1: "Bacharelado",
    2: "Licenciatura",
    3: "Tecnológico",
    4: "Bacharelado e Licenciatura",
}

MODALIDADE_ENSINO: dict[int, str] = {
    1: "Presencial",
    2: "EaD",
}

NIVEL_ACADEMICO: dict[int, str] = {
    1: "Graduação",
    2: "Sequencial de Formação Específica",
}
