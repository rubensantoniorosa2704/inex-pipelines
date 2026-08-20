"""
pipelines/enade/schema_microdados.py — Schema dos Microdados do ENADE.

Baseado nos dicionários de variáveis oficiais do INEP (2004–2023).
Cobertura: 2004–2023 (exceto 2020 — pandemia).

## Estrutura dos arquivos bronze

Os microdados estão divididos em N arquivos por ano, todos CSVs com
separador ';' e encoding latin-1. Os relevantes para este pipeline:

  arq1 — metadados do curso: CO_IES, CO_CATEGAD, CO_ORGACAD, CO_GRUPO,
         CO_MODALIDADE, CO_MUNIC_CURSO, CO_UF_CURSO, CO_REGIAO_CURSO.
         Uma linha por estudante inscrito, mas dados são de curso (repetidos).

  arq3 — desempenho dos estudantes: notas, presença, vetores de gabarito/
         escolha/acerto. Uma linha por estudante inscrito.

## Relação entre arq1 e arq3

Os arquivos NÃO são row-aligned (linhas embaralhadas independentemente, LGPD).
O join é many-to-one: arq3 (por estudante) LEFT JOIN arq1 deduplicado (por CO_CURSO).
CO_CURSO é a chave — metadados do curso são idênticos para todos os alunos.

## Granularidade do silver

Uma linha por estudante inscrito, com metadados do curso + notas/vetores.
Sem chave única por estudante (dados anônimos).

## Variações históricas

  2004-2008: arq3 inclui PESO_AMOSTRA e TP_INSCRICAO como colunas-chave;
             NT_CE_D4..D18 variável; sem NU_ITEM_*_Z/X/N.
  2009:      sem PESO_AMOSTRA; ainda tem TP_INSCRICAO e NU_ITEM_DIFG/DICE.
  2010-2012: layout limpo; sem PESO_AMOSTRA/TP_INSCRICAO; 42 cols.
  2013:      adiciona NT_FG_D1_PT/CT e NT_FG_D2_PT/CT.
  2014-2022: layout estável (52 cols): NU_ITEM_*_Z/X/N + notas detalhadas.
  2023:      reduzido (44 cols): apenas 1 questão discursiva por seção.

O silver usa pl.concat(..., how="diagonal") — colunas ausentes ficam null.
"""

import polars as pl

# ---------------------------------------------------------------------------
# Mapeamento: arq1 — metadados do curso
# Colunas presentes em arq1 para todos os anos (com variações).
# ---------------------------------------------------------------------------

ARQ1_COLUMN_MAP: dict[str, str] = {
    "NU_ANO": "ano",
    "CO_CURSO": "co_curso",
    "CO_IES": "co_ies",
    "CO_CATEGAD": "tp_categoria_adm",
    "CO_ORGACAD": "tp_org_academica",
    "CO_GRUPO": "co_grupo",
    "CO_MODALIDADE": "tp_modalidade",       # 2010+
    "CO_MUNIC_CURSO": "co_municipio",
    "CO_UF_CURSO": "co_uf",
    "CO_REGIAO_CURSO": "co_regiao",
    "CO_REGIÃO_CURSO": "co_regiao",         # variação com acento (2007, 2010-2022)
}

# ---------------------------------------------------------------------------
# Mapeamento: arq3 — desempenho (notas, presença, vetores)
# Inclui todas as colunas que existem em algum ano.
# Colunas ausentes em um ano simplesmente não são selecionadas.
# ---------------------------------------------------------------------------

ARQ3_COLUMN_MAP: dict[str, str] = {
    "NU_ANO": "ano",
    "CO_CURSO": "co_curso",

    # --- Presença ---
    "TP_PRES": "tp_pres",
    "TP_PR_GER": "tp_pr_ger",
    "TP_PR_OB_FG": "tp_pr_ob_fg",
    "TP_PR_DI_FG": "tp_pr_di_fg",
    "TP_PR_OB_CE": "tp_pr_ob_ce",
    "TP_PR_DI_CE": "tp_pr_di_ce",

    # --- Contagem de itens válidos ---
    "NU_ITEM_OFG": "nu_item_ofg",
    "NU_ITEM_OFG_Z": "nu_item_ofg_z",       # 2014+: anulados
    "NU_ITEM_OFG_X": "nu_item_ofg_x",       # 2014+: excluídos
    "NU_ITEM_OFG_N": "nu_item_ofg_n",       # 2014+: não respondidos
    "NU_ITEM_OCE": "nu_item_oce",
    "NU_ITEM_OCE_Z": "nu_item_oce_z",       # 2014+
    "NU_ITEM_OCE_X": "nu_item_oce_x",       # 2014+
    "NU_ITEM_OCE_N": "nu_item_oce_n",       # 2014+

    # --- Vetores (strings de caracteres, posição = questão) ---
    "DS_VT_GAB_OFG_FIN": "vt_gab_ofg",      # gabarito final FG
    "DS_VT_GAB_OCE_FIN": "vt_gab_oce",      # gabarito final CE
    "DS_VT_ESC_OFG": "vt_esc_ofg",          # escolha do aluno FG
    "DS_VT_ACE_OFG": "vt_ace_ofg",          # acertos FG
    "DS_VT_ESC_OCE": "vt_esc_oce",          # escolha do aluno CE
    "DS_VT_ACE_OCE": "vt_ace_oce",          # acertos CE

    # --- Notas ---
    "NT_GER": "nt_ger",                      # nota bruta geral
    "NT_FG": "nt_fg",                        # nota formação geral
    "NT_OBJ_FG": "nt_obj_fg",               # nota objetiva FG
    "NT_DIS_FG": "nt_dis_fg",               # nota discursiva FG
    "NT_CE": "nt_ce",                        # nota componente específico
    "NT_OBJ_CE": "nt_obj_ce",               # nota objetiva CE
    "NT_DIS_CE": "nt_dis_ce",               # nota discursiva CE

    # --- Notas por questão discursiva ---
    "NT_FG_D1": "nt_fg_d1",                 # discursiva FG questão 1
    "NT_FG_D1_PT": "nt_fg_d1_pt",           # 2013+: língua portuguesa
    "NT_FG_D1_CT": "nt_fg_d1_ct",           # 2013+: conteúdo
    "NT_FG_D2": "nt_fg_d2",                 # discursiva FG questão 2 (até 2022)
    "NT_FG_D2_PT": "nt_fg_d2_pt",           # 2013-2022
    "NT_FG_D2_CT": "nt_fg_d2_ct",           # 2013-2022
    "NT_CE_D1": "nt_ce_d1",                 # discursiva CE questão 1
    "NT_CE_D2": "nt_ce_d2",                 # discursiva CE questão 2 (até 2022)
    "NT_CE_D3": "nt_ce_d3",                 # discursiva CE questão 3 (até 2022)

    # --- Situação das questões discursivas ---
    "TP_SFG_D1": "tp_sfg_d1",
    "TP_SFG_D2": "tp_sfg_d2",               # até 2022
    "TP_SCE_D1": "tp_sce_d1",
    "TP_SCE_D2": "tp_sce_d2",               # até 2022
    "TP_SCE_D3": "tp_sce_d3",               # até 2022

    # --- Percepção da prova ---
    "CO_RS_I1": "co_rs_i1",
    "CO_RS_I2": "co_rs_i2",
    "CO_RS_I3": "co_rs_i3",
    "CO_RS_I4": "co_rs_i4",
    "CO_RS_I5": "co_rs_i5",
    "CO_RS_I6": "co_rs_i6",
    "CO_RS_I7": "co_rs_i7",
    "CO_RS_I8": "co_rs_i8",
    "CO_RS_I9": "co_rs_i9",
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias no silver (devem existir em TODOS os anos)
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS: list[str] = [
    "ano",
    "co_ies",
    "co_curso",
    "co_grupo",
    "tp_pres",
    "nt_ger",
]

# ---------------------------------------------------------------------------
# Tipos canônicos do silver
# ---------------------------------------------------------------------------

SILVER_SCHEMA: dict[str, pl.DataType] = {
    # --- Identificação ---
    "ano": pl.Int32,
    "co_ies": pl.Int64,
    "co_curso": pl.Int64,
    "tp_categoria_adm": pl.Int32,
    "tp_org_academica": pl.Int32,
    "co_grupo": pl.Int32,
    "tp_modalidade": pl.Int32,
    "co_municipio": pl.Int64,
    "co_uf": pl.Int32,
    "co_regiao": pl.Int32,

    # --- Presença ---
    "tp_pres": pl.Int32,
    "tp_pr_ger": pl.Int32,
    "tp_pr_ob_fg": pl.Int32,
    "tp_pr_di_fg": pl.Int32,
    "tp_pr_ob_ce": pl.Int32,
    "tp_pr_di_ce": pl.Int32,

    # --- Contagem de itens ---
    "nu_item_ofg": pl.Int32,
    "nu_item_ofg_z": pl.Int32,
    "nu_item_ofg_x": pl.Int32,
    "nu_item_ofg_n": pl.Int32,
    "nu_item_oce": pl.Int32,
    "nu_item_oce_z": pl.Int32,
    "nu_item_oce_x": pl.Int32,
    "nu_item_oce_n": pl.Int32,

    # --- Notas (0-100, uma casa decimal) ---
    "nt_ger": pl.Float32,
    "nt_fg": pl.Float32,
    "nt_obj_fg": pl.Float32,
    "nt_dis_fg": pl.Float32,
    "nt_ce": pl.Float32,
    "nt_obj_ce": pl.Float32,
    "nt_dis_ce": pl.Float32,
    "nt_fg_d1": pl.Float32,
    "nt_fg_d1_pt": pl.Float32,
    "nt_fg_d1_ct": pl.Float32,
    "nt_fg_d2": pl.Float32,
    "nt_fg_d2_pt": pl.Float32,
    "nt_fg_d2_ct": pl.Float32,
    "nt_ce_d1": pl.Float32,
    "nt_ce_d2": pl.Float32,
    "nt_ce_d3": pl.Float32,

    # --- Situação discursivas ---
    "tp_sfg_d1": pl.Int32,
    "tp_sfg_d2": pl.Int32,
    "tp_sce_d1": pl.Int32,
    "tp_sce_d2": pl.Int32,
    "tp_sce_d3": pl.Int32,
}

# Vetores permanecem como Utf8 (String) — serão explodidos em pipeline separado.
# CO_RS_I* permanecem como Utf8 — categorias de resposta (A-E).
