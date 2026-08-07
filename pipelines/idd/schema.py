"""
pipelines/idd/schema.py — Schema do IDD (Indicador de Diferença entre Desempenhos).

Baseado na inspeção direta das planilhas XLSX do INEP (2016–2023).
Não há dicionário de dados separado para este indicador.

Granularidade: uma linha por (co_ies, co_curso, ano).
Chave de join com dim_ies: co_ies.

Variações históricas confirmadas via inspeção dos arquivos reais:

  Ano  | Aba              | Colunas especiais
  -----|------------------|----------------------------------------------------------
  2016 | Plan1            | 'Área de Enquadramento' (≠ 'Área de Avaliação')
       |                  | 'Concluintes Inscritos' / 'Concluintes Participantes'
       |                  | 'Concluintes Participantes com nota no Enem'
       |                  | 'Percentual de Concluintes participantes com nota no Enem'
       |                  | 'Nota Padronizada - IDD' (≠ 'IDD (Contínuo)')
       |                  | Sem 'Código da Área' (coluna inexistente em 2016)
  2017 | IDD 2017         | 'Nº de ...' prefix; coluna 'Observação' extra
  2018 | IDD_2018         | igual 2017
  2019 | IDD2019          | igual 2017, sem 'Observação'
  2021 | PLANILHA_IDD     | 'Grau Acadêmico', 'Nome da IES*', 'Sigla da IES*'
       |                  | 'Código do Município**', 'Município do Curso**', 'Sigla da UF**'
       |                  | 'Entidade Beneficiente de Assistência Social (CEBAS)'
       |                  | 'Observação'
  2022 | IDD 2022         | 'Entidade Beneficiente de Assistência Social (CEBAS)'
  2023 | IDD_2023         | igual 2022

  Em todos os anos (2017–2023), a coluna de participantes no IDD se chama:
    'Nº de Concluintes Participantes com nota no Enem'
  Em 2016:
    'Concluintes Participantes com nota no Enem'
"""

import polars as pl

# ---------------------------------------------------------------------------
# Mapeamento: nome original (após strip) → nome canônico
# Todas as variantes históricas conhecidas estão aqui.
# ---------------------------------------------------------------------------

IDD_COLUMN_MAP: dict[str, str] = {
    # --- Ano ---
    "Ano": "ano",

    # --- Área ---
    "Código da Área": "co_area",
    "Área de Avaliação": "area_avaliacao",
    "Área de Enquadramento": "area_avaliacao",       # 2016: nome alternativo

    # --- Grau (apenas 2021) ---
    "Grau Acadêmico": "grau_academico",

    # --- IES ---
    "Código da IES": "co_ies",
    "Nome da IES": "no_ies",
    "Nome da IES*": "no_ies",                        # 2021: asterisco de rodapé
    "Sigla da IES": "sg_ies",
    "Sigla da IES*": "sg_ies",                       # 2021

    # --- Classificação da IES ---
    "Organização Acadêmica": "org_academica",
    "Categoria Administrativa": "categoria_adm",

    # --- Curso ---
    "Código do Curso": "co_curso",
    "Modalidade de Ensino": "modalidade",

    # --- Localização ---
    "Código do Município": "co_municipio",
    "Código do Município**": "co_municipio",         # 2021
    "Município do Curso": "no_municipio",
    "Município do Curso**": "no_municipio",          # 2021
    "Sigla da UF": "sg_uf",
    "Sigla da UF**": "sg_uf",                        # 2021
    "Sigla da UF** ": "sg_uf",                       # 2021 com espaço trailing

    # --- Quantitativos de alunos ---
    # 2017–2023
    "Nº de Concluintes Inscritos": "qt_inscritos",
    "Nº de Concluintes Participantes": "qt_participantes",
    "Nº de Concluintes Participantes com nota no Enem": "qt_participantes_idd",
    "Proporção de Concluintes participantes com nota no Enem": "prop_participantes_idd",
    # 2016: sem prefixo "Nº de"
    "Concluintes Inscritos": "qt_inscritos",
    "Concluintes Participantes": "qt_participantes",
    "Concluintes Participantes com nota no Enem": "qt_participantes_idd",
    "Percentual de Concluintes participantes com nota no Enem": "prop_participantes_idd",

    # --- Métricas IDD ---
    "Nota Bruta - IDD": "nota_bruta_idd",
    "Nota Padronizada - IDD": "idd_continuo",        # 2016: nome antigo
    "IDD (Contínuo)": "idd_continuo",               # 2017–2023
    "IDD (Faixa)": "idd_faixa",

    # --- Colunas extras ---
    # observacao: quando preenchida, indica que o IDD não pôde ser calculado
    # estatisticamente (ex.: "Curso para o qual estatisticamente não foi possível
    # calcular o indicador"). Cursos com observação têm idd_continuo e idd_faixa nulos.
    "Observação": "observacao",                      # 2017, 2018, 2021

    # in_cebas: Entidade Beneficente de Assistência Social.
    # No XLSX: "X" = é CEBAS, "-" = não é CEBAS. Convertido para Boolean no silver.
    "Entidade Beneficiente de Assistência Social (CEBAS)": "in_cebas",  # 2021–2023
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias no silver
# ---------------------------------------------------------------------------

IDD_REQUIRED_COLUMNS: list[str] = [
    "ano",
    "co_ies",
    "co_curso",
    "area_avaliacao",
    "nota_bruta_idd",
    "idd_continuo",
    "idd_faixa",
]

# ---------------------------------------------------------------------------
# Tipos canônicos do silver
# ---------------------------------------------------------------------------

IDD_SILVER_SCHEMA: dict[str, pl.DataType] = {
    "ano": pl.Int32,
    "co_area": pl.Int64,
    "co_ies": pl.Int64,
    "co_curso": pl.Int64,
    "co_municipio": pl.Int64,
    "qt_inscritos": pl.Int32,
    "qt_participantes": pl.Int32,
    "qt_participantes_idd": pl.Int32,
    "prop_participantes_idd": pl.Float64,
    "nota_bruta_idd": pl.Float64,
    "idd_continuo": pl.Float64,
    "idd_faixa": pl.Int32,
    "in_cebas": pl.Boolean,
}
