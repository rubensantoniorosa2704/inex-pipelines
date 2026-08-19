"""
pipelines/igc/schema.py — Schema do IGC (Índice Geral de Cursos).

Baseado na inspeção direta das planilhas do INEP (2017–2023).
2020 não existe (pandemia — ENADE não aplicado).

Granularidade: uma linha por (co_ies, ano).
Chave de join com dim_ies: co_ies.

## Anos cobertos: 2017–2023

O INEP publica o IGC desde 2007, mas os anos anteriores a 2017 apresentam
incompatibilidades que tornam o processamento automatizado frágil:

  - 2007: arquivo .7z com layout desconhecido
  - 2008: layout mínimo (10 colunas), sem proporções alfa/beta/gama nem
    conceitos separados de mestrado/doutorado
  - 2009: header na segunda linha, colunas vazias no header real
  - 2010–2016: dados distribuídos em múltiplas sheets por organização
    acadêmica (Universidades, Centros Universitários, Faculdades),
    exigindo concatenação; nomes de colunas muito variados entre anos

A partir de 2017 o INEP unifica todos os dados em uma sheet única com
layout consistente (~15–18 colunas). Os ganhos de cobertura de 2008–2016
não compensam a fragilidade e o risco de erros silenciosos.

Variações históricas (2017–2023):

  2017       : sem coluna 'Ano' (inferido); 'IGC (Faixa)'; sem CEBAS
  2018       : tem 'Ano'; 'IGC (Faixa)'; 'TX_OBS (Observação)'; sem CEBAS
  2019       : tem 'Ano'; 'IGC (Faixa)'; 'TX_OBS'; sem CEBAS
  2021       : asteriscos nos nomes (ex: 'Código da IES*'); tem CEBAS
  2022       : sem asteriscos; '(Observação)'; tem CEBAS
  2023       : sem asteriscos; sem TX_OBS; tem CEBAS
"""

import polars as pl

# ---------------------------------------------------------------------------
# Mapeamento: nome original (após strip) → nome canônico
# Cobre todas as variações de 2017 a 2023.
# ---------------------------------------------------------------------------

IGC_COLUMN_MAP: dict[str, str] = {
    # --- Ano ---
    "Ano": "ano",

    # --- Identificação da IES ---
    "Código da IES": "co_ies",
    "Código da IES*": "co_ies",                      # 2021

    "Nome da IES": "no_ies",
    "Nome da IES*": "no_ies",                        # 2021

    "Sigla da IES": "sg_ies",
    "Sigla da IES*": "sg_ies",                       # 2021

    # --- Organização e categoria ---
    "Organização Acadêmica": "org_academica",
    "Organização Acadêmica*": "org_academica",       # 2021
    "Org. Acadêmica": "org_academica",               # 2017

    "Categoria Administrativa": "categoria_adm",
    "Categ. Administrativa": "categoria_adm",        # 2017

    # --- Localização ---
    "Sigla da UF": "sg_uf",
    "UF da IES": "sg_uf",                            # 2017

    # --- Cursos avaliados ---
    "Nº de Cursos com CPC no triênio": "qt_cursos_cpc",
    "Nº de Cursos com CPC no Triênio": "qt_cursos_cpc",  # 2017

    # --- Proporções (pesos da fórmula do IGC) ---
    "Alfa (Proporção de Graduação)": "alfa",
    "alfa (Proporção de Graduandos)": "alfa",        # 2017

    "Beta (Proporção de Mestrado - Equivalente)": "beta",
    "beta (Proporção de Mestrandos - Equivalente)": "beta",  # 2017

    "Gama (Proporção de Doutorandos – Equivalente)": "gama",
    "Gama (Proporção de Doutorandos - Equivalente)": "gama",  # variação de travessão
    "gama (Proporção de Doutorandos - Equivalente)": "gama",  # 2017

    # --- Conceitos médios ---
    "Conceito Médio de Graduação": "conceito_graduacao",
    "Conceito médio da Graduação": "conceito_graduacao",  # 2017

    "Conceito Médio de Mestrado": "conceito_mestrado",
    "Conceito Médio do Mestrado": "conceito_mestrado",    # 2017

    "Conceito Médio do doutorado": "conceito_doutorado",

    # --- IGC ---
    "IGC (Contínuo)": "igc_continuo",
    "IGC (Faixa)": "igc_faixa",

    # --- Observação (descartada) ---
    "TX_OBS": "_obs",
    "TX_OBS (Observação)": "_obs",
    "(Observação)": "_obs",
    "Observação": "_obs",

    # --- CEBAS ---
    "Entidade Beneficiente de Assistência Social (CEBAS)": "in_cebas",
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias no silver (presentes em todos os anos 2017–2023)
# ---------------------------------------------------------------------------

IGC_REQUIRED_COLUMNS: list[str] = [
    "ano",
    "co_ies",
    "no_ies",
    "sg_uf",
    "org_academica",
    "categoria_adm",
    "qt_cursos_cpc",
    "alfa",
    "conceito_graduacao",
    "beta",
    "conceito_mestrado",
    "conceito_doutorado",
    "igc_continuo",
    "igc_faixa",
]

# ---------------------------------------------------------------------------
# Tipos canônicos do silver
# ---------------------------------------------------------------------------

IGC_SILVER_SCHEMA: dict[str, pl.DataType] = {
    "ano": pl.Int32,
    "co_ies": pl.Int64,
    "qt_cursos_cpc": pl.Int32,
    "alfa": pl.Float64,
    "conceito_graduacao": pl.Float64,
    "beta": pl.Float64,
    "conceito_mestrado": pl.Float64,
    "gama": pl.Float64,
    "conceito_doutorado": pl.Float64,
    "igc_continuo": pl.Float64,
    "igc_faixa": pl.Int32,
    "in_cebas": pl.Int32,
}
