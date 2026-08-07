"""
pipelines/cpc/schema.py — Schema do CPC (Conceito Preliminar de Curso).

Baseado na inspeção direta das planilhas XLSX do INEP (2021–2023).
O CPC é calculado nos anos de aplicação do ENADE (ciclos trienais).

Granularidade: uma linha por (co_ies, co_curso, ano).
Chave de join com dim_ies: co_ies.

Variações históricas conhecidas (2021–2023):
  - 2021: aba 'CPC2021'; colunas com asteriscos ('Código da IES*', 'Código do Curso**');
          coluna 'Grau acadêmico' presente; ordem das colunas diferente.
  - 2022: aba 'CPC 2022'; colunas sem asteriscos; ordem reorganizada.
  - 2023: aba 'CPC_2023'; mesmo layout do 2022.
  - Em todos os anos as colunas têm espaços no início/fim (strip necessário).
"""

import polars as pl

# ---------------------------------------------------------------------------
# Mapeamento: nome original (após strip) → nome canônico
# ---------------------------------------------------------------------------

CPC_COLUMN_MAP: dict[str, str] = {
    "Ano": "ano",
    # Identificação do curso
    "Código da Área": "co_area",
    "Área de Avaliação": "area_avaliacao",
    "Grau acadêmico": "grau_academico",          # apenas 2021
    "Modalidade de Ensino": "modalidade",
    # Identificação da IES
    "Código da IES": "co_ies",
    "Código da IES*": "co_ies",                  # 2021
    "Nome da IES": "no_ies",
    "Nome da IES*": "no_ies",                    # 2021
    "Sigla da IES": "sg_ies",
    "Sigla da IES*": "sg_ies",                   # 2021
    "Organização Acadêmica": "org_academica",
    "Organização Acadêmica*": "org_academica",   # 2021
    "Categoria Administrativa": "categoria_adm",
    "Categoria Administrativa*": "categoria_adm", # 2021
    # Identificação do curso
    "Código do Curso": "co_curso",
    "Código do Curso**": "co_curso",             # 2021
    # Localização
    "Código do Município": "co_municipio",
    "Código do Município***": "co_municipio",    # 2021
    "Município do Curso": "no_municipio",
    "Município do Curso***": "no_municipio",     # 2021
    "Sigla da UF": "sg_uf",
    "Sigla da UF**": "sg_uf",                    # 2021
    "Sigla da UF** ": "sg_uf",                   # 2021 (com espaço extra)
    # Participação
    "Nº de Concluintes Inscritos": "qt_inscritos",
    "Nº de Concluintes Participantes": "qt_participantes",
    "Concluintes participantes com nota no Enem": "qt_participantes_idd",
    "Proporção de concluintes participantes com nota no Enem": "prop_participantes_idd",
    # Notas ENADE
    "Nota Bruta - FG": "nota_bruta_fg",
    "Nota Padronizada - FG": "nota_pad_fg",
    "Nota Bruta - CE": "nota_bruta_ce",
    "Nota Padronizada - CE": "nota_pad_ce",
    "Conceito Enade (Contínuo)": "enade_continuo",
    # Notas IDD
    "Nota Bruta - IDD": "nota_bruta_idd",
    "Nota Padronizada - IDD": "nota_pad_idd",
    # Corpo docente
    "Nota Bruta - Mestres": "nota_bruta_mestres",
    "Nota Padronizada - Mestres": "nota_pad_mestres",
    "Nota Bruta - Doutores": "nota_bruta_doutores",
    "Nota Padronizada - Doutores": "nota_pad_doutores",
    "Nota Bruta – Regime de Trabalho": "nota_bruta_regime",
    "Nota Padronizada - Regime de Trabalho": "nota_pad_regime",
    # Percepção discente (questionário)
    "Nota Bruta – Organização Didático-Pedagógica": "nota_bruta_org_didatica",
    "Nota Padronizada - Organização Didático-Pedagógica": "nota_pad_org_didatica",
    "Nota Bruta – Infraestrutura e Instalações Físicas": "nota_bruta_infra",
    "Nota Padronizada - Infraestrutura e Instalações Físicas": "nota_pad_infra",
    "Nota Bruta – Oportunidade de Ampliação da Formação": "nota_bruta_oportunidade",
    "Nota Padronizada - Oportunidade de Ampliação da Formação": "nota_pad_oportunidade",
    # CPC final
    "CPC (Contínuo)": "cpc_continuo",
    "CPC (Faixa)": "cpc_faixa",
    # Beneficência
    "Entidade Beneficiente de Assistência Social (CEBAS)": "in_cebas",
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias no silver
# ---------------------------------------------------------------------------

CPC_REQUIRED_COLUMNS: list[str] = [
    "ano",
    "co_ies",
    "co_curso",
    "area_avaliacao",
    "enade_continuo",
    "cpc_continuo",
    "cpc_faixa",
]

# ---------------------------------------------------------------------------
# Tipos canônicos do silver
# ---------------------------------------------------------------------------

CPC_SILVER_SCHEMA: dict[str, pl.DataType] = {
    "ano": pl.Int32,
    "co_area": pl.Int64,
    "co_ies": pl.Int64,
    "co_curso": pl.Int64,
    "co_municipio": pl.Int64,
    "qt_inscritos": pl.Int32,
    "qt_participantes": pl.Int32,
    "qt_participantes_idd": pl.Int32,
    "prop_participantes_idd": pl.Float64,
    "nota_bruta_fg": pl.Float64,
    "nota_pad_fg": pl.Float64,
    "nota_bruta_ce": pl.Float64,
    "nota_pad_ce": pl.Float64,
    "enade_continuo": pl.Float64,
    "nota_bruta_idd": pl.Float64,
    "nota_pad_idd": pl.Float64,
    "nota_bruta_mestres": pl.Float64,
    "nota_pad_mestres": pl.Float64,
    "nota_bruta_doutores": pl.Float64,
    "nota_pad_doutores": pl.Float64,
    "nota_bruta_regime": pl.Float64,
    "nota_pad_regime": pl.Float64,
    "nota_bruta_org_didatica": pl.Float64,
    "nota_pad_org_didatica": pl.Float64,
    "nota_bruta_infra": pl.Float64,
    "nota_pad_infra": pl.Float64,
    "nota_bruta_oportunidade": pl.Float64,
    "nota_pad_oportunidade": pl.Float64,
    "cpc_continuo": pl.Float64,
    "cpc_faixa": pl.Int32,
    "in_cebas": pl.Int32,
}
