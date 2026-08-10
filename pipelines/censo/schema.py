"""
pipelines/censo/schema.py — Schema do Censo da Educação Superior (arquivo IES).

Baseado na inspeção direta das planilhas do INEP (2009–2024) e no dicionário 
de dados oficial:
  Anexos/ANEXO I - Dicionário de Dados/dicionário_dados_educação_superior.xlsx
  Aba: cadastro_ies

Granularidade: uma linha por (co_ies, ano).
Chave de join padrão: co_ies.

## Limitações conhecidas

- 2009: nomenclatura diferente para docentes (sem prefixo "QT_")
- 2023+: tp_rede passa a existir como campo próprio; anos anteriores 
  devem derivar de tp_categoria_adm (≤3 = Pública, >3 = Privada)

Variações históricas por período:

  2009      : DOC_EX_* (sem QT_), QT_DOCENTE_* (nomes alternativos)  
  2010–2022 : QT_DOC_EX_* (padrão), campos endereço detalhado
  2021      : CO_LOCAL_OFERTA, CO_PROJETO, NO_LOCAL_OFERTA (metadados)
  2023–2024 : IN_COMUNITARIA, IN_CONFESSIONAL (categorias adicionais)

Cobertura validada: 16 anos (2009–2024) com 100% de mapeamento.
"""

import polars as pl

# ---------------------------------------------------------------------------
# Mapeamento de colunas: nome original INEP → nome canônico
# Colunas usadas para dim_ies e hist_ies (identidade e classificação da IES)
# ---------------------------------------------------------------------------

IES_COLUMN_MAP: dict[str, str] = {
    "NU_ANO_CENSO": "ano",
    # Localização
    "CO_REGIAO_IES": "co_regiao",
    "NO_REGIAO_IES": "no_regiao",
    "CO_UF_IES": "co_uf",
    "SG_UF_IES": "sg_uf",
    "NO_UF_IES": "no_uf",
    "CO_MUNICIPIO_IES": "co_municipio",
    "NO_MUNICIPIO_IES": "no_municipio",
    "IN_CAPITAL_IES": "in_capital",
    "CO_MESORREGIAO_IES": "co_mesorregiao",
    "NO_MESORREGIAO_IES": "no_mesorregiao",
    "CO_MICRORREGIAO_IES": "co_microrregiao",
    "NO_MICRORREGIAO_IES": "no_microrregiao",
    # Classificação
    "TP_ORGANIZACAO_ACADEMICA": "tp_org_academica",
    "TP_REDE": "tp_rede",
    "TP_CATEGORIA_ADMINISTRATIVA": "tp_categoria_adm",
    # Mantenedora
    "CO_MANTENEDORA": "co_mantenedora",
    "NO_MANTENEDORA": "no_mantenedora",
    # Identidade da IES
    "CO_IES": "co_ies",
    "NO_IES": "no_ies",
    "SG_IES": "sg_ies",
    
    # Endereço detalhado (a partir de 2010) - campos descartáveis
    "DS_ENDERECO_IES": "_endereco_ies",                       # muito detalhado
    "DS_NUMERO_ENDERECO_IES": "_numero_endereco_ies",         # não essencial
    "DS_COMPLEMENTO_ENDERECO_IES": "_complemento_endereco_ies", # não essencial
    "NO_BAIRRO_IES": "_bairro_ies",                          # não essencial
    "NU_CEP_IES": "_cep_ies",                                # não essencial
    
    # Campos adicionais recentes
    "CO_LOCAL_OFERTA": "_co_local_oferta",                    # 2021: metadado
    "CO_PROJETO": "_co_projeto",                              # 2021: metadado  
    "NO_LOCAL_OFERTA": "_no_local_oferta",                    # 2021: metadado
    "IN_COMUNITARIA": "in_comunitaria",                       # 2023+: indicador categoria
    "IN_CONFESSIONAL": "in_confessional",                     # 2023+: indicador categoria
}

# ---------------------------------------------------------------------------
# Mapeamento de colunas métricas: ficam em fact_censo_ies, não na dimensão
# ---------------------------------------------------------------------------

FACT_COLUMN_MAP: dict[str, str] = {
    "NU_ANO_CENSO": "ano",
    "CO_IES": "co_ies",
    
    # Técnico-administrativos
    "QT_TEC_TOTAL": "qt_tec_total",
    "QT_TEC_FUNDAMENTAL_INCOMP_FEM": "qt_tec_fund_incomp_fem",
    "QT_TEC_FUNDAMENTAL_INCOMP_MASC": "qt_tec_fund_incomp_masc",
    "QT_TEC_FUNDAMENTAL_COMP_FEM": "qt_tec_fund_comp_fem",
    "QT_TEC_FUNDAMENTAL_COMP_MASC": "qt_tec_fund_comp_masc",
    "QT_TEC_MEDIO_FEM": "qt_tec_medio_fem",
    "QT_TEC_MEDIO_MASC": "qt_tec_medio_masc",
    "QT_TEC_SUPERIOR_FEM": "qt_tec_superior_fem",
    "QT_TEC_SUPERIOR_MASC": "qt_tec_superior_masc",
    "QT_TEC_ESPECIALIZACAO_FEM": "qt_tec_esp_fem",
    "QT_TEC_ESPECIALIZACAO_MASC": "qt_tec_esp_masc",
    "QT_TEC_MESTRADO_FEM": "qt_tec_mest_fem",
    "QT_TEC_MESTRADO_MASC": "qt_tec_mest_masc",
    "QT_TEC_DOUTORADO_FEM": "qt_tec_dout_fem",
    "QT_TEC_DOUTORADO_MASC": "qt_tec_dout_masc",
    
    # Docentes - nomes padrão (2010+)
    "QT_DOC_TOTAL": "qt_doc_total",
    "QT_DOC_EXE": "qt_doc_exe",
    "QT_DOC_EX_FEMI": "qt_doc_exe_fem",
    "QT_DOC_EX_MASC": "qt_doc_exe_masc",
    "QT_DOC_EX_SEM_GRAD": "qt_doc_exe_sem_grad",
    "QT_DOC_EX_GRAD": "qt_doc_exe_grad",
    "QT_DOC_EX_ESP": "qt_doc_exe_esp",
    "QT_DOC_EX_MEST": "qt_doc_exe_mest",
    "QT_DOC_EX_DOUT": "qt_doc_exe_dout",
    "QT_DOC_EX_INT": "qt_doc_exe_int",
    "QT_DOC_EX_INT_DE": "qt_doc_exe_int_de",
    "QT_DOC_EX_INT_SEM_DE": "qt_doc_exe_int_sem_de",
    "QT_DOC_EX_PARC": "qt_doc_exe_parc",
    "QT_DOC_EX_HOR": "qt_doc_exe_hor",
    
    # Docentes por faixa etária
    "QT_DOC_EX_0_29": "qt_doc_exe_0_29",
    "QT_DOC_EX_30_34": "qt_doc_exe_30_34",
    "QT_DOC_EX_35_39": "qt_doc_exe_35_39",
    "QT_DOC_EX_40_44": "qt_doc_exe_40_44",
    "QT_DOC_EX_45_49": "qt_doc_exe_45_49",
    "QT_DOC_EX_50_54": "qt_doc_exe_50_54",
    "QT_DOC_EX_55_59": "qt_doc_exe_55_59",
    "QT_DOC_EX_60_MAIS": "qt_doc_exe_60_mais",
    
    # Docentes por cor/raça
    "QT_DOC_EX_BRANCA": "qt_doc_exe_branca",
    "QT_DOC_EX_PRETA": "qt_doc_exe_preta",
    "QT_DOC_EX_PARDA": "qt_doc_exe_parda",
    "QT_DOC_EX_AMARELA": "qt_doc_exe_amarela",
    "QT_DOC_EX_INDIGENA": "qt_doc_exe_indigena",
    "QT_DOC_EX_COR_ND": "qt_doc_exe_cor_nd",
    
    # Docentes por nacionalidade
    "QT_DOC_EX_BRA": "qt_doc_exe_bra",
    "QT_DOC_EX_EST": "qt_doc_exe_est",
    "QT_DOC_EX_COM_DEFICIENCIA": "qt_doc_exe_com_deficiencia",
    
    # Docentes - variantes históricas sem prefixo QT_ (2009)
    "DOC_EX_0_29": "qt_doc_exe_0_29",                         # 2009
    "DOC_EX_30_34": "qt_doc_exe_30_34",                       # 2009
    "DOC_EX_35_39": "qt_doc_exe_35_39",                       # 2009
    "DOC_EX_40_44": "qt_doc_exe_40_44",                       # 2009
    "DOC_EX_45_49": "qt_doc_exe_45_49",                       # 2009
    "DOC_EX_50_54": "qt_doc_exe_50_54",                       # 2009
    "DOC_EX_55_59": "qt_doc_exe_55_59",                       # 2009
    "DOC_EX_60_MAIS": "qt_doc_exe_60_mais",                   # 2009
    "DOC_EX_BRANCA": "qt_doc_exe_branca",                     # 2009
    "DOC_EX_PRETA": "qt_doc_exe_preta",                       # 2009
    "DOC_EX_PARDA": "qt_doc_exe_parda",                       # 2009
    "DOC_EX_AMARELA": "qt_doc_exe_amarela",                   # 2009
    "DOC_EX_INDIGENA": "qt_doc_exe_indigena",                 # 2009
    "DOC_EX_COR_ND": "qt_doc_exe_cor_nd",                     # 2009
    "DOC_EX_BRA": "qt_doc_exe_bra",                           # 2009
    "DOC_EX_EST": "qt_doc_exe_est",                           # 2009
    "DOC_EX_COM_DEFICIENCIA": "qt_doc_exe_com_deficiencia",   # 2009
    "DOC_EX_FEMI": "qt_doc_exe_fem",                          # 2009
    "DOC_EX_MASC": "qt_doc_exe_masc",                         # 2009
    "DOC_EX_SEM_GRAD": "qt_doc_exe_sem_grad",                 # 2009
    "DOC_EX_GRAD": "qt_doc_exe_grad",                         # 2009
    "DOC_EX_ESP": "qt_doc_exe_esp",                           # 2009
    "DOC_EX_MEST": "qt_doc_exe_mest",                         # 2009
    "DOC_EX_DOUT": "qt_doc_exe_dout",                         # 2009
    "DOC_EX_INT": "qt_doc_exe_int",                           # 2009
    "DOC_EX_INT_DE": "qt_doc_exe_int_de",                     # 2009
    "DOC_EX_INT_SEM_DE": "qt_doc_exe_int_sem_de",             # 2009
    "DOC_EX_PARC": "qt_doc_exe_parc",                         # 2009
    "DOC_EX_HOR": "qt_doc_exe_hor",                           # 2009
    "QT_DOCENTE_TOTAL": "qt_doc_total",                       # 2009: nome alternativo
    "QT_DOCENTE_EXE": "qt_doc_exe",                           # 2009: nome alternativo
    
    # Biblioteca (disponibilidade varia por ano — veja dicionário)
    "IN_ACESSO_PORTAL_CAPES": "in_acesso_portal_capes",
    "IN_ASSINA_OUTRA_BASE": "in_assina_outra_base",
    "IN_ACESSO_OUTRAS_BASES": "in_acesso_outras_bases",      # 2009–2017
    "IN_REPOSITORIO_INSTITUCIONAL": "in_repositorio_inst",   # a partir de 2015
    "IN_BUSCA_INTEGRADA": "in_busca_integrada",              # a partir de 2015
    "IN_SERVICO_INTERNET": "in_servico_internet",            # a partir de 2015
    "IN_PARTICIPA_REDE_SOCIAL": "in_participa_rede_social",  # a partir de 2015
    "IN_CATALOGO_ONLINE": "in_catalogo_online",              # a partir de 2015
    "QT_PERIODICO_ELETRONICO": "qt_periodico_eletronico",    # a partir de 2015
    "QT_LIVRO_ELETRONICO": "qt_livro_eletronico",            # a partir de 2015
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias no silver (dim + hist)
# ---------------------------------------------------------------------------

IES_REQUIRED_COLUMNS: list[str] = [
    "co_ies",
    "ano",
    "no_ies",
    "sg_ies",
    "sg_uf",
    "co_municipio",
    "no_municipio",
    "tp_categoria_adm",
    "tp_org_academica",
    # tp_rede: presente apenas a partir de 2023; anos anteriores derivam de tp_categoria_adm
]

# ---------------------------------------------------------------------------
# Tipos canônicos do silver IES
# ---------------------------------------------------------------------------

IES_SILVER_SCHEMA: dict[str, pl.DataType] = {
    "ano": pl.Int32,
    "co_regiao": pl.Int32,
    "no_regiao": pl.String,
    "co_uf": pl.Int32,
    "sg_uf": pl.String,
    "no_uf": pl.String,
    "co_municipio": pl.Int64,
    "no_municipio": pl.String,
    "in_capital": pl.Int32,
    "co_mesorregiao": pl.Int32,
    "no_mesorregiao": pl.String,
    "co_microrregiao": pl.Int32,
    "no_microrregiao": pl.String,
    "tp_org_academica": pl.Int32,
    "tp_rede": pl.Int32,
    "tp_categoria_adm": pl.Int32,
    "co_mantenedora": pl.Int64,
    "no_mantenedora": pl.String,
    "co_ies": pl.Int64,
    "no_ies": pl.String,
    "sg_ies": pl.String,
}

# ---------------------------------------------------------------------------
# Decodificadores — baseados estritamente no dicionário de dados
# ---------------------------------------------------------------------------

# TP_ORGANIZACAO_ACADEMICA
# Nota: opção 5 não existe em 2009 (conforme dicionário)
ORG_ACADEMICA: dict[int, str] = {
    1: "Universidade",
    2: "Centro Universitário",
    3: "Faculdade",
    4: "Instituto Federal de Educação, Ciência e Tecnologia",
    5: "Centro Federal de Educação Tecnológica",
}

# TP_REDE
REDE: dict[int, str] = {
    1: "Pública",
    2: "Privada",
}

# TP_CATEGORIA_ADMINISTRATIVA
# Notas do dicionário:
#   - Categorias 6, 8 e 9 somente em 2009
#   - Categoria 7 criada em 2012
CATEGORIA_ADM: dict[int, str] = {
    1: "Pública Federal",
    2: "Pública Estadual",
    3: "Pública Municipal",
    4: "Privada com fins lucrativos",
    5: "Privada sem fins lucrativos",
    6: "Privada - Particular em sentido estrito",  # apenas 2009
    7: "Especial",                                  # a partir de 2012
    8: "Privada comunitária",                       # apenas 2009
    9: "Privada confessional",                      # apenas 2009
}
