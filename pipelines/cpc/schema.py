"""
pipelines/cpc/schema.py — Schema do CPC (Conceito Preliminar de Curso).

Baseado na inspeção direta das planilhas do INEP (2007-2023).
O CPC é calculado nos anos de aplicação do ENADE (ciclos trienais).
2020 não existe (pandemia).

Granularidade:
  - 2017-2023: uma linha por (co_ies, co_curso, ano)
  - 2007-2016: uma linha por (co_ies, co_area, ano)  ← sem co_curso
Chave de join com dim_ies: co_ies.

## Limitação conhecida: ausência de co_curso antes de 2017

O INEP não publicava o código do curso (co_curso) nos arquivos do CPC
anteriores a 2017. Nesses anos, a granularidade mínima identificável é
(co_ies, co_area, ano) — área de avaliação por IES.

Imputar co_curso via join com o censo (co_ies + co_area) não é viável:
  - Apenas ~39% dos pares (co_ies, co_area) são unívocos no censo de 2016
  - Os ~61% restantes têm 2 ou mais cursos da mesma área na mesma IES
  - Imputação produziria erros silenciosos nos joins futuros

Consequências práticas:
  - Análises por IES   (co_ies + ano):              sem limitação, 2007-2023
  - Análises por área  (co_ies + co_area + ano):    sem limitação, 2007-2023
  - Join com censo no nível do curso:               apenas 2017 em diante
  - visão_ies futura: 2007-2016 contribuem com médias por IES, não por curso

Variações históricas por período:

  2007       : sem co_curso; colunas em inglês/abreviadas; CPC = 'CPC Contínuo'
               muitas colunas auxiliares descartáveis (ingressantes, médias brutas,
               proporções de docentes, códigos internos de região/rede)
  2008       : similar ao 2007; 'IDD Índice' / 'IDD Índice Auxiliar' descartáveis
  2009       : sem co_curso; 'CPC contínuo' / 'CPC faixa'
               colunas auxiliares de ingressantes e proporções de docentes descartáveis
  2010-2011  : similar ao 2009, pequenas diferenças de nomes
  2012       : sem co_curso; 'CPC (contínuo)' / 'CPC (faixa)'
               colunas auxiliares de ingressantes e proporções de docentes descartáveis
  2013       : sem co_curso; 'CPC Contínuo' / 'CPC Faixa'; 'Observação' descartável
  2014       : xlsx; sem co_curso; 'CPC Contínuo' / 'CPC Faixa'
               'Concluintes Inscritos' / 'Concluintes Participantes com nota no Enem'
               sem o prefixo 'Nº de'; 'Observação' descartável
  2015-2016  : xls; sem co_curso; mesmo padrão de 2014; 'Obs'/'Observação' descartável
  2017       : xlsx; tem co_curso; coluna 'Edição' em vez de 'Ano'; 'Observação' descartável
  2018       : xlsx; 'Nota Bruta - Oportunidade…' com hífen simples (-) em vez de (-)
  2019       : xlsx; layout moderno; tem co_curso; 'Observação' descartável
  2021       : xlsx; asteriscos extras (**, ***, ***) nos nomes; 'Modalidade de Ensino***'
  2022-2023  : xlsx; layout moderno; cobertura total
"""

import polars as pl

# ---------------------------------------------------------------------------
# Mapeamento: nome original (após strip) → nome canônico
# Cobre todas as variações de 2007 a 2023.
# ---------------------------------------------------------------------------

CPC_COLUMN_MAP: dict[str, str] = {
    # --- Ano ---
    "Ano": "ano",
    "Ano Enade": "ano",
    "Edição": "ano",                                 # 2017

    # --- Identificação da área ---
    "Código da Área": "co_area",
    "Código da área": "co_area",
    "Cód.Área": "co_area",
    "co_grupo": "co_area",                           # 2007
    "Área de Avaliação": "area_avaliacao",
    "Área de enquadramento": "area_avaliacao",
    "Área de Enquadramento": "area_avaliacao",
    "Área de Enquadramento": "area_avaliacao",
    "Área": "area_avaliacao",                        # 2007
    "Área de Enquadramento": "area_avaliacao",
    "Cód.Área": "co_area",

    # --- Grau acadêmico ---
    "Grau acadêmico": "grau_academico",              # 2021

    # --- Modalidade ---
    "Modalidade de Ensino": "modalidade",
    "Modalidade de Ensino***": "modalidade",         # 2021: três asteriscos de rodapé

    # --- Identificação da IES ---
    "Código da IES": "co_ies",
    "Código da IES*": "co_ies",                      # 2021
    "co_ies": "co_ies",                              # 2007
    "Código da IES Antiga": "_co_ies_antiga",        # 2009 (descartar)
    "Nome da IES": "no_ies",
    "Nome da IES*": "no_ies",                        # 2021
    "IES": "no_ies",                                 # 2009
    "Sigla da IES": "sg_ies",
    "Sigla da IES*": "sg_ies",                       # 2021
    "Sigla": "sg_ies",                               # 2009
    "Sigla IES": "sg_ies",                           # 2007
    "Organização Acadêmica": "org_academica",
    "Organização Acadêmica*": "org_academica",       # 2021
    "Organização": "org_academica",                  # 2009
    "Org. Acadêmica": "org_academica",               # 2013
    "Organização acadêmica": "org_academica",
    "Categoria Administrativa": "categoria_adm",
    "Categoria Administrativa*": "categoria_adm",   # 2021
    "Dep. Administrativa": "categoria_adm",          # 2007/2009
    "Categ. Administrativa": "categoria_adm",        # 2013
    "Categoria administrativa": "categoria_adm",

    # --- Identificação do curso ---
    "Código do Curso": "co_curso",
    "Código do Curso**": "co_curso",                 # 2021

    # --- Localização ---
    "Código do Município": "co_municipio",
    "Código do Município***": "co_municipio",        # 2021
    "Código do município do curso": "co_municipio",
    "Cód. Município": "co_municipio",                # 2013
    "codmunic_inep": "co_municipio",                 # 2007
    "Código da Subárea": "_co_subarea",              # 2009 (descartar)
    "Município do Curso": "no_municipio",
    "Município do Curso***": "no_municipio",         # 2021
    "Município do curso": "no_municipio",
    "Município (funcionamento do curso)": "no_municipio",  # 2007
    "Município": "no_municipio",                     # 2009
    "Sigla da UF": "sg_uf",
    "Sigla da UF**": "sg_uf",                        # 2021
    "Sigla da UF** ": "sg_uf",                       # 2021 (espaço extra)
    "UF do curso": "sg_uf",
    "UF do Curso": "sg_uf",
    "UF": "sg_uf",

    # --- Participação ---
    "Nº de Concluintes Inscritos": "qt_inscritos",
    "Número concluintes inscritos no Enade": "qt_inscritos",
    "Conluintes Inscritos": "qt_inscritos",          # typo INEP 2013
    "Concluintes Inscritos": "qt_inscritos",         # 2014-2016: sem prefixo "Nº de"
    "n. alunos presentes Enade": "qt_inscritos",     # 2007
    "Nº de Concluintes Participantes": "qt_participantes",
    "Nº  de Concluintes Participantes": "qt_participantes",  # 2017 (espaço duplo)
    "Número concluintes participantes no Enade": "qt_participantes",
    "Concluintes Participantes": "qt_participantes",
    "Participantes Concluintes": "qt_participantes", # 2007
    "Número de Participantes Concluintes": "qt_participantes",  # 2009
    "Concluintes participantes com nota no Enem": "qt_participantes_idd",
    "Concluintes Participantes com nota no Enem": "qt_participantes_idd",  # 2014-2016
    "Nº de Concluintes Participantes com nota no Enem": "qt_participantes_idd",  # 2017
    "Número de ingressantes participantes no Enem": "qt_participantes_idd",  # 2012 (proxy)
    "Ingressantes participantes no Enem": "qt_participantes_idd",  # 2013
    "Proporção de concluintes participantes com nota no Enem": "prop_participantes_idd",
    "Percentual de Concluintes participantes com nota no Enem": "prop_participantes_idd",

    # --- Identificação da IES (variações adicionais) ---
    "Código IES": "co_ies",                          # 2011
    "Cód.IES": "co_ies",                             # 2013
    "Código Área": "co_area",                        # 2011
    "Código Área Agrupamento": "_co_area_agrup",     # 2011 (descartar)
    "Área Enquadramento": "area_avaliacao",          # 2011
    "Código UF": "_co_uf",                           # 2010/2011 (descartar)
    "Código da UF": "_co_uf",                        # 2010 (descartar)
    "Sigla UF": "sg_uf",                             # 2011
    "Código Município": "co_municipio",              # 2011
    "IES": "no_ies",                                 # 2010

    # --- Notas ENADE ---
    "Nota Bruta - FG": "nota_bruta_fg",
    "Nota bruta de formação geral": "nota_bruta_fg",
    "Nota Bruta - CE": "nota_bruta_ce",
    "Nota bruta de componente específico": "nota_bruta_ce",
    "Nota Padronizada - FG": "nota_pad_fg",
    "Nota padronizada de formação geral": "nota_pad_fg",
    "Nota Padronizada - CE": "nota_pad_ce",
    "Nota padronizada de componente específico": "nota_pad_ce",
    "Conceito Enade (Contínuo)": "enade_continuo",
    "Nota Contínua do Enade": "enade_continuo",      # 2013/2014/2017
    "Nota Enade Concluintes = Conceito Enade contínuo": "enade_continuo",  # 2009
    "Nota de concluintes": "enade_continuo",         # 2012
    "Nota Enade Concluintes": "enade_continuo",      # 2010
    "Nota Enade": "_nota_enade_dup",                 # 2007: duplicata de Conceito_Enade — descartada
    "Conceito_Enade": "enade_continuo",              # 2007
    "Nota Enade Concluintes": "enade_continuo",      # 2010/2011 (em 2008 é duplicata — tratado no silver)
    "Conceito Enade Faixa": "_enade_faixa_raw",      # 2010/2011 (descartar — não é contínuo)
    # Componentes docentes em 2011
    "Nota Mestrado": "nota_pad_mestres",             # 2011
    "Nota Doutorado": "nota_pad_doutores",           # 2011
    "Nota Regime": "nota_pad_regime",                # 2011
    # Percepção discente em 2011
    "Nota de Infraestrutura": "nota_pad_infra",      # 2011
    "Nota de Organização Pedagógica": "nota_pad_org_didatica",  # 2011
    # Participação 2010/2011
    "Número de Concluintes Participantes": "qt_participantes",
    "Número Concluintes Inscritos": "qt_inscritos",
    "Número de Concluintes Inscritos": "qt_inscritos",

    # --- Notas IDD ---
    "Nota Bruta - IDD": "nota_bruta_idd",
    "Nota Bruta do IDD": "nota_bruta_idd",
    "Nota bruta do IDD": "nota_bruta_idd",
    "Nota Padronizada - IDD": "nota_pad_idd",
    "Nota Padronizada do IDD": "nota_pad_idd",
    "Nota padronizada do IDD": "nota_pad_idd",
    "Nota IDD": "nota_bruta_idd",                    # 2007/2008
    "Conceito_IDD": "_idd_conceito_dup",             # 2007: duplicata de Nota IDD — descartado

    # --- Corpo docente ---
    "Nota Bruta - Mestres": "nota_bruta_mestres",
    "Nota Padronizada - Mestres": "nota_pad_mestres",
    "Nota padronizada de mestres": "nota_pad_mestres",
    "Nota_mestre": "nota_pad_mestres",               # 2009
    "Nota Bruta - Doutores": "nota_bruta_doutores",
    "Nota Padronizada - Doutores": "nota_pad_doutores",
    "Nota padronizada de doutores": "nota_pad_doutores",
    "Nota_doutor": "nota_pad_doutores",              # 2007/2009
    "Nota Bruta - Regime de Trabalho": "nota_bruta_regime",             # 2019-2023: travessão
    "Nota Bruta - Regime de Trabalho": "nota_bruta_regime",
    "Nota Padronizada - Regime de Trabalho": "nota_pad_regime",
    "Nota padronizada de regime de trabalho (integral / parcial)": "nota_pad_regime",
    "Nota padronizada de regime de trabalho": "nota_pad_regime",
    "Nota_regime": "nota_pad_regime",                # 2007/2009

    # --- Percepção discente ---
    "Nota Bruta - Organização Didático-Pedagógica": "nota_bruta_org_didatica",   # 2019-2023: travessão
    "Nota Bruta - Organização Didático-Pedagógica": "nota_bruta_org_didatica",
    "Nota Bruta - Org. Didático-Pedagógica": "nota_bruta_org_didatica",          # 2013
    "Nota Padronizada - Organização Didático-Pedagógica": "nota_pad_org_didatica",
    "Nota padronizada de organização didático pedagógica": "nota_pad_org_didatica",
    "Nota Padronizada - Org. Didático-Pedagógica": "nota_pad_org_didatica",       # 2013
    "Nota_pedag": "nota_pad_org_didatica",           # 2007/2009
    "Nota Bruta - Infraestrutura e Instalações Físicas": "nota_bruta_infra",     # 2019-2023: travessão
    "Nota Bruta - Infraestrutura e Instalações Físicas": "nota_bruta_infra",
    "Nota Bruta - Infraestrutura": "nota_bruta_infra",  # 2013
    "Nota Padronizada - Infraestrutura e Instalações Físicas": "nota_pad_infra",
    "Nota padronizada de infraestrutura": "nota_pad_infra",
    "Nota Padronizada - Infraestrutura": "nota_pad_infra",  # 2013
    "Nota_infra": "nota_pad_infra",                  # 2007/2009
    "Nota Bruta - Oportunidade de Ampliação da Formação": "nota_bruta_oportunidade",   # 2019-2023: travessão
    "Nota Bruta - Oportunidade de Ampliação da Formação": "nota_bruta_oportunidade",   # 2018: hífen simples
    "Nota Bruta - Oportunidades de Ampliação da Formação": "nota_bruta_oportunidade",
    "Nota Bruta - Oport. Ampliação": "nota_bruta_oportunidade",  # 2013
    "Nota Padronizada - Oportunidade de Ampliação da Formação": "nota_pad_oportunidade",
    "Nota Padronizada - Oportunidades de Ampliação da Formação": "nota_pad_oportunidade",
    "Nota Padronizada - Oport. Ampliação": "nota_pad_oportunidade",  # 2013

    # --- Colunas descartáveis (publicadas pelo INEP mas não usadas no silver) ---
    # Observação editorial (indica cursos sem conceito em alguns anos)
    "Observação": "_observacao",                     # 2013, 2014, 2016, 2017, 2019
    "Obs": "_observacao",                            # 2015

    # Contagem de docentes / matrículas — metadado não utilizado no silver
    "Nº de Docentes": "_qt_docentes",                # 2017
    "Nr. de Docentes": "_qt_docentes",               # 2014, 2015, 2016
    "Número de Docentes": "_qt_docentes",            # 2007, 2008
    "Número de docentes": "_qt_docentes",            # 2009, 2010
    "Número docentes": "_qt_docentes",               # 2011
    "Nr. de Matrículas": "_qt_matriculas",           # 2014

    # Contagem de cursos na unidade de cálculo — metadado não utilizado no silver
    "Número de Cursos na Unidade de Cálculo": "_qt_cursos_unidade",  # 2010
    "Número Cursos Unidade": "_qt_cursos_unidade",   # 2011
    "Número de cursos da unidade": "_qt_cursos_unidade",  # 2012, 2014
    "Nr. Cursos da Unidade": "_qt_cursos_unidade",   # 2013

    # Região do curso — não usada no silver (sg_uf é suficiente)
    "Região do Curso": "_regiao",                    # 2013
    "Região do curso": "_regiao",                    # 2014

    # Notas brutas gerais (média FG+CE) — componentes já capturados separadamente
    "Nota Bruta - Geral": "_nota_bruta_geral",       # 2013, 2014
    "Nota bruta geral": "_nota_bruta_geral",         # 2012

    # Dados de ingressantes — fora do escopo do CPC de concluintes
    "Participantes Ingressantes": "_qt_ingressantes_part",       # 2007, 2008
    "Número de Participantes Ingressantes": "_qt_ingressantes_part",  # 2009, 2010
    "Número Ingressantes Participantes no  Enem": "_qt_ingressantes_enem",  # 2011
    "Número de Ingressantes Inscritos": "_qt_ingressantes_inscritos",  # 2010
    "Número Ingressantes Inscritos": "_qt_ingressantes_inscritos",  # 2011
    "Número de ingressantes inscritos no Enade": "_qt_ingressantes_inscritos",  # 2012
    "Ingressantes inscritos no Enade": "_qt_ingressantes_inscritos",  # 2013
    "Nota Enade Ingressantes": "_nota_enade_ingressantes",       # 2008, 2009, 2010
    "Nota Enem Ingressantes": "_nota_enade_ingressantes",        # 2011
    "Nota dos ingressantes no Enem": "_nota_enade_ingressantes", # 2012
    "Nota dos Ingressantes no Enem": "_nota_enade_ingressantes", # 2013
    "Escolaridade dos pais (superior) dos ingressantes no Enem": "_esc_pais_ingressantes",  # 2012
    "% de Ingressantes cujos pais têm ensino superior": "_esc_pais_ingressantes",  # 2013

    # Médias brutas FG/CE por concluintes e ingressantes — não usadas no silver
    "Média FG_Conc": "_media_fg_conc",               # 2007-2010
    "Média FG Conc": "_media_fg_conc",               # 2011
    "Média CE_Conc": "_media_ce_conc",               # 2007-2010
    "Média CE Conc": "_media_ce_conc",               # 2011
    "Média FG_Ing": "_media_fg_ing",                 # 2007-2010
    "Média CE_Ing": "_media_ce_ing",                 # 2007-2010
    "Média Geral Conc": "_media_geral_conc",         # 2007, 2008
    "Média Geral Ing": "_media_geral_ing",           # 2007, 2008

    # Proporções brutas de docentes — já capturadas pelas notas padronizadas
    "% Docentes Doutores": "_prop_doutores",         # 2007, 2008
    "% Docentes Mestres": "_prop_mestres",           # 2008
    "% Docentes Regime Parcial/Integral": "_prop_regime",  # 2007, 2008
    "Docentes Doutores": "_qt_doutores",             # 2007, 2008
    "Docentes Mestres": "_qt_mestres",               # 2008
    "Docentes Regime Parcial/Integral": "_qt_regime_parcial",  # 2007, 2008
    "Proporção de Docentes no Mínimo Mestres": "_prop_mestres",   # 2009, 2010
    "Proporção de Docentes no mínimo Doutores": "_prop_doutores", # 2009, 2010
    "Proporção de Docentes Regime Parcial/Integral": "_prop_regime",  # 2009, 2010
    "Proporção Docentes Mestres": "_prop_mestres",   # 2011
    "Proporção Docentes Doutores": "_prop_doutores", # 2011
    "Proporção Docentes Parc Integral": "_prop_regime",  # 2011
    "% de Mestres": "_prop_mestres",                 # 2012
    "% de mestres": "_prop_mestres",                 # 2012 (variante minúscula)
    "% de Doutores": "_prop_doutores",               # 2012
    "% de regime de trabalho (integral / parcial)": "_prop_regime",  # 2012
    "Nota Padronizada de Mestres": "_nota_pad_mestres_dup",  # 2013 (duplicata)
    "Nota Padronizada de Doutores": "_nota_pad_doutores_dup",  # 2013 (duplicata)
    "% de Mestres": "_prop_mestres",
    "% de Doutores": "_prop_doutores",
    "% regime de trabalho mínimo parcial": "_prop_regime",  # 2013
    "% mestres": "_prop_mestres",
    "% doutores": "_prop_doutores",

    # Proporções de percepção discente — já capturadas pelas notas padronizadas
    "% avaliam bem o plano de ensino (resp. A)": "_prop_plano_ensino",  # 2007, 2008
    "% equipamentos disponíveis são suficientes (resp. A/B)": "_prop_equipamentos",  # 2007, 2008
    "Número de alunos que responderam a pergunta sobre infraestrutura": "_qt_resp_infra",   # 2009, 2010
    "Número de alunos que responderam a pergunta sobre plano de ensino": "_qt_resp_plano",  # 2009, 2010
    "Proporção que avalia que os equipamentos disponíveis são suficientes (resp. A/B)": "_prop_equipamentos",  # 2009, 2010
    "Proporção que avalia bem o plano de ensino (resp. A)": "_prop_plano_ensino",  # 2009, 2010
    "Proporção de respostas sobre infraestrutura": "_prop_resp_infra",  # 2011
    "Proporção de respostas sobre plano ensino": "_prop_resp_plano",    # 2011
    "% de infraestrutura": "_prop_infra",            # 2012
    "% organização didático pedagógica": "_prop_org_didatica",  # 2012

    # Conceito Enade faixa (legado 2009) — não é contínuo, descartado
    "Conceito Enade faixa": "_conceito_enade_faixa_legado",  # 2009

    # Códigos internos / campos legados do Stata
    "cd_dep": "_cd_dep",                             # 2007
    "cd_catad": "_cd_catad",                         # 2007, 2008
    "cd_orgac": "_cd_orgac",                         # 2007, 2008
    "co_regiao_habil": "_co_regiao_habil",           # 2007, 2008
    "código UF": "_codigo_uf_legado",                # 2007, 2008
    "Rede": "_rede",                                 # 2007
    "Termo_insumos": "_termo_insumos",               # 2007
    "Sub Area": "_sub_area",                         # 2008
    "Sub Área": "_sub_area",                         # 2009
    "IDD Índice Auxiliar": "_idd_indice_auxiliar",   # 2008
    "IDD Índice": "_idd_indice",                     # 2008

    # --- CPC final ---
    "CPC (Contínuo)": "cpc_continuo",
    "CPC (contínuo)": "cpc_continuo",               # 2012
    "CPC Contínuo": "cpc_continuo",                 # 2007/2013/2014
    "CPC contínuo": "cpc_continuo",                 # 2009/2010/2011
    "Conceito Preliminar Curso": "_cpc_prelim_dup",  # 2007: mesmo que CPC Contínuo — descartado
    "CPC (Faixa)": "cpc_faixa",
    "CPC (faixa)": "cpc_faixa",                     # 2012
    "CPC Faixa": "cpc_faixa",                       # 2013/2014
    "CPC faixa": "cpc_faixa",                       # 2009/2010/2011

    # --- Beneficência ---
    "Entidade Beneficiente de Assistência Social (CEBAS)": "in_cebas",
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias no silver
# Nota: co_curso não existe antes de 2017 — validado separadamente
# ---------------------------------------------------------------------------

CPC_REQUIRED_COLUMNS: list[str] = [
    "ano",
    "co_ies",
    "area_avaliacao",
    "enade_continuo",
    "cpc_continuo",
    "cpc_faixa",
]

# co_curso só existe a partir de 2017
CPC_REQUIRED_COLUMNS_WITH_CURSO: list[str] = CPC_REQUIRED_COLUMNS + ["co_curso"]

# Anos sem co_curso no arquivo original do INEP
YEARS_WITHOUT_CO_CURSO = {2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016}

# Anos sem cpc_faixa no arquivo original do INEP (2007/2008 só publicaram o contínuo)
YEARS_WITHOUT_CPC_FAIXA = {2007, 2008}

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
