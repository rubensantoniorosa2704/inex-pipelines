# inex-pipelines — Design

## Arquitetura geral

```
inex-ingest (Go)           → bronze: arquivos extraídos em data/{indicador}/{ano}/
inex-pipelines (Python)    → silver: Parquet limpo por ano
                           → gold: modelos analíticos por indicador
                           → gold_agregado: visão_ies (join central)
API (a definir)            → recebe JSON de query, gera SQL, executa no DuckDB
Frontend React (a definir) → query builder visual, tabelas, gráficos, consultas salvas
```

## Estrutura do repositório

```
inex-pipelines/
  pipelines/
    censo/
      silver.py       # limpeza e padronização do Censo
      gold.py         # dim_ies + hist_ies_nome
      schema.py       # tipos e colunas esperados
    enade/
      silver.py
      gold.py
      schema.py
    cpc/
    idd/
    igc/
  shared/
    io.py             # leitura/escrita Parquet padronizada, leitura CSV INEP
    types.py          # tipos comuns entre indicadores
    validate.py       # checagens reutilizáveis
  scripts/
    ingest.sh         # wrapper para chamar inex-ingest como dependência
  tests/
  pyproject.toml
```

## Contrato das colunas comuns (shared/)

Todos os indicadores devem usar estes nomes para os campos em comum:

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `co_ies` | `Int64` | Código INEP da IES — chave primária universal |
| `ano` | `Int32` | Ano de referência |
| `no_ies` | `String` | Nome da IES naquele ano |
| `sg_ies` | `String` | Sigla da IES naquele ano |
| `sg_uf` | `String` | Sigla do estado (UF) |
| `co_municipio` | `Int64` | Código IBGE do município |
| `no_municipio` | `String` | Nome do município |
| `categoria_adm` | `String` | Categoria administrativa (Pública Federal, Privada, etc.) |
| `org_academica` | `String` | Organização acadêmica (Universidade, Centro Universitário, etc.) |

## Modelo de dados gold (implementado)

### `dim_ies` — dimensão atual das IES
Chave: `co_ies` | 3.759 linhas | 24 colunas

Estado mais recente de cada IES (ano de referência: 2024).
Colunas principais: `co_ies`, `ano_referencia`, `no_ies_atual`, `sg_ies_atual`,
`sg_uf`, `no_uf`, `co_municipio`, `no_municipio`, `co_regiao`, `no_regiao`,
`categoria_adm_atual`, `org_academica_atual`, `rede_atual`, `no_mantenedora_atual`.

Regra: sempre o registro do **ano mais recente** disponível no Censo.
`sg_ies_atual` pode ser nula (~18% das IES não têm sigla registrada no INEP).

### `hist_ies` — histórico de atributos que mudam
Chave: `(co_ies, ano)` | 39.363 linhas | 12 colunas

Uma linha por IES por ano. Registra mudanças de nome, sigla, categoria administrativa,
organização acadêmica e mantenedora ao longo do tempo.

Exemplo real (UNIFEBE, co_ies=87): era **Pública Municipal** de 2009 a 2012,
mudou para **Privada sem fins lucrativos** a partir de 2013.

### `fact_censo_ies` — métricas anuais do Censo
Chave: `(co_ies, ano)` | 39.363 linhas | 58 colunas

Métricas quantitativas por IES por ano: docentes por titulação/regime/faixa etária/cor/raça,
técnico-administrativos por escolaridade, indicadores de biblioteca.

Nulos esperados e documentados:
- `qt_doc_*`: nulos em 2009 (INEP não publicou dados de docentes naquele ano)
- Colunas de biblioteca (`in_repositorio_inst`, etc.): nulos em 2009–2014

### Tabelas de fato a implementar
Chave universal: `(co_ies, ano)`

```
fact_enade    → métricas do ENADE por IES/ano
fact_cpc      → Conceito Preliminar de Curso
fact_idd      → Indicador de Diferença entre Desempenhos
fact_igc      → Índice Geral de Cursos
```

### `visão_ies` — tabela gold agregada (a implementar)
Chave: `(co_ies, ano)`

Join de `dim_ies` com todas as tabelas `fact_*`. Tabela principal do frontend.

## Dependência: inex-ingest

O `inex-ingest` é chamado via script como etapa 0 da pipeline. O caminho dos dados brutos é configurável via variável de ambiente `INEX_DATA_DIR` (padrão: `../inex-ingest/data` em dev local, `/data` em Docker).

```bash
# scripts/ingest.sh — chama o inex-ingest para baixar um dataset
cd ../inex-ingest && ./inex-ingest ingest --dataset "$1" ${@:2}
```

## Stack técnica

| Ferramenta | Uso |
|-----------|-----|
| **Python 3.11+** | Linguagem das pipelines |
| **Polars** | Leitura, limpeza e transformação de DataFrames (mais rápido que Pandas para CSVs grandes) |
| **PyArrow** | Serialização Parquet |
| **DuckDB** | Consultas analíticas sobre Parquet, construção do gold agregado |
| **Click** | CLI para rodar pipelines individualmente |
| **pytest** | Testes unitários |
| **ruff** | Linting e formatação |

## Decisões de design

- **Polars sobre Pandas**: os CSVs do INEP são grandes (Censo pode ter milhões de linhas). Polars é significativamente mais rápido e tem melhor suporte a tipos.
- **Parquet com zstd**: compressão boa, leitura rápida, compatível com DuckDB e praticamente qualquer ferramenta analítica.
- **Pipeline por indicador**: cada indicador tem suas próprias especificidades (colunas, encoding, estrutura de arquivos). Um pipeline genérico forçaria abstrações desnecessárias.
- **`co_ies` como chave universal**: o código INEP nunca muda, mesmo quando o nome da IES muda. É a âncora de todos os joins.
- **Nome mais recente obrigatório**: `dim_ies` sempre reflete o estado atual. O histórico fica em `hist_ies_nome` para consulta secundária.
