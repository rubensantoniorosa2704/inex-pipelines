# inex-pipelines — Product

## Visão geral

`inex-pipelines` é o repositório de ETL que transforma os microdados brutos do INEP (baixados pelo `inex-ingest`) em dados limpos, padronizados e prontos para consumo analítico.

O objetivo final é alimentar uma plataforma de consulta pública — um frontend React que permite a qualquer pessoa explorar indicadores do ensino superior brasileiro (ENADE, CPC, IDD, IGC, etc.) sem precisar de SQL ou ferramentas técnicas.

## Contexto

- Os dados brutos são extraídos pelo [`inex-ingest`](../inex-ingest), um CLI em Go que baixa e extrai os microdados do INEP em `data/{indicador}/{ano}/`.
- Este repo é responsável pelas camadas **silver** e **gold**, e pela construção das dimensões centrais (especialmente `dim_ies`).
- Os dados gold serão consumidos por uma API (DuckDB) e um frontend React (a ser criado em repo separado).

## Camadas de dados

| Camada | Responsabilidade |
|--------|-----------------|
| **Bronze** | Arquivos brutos extraídos pelo `inex-ingest`. Não é responsabilidade deste repo. |
| **Silver** | Limpeza e padronização: tipos corretos, encoding corrigido (Latin-1), nomes de colunas normalizados, remoção de linhas inválidas. Um Parquet por ano por indicador. |
| **Gold** | Modelos analíticos prontos para consumo: agregações, joins, tabelas desnormalizadas otimizadas para as queries do frontend. |

## Dimensão central: IES

A `dim_ies` é a tabela de lookup central do sistema inteiro. Todo indicador referencia `co_ies` (código INEP da instituição), que é a chave estável mesmo quando o nome da IES muda ao longo do tempo.

### Regras:
- `dim_ies` sempre exibe o **nome mais recente** da IES (`no_ies_atual`, `sg_ies_atual`).
- `hist_ies_nome` guarda o histórico completo de nomes por ano, para exibição secundária.
- Fonte: **Censo da Educação Superior** (disponível desde 1995 no `inex-ingest`).

## Ordem de execução da pipeline

```
1. censo     → dim_ies + hist_ies_nome   ← base de tudo, deve rodar primeiro
2. enade     → gold_enade
3. cpc       → gold_cpc
4. idd       → gold_idd
5. igc       → gold_igc
6. agregado  → visão_ies                 ← join de dim_ies com todos os gold
```

## Visão agregada (gold final)

A `visão_ies` é uma tabela desnormalizada com chave `(co_ies, ano)` que consolida todos os indicadores:

```
co_ies | ano | no_ies_atual | sg_ies_atual | uf | municipio | categoria_adm | org_academica | enade | cpc | idd | igc | ...
```

Esta é a tabela principal que o frontend vai consultar.

## Próximos passos

### Concluído ✅
- Pipeline do Censo da Educação Superior (2009–2024)
  - `dim_ies`: 3.759 IES, estado atual
  - `hist_ies`: 39.363 linhas, histórico de atributos por ano
  - `fact_censo_ies`: 39.363 linhas, métricas de docentes e técnicos
- Shared: `io.py`, `validate.py`, `paths.py`, `types.py`
- Silver incremental (só reprocessa se bronze mudou)

### A implementar
1. Pipeline do ENADE — baixar, inspecionar dicionário, escrever schema → silver → gold
2. Pipeline do CPC
3. Pipeline do IDD
4. Pipeline do IGC
5. `visão_ies` — tabela gold agregada (join de todos os `fact_*` com `dim_ies`)
