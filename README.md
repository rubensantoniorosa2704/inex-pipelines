# inex-pipelines

Pipelines ETL que transformam os microdados brutos do INEP em dados limpos, padronizados e prontos para análise. Parte de um ecossistema maior para democratizar o acesso aos indicadores do ensino superior brasileiro.

## Contexto

Os dados brutos são baixados pelo [`inex-ingest`](https://github.com/rubensantoniorosa2704/inex-ingest), um CLI em Go que extrai os microdados do INEP em `data/{indicador}/{ano}/`. Este repo é responsável pelas camadas **silver** e **gold**.

```
inex-ingest     → bronze: arquivos extraídos
inex-pipelines  → silver: Parquets limpos por ano
                → gold: modelos analíticos prontos para consumo
```

## Camadas de dados

| Camada | Responsabilidade |
|--------|-----------------|
| **Bronze** | Arquivos brutos do INEP. Responsabilidade do `inex-ingest`. |
| **Silver** | Um Parquet por indicador por ano. Limpeza de encoding, tipos corretos, nomes de colunas padronizados. |
| **Gold** | Modelos analíticos com chave `(co_ies, ano)`. Prontos para join e consulta via DuckDB. |

## Tabelas gold disponíveis

| Tabela | Chave | Descrição |
|--------|-------|-----------|
| `dim_ies` | `co_ies` | Uma linha por IES, estado mais recente |
| `hist_ies` | `(co_ies, ano)` | Histórico de nome, categoria e organização acadêmica por ano |
| `fact_censo_ies` | `(co_ies, ano)` | Métricas anuais do Censo: docentes e técnico-administrativos |
| `fact_cpc` | `(co_ies, co_curso, ano)` | CPC por curso: notas ENADE, IDD, corpo docente e percepção discente |
| `fact_idd` | `(co_ies, co_curso, ano)` | IDD por curso: desempenho observado vs. esperado |
| `fact_censo_cursos` | `(co_ies, co_curso, ano)` | Censo por curso: vagas, ingressantes, matrículas, concluintes, cor/raça, financiamento |
| `fact_igc` | `(co_ies, ano)` | IGC por IES: conceitos médios de graduação/mestrado/doutorado, proporções e IGC contínuo/faixa |

O `co_ies` (código INEP) é a chave estável de todas as tabelas — não muda mesmo quando a IES muda de nome ou categoria administrativa.

## Pipelines implementados

- [x] **Censo da Educação Superior — IES** — 2009–2024
- [x] **Censo da Educação Superior — Cursos** — 2009–2024
- [x] **CPC** — 2007–2023
- [x] **IDD** — 2016–2023
- [x] **IGC** — 2017–2023

## Pipelines planejados

- [ ] **IGC (2008–2016)** — anos anteriores com layout multi-sheet; não implementado por fragilidade (ver schema.py)
- [ ] **Conceito ENADE** — resultado agregado por curso, 2004–2025
- [ ] **ENADE Microdados** — dados por aluno, perfil socioeconômico; pipeline separado por volume e complexidade
- [ ] **visão_ies** — tabela agregada com todos os indicadores por IES

## Estrutura

```
pipelines/
  censo/
    schema.py     # mapeamento de colunas do INEP → nomes canônicos
    silver.py     # limpeza e padronização
    gold.py       # dim_ies, hist_ies, fact_censo_ies
  enade/          # a implementar
  cpc/            # a implementar
  idd/            # a implementar
  igc/            # a implementar
shared/
  io.py           # leitura de CSV do INEP, escrita de Parquet
  paths.py        # resolução de caminhos via variáveis de ambiente
  types.py        # tipos canônicos compartilhados
  validate.py     # checagens de qualidade reutilizáveis
```

## Pré-requisitos

- Python 3.11+
- [`inex-ingest`](https://github.com/rubensantoniorosa2704/inex-ingest) para baixar os dados bronze

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Uso

### 1. Baixar os dados bronze

```bash
# No diretório do inex-ingest
./inex-ingest ingest --dataset censo-superior --year 2009-2024 --data-dir ../inex-pipelines/data/bronze
```

### 2. Gerar o silver

```bash
# Processa apenas anos ainda não processados (ou cujo bronze mudou)
python -m pipelines.censo.silver --year 2009-2024

# Forçar reprocessamento
python -m pipelines.censo.silver --year 2023 --force

# Com log detalhado
python -m pipelines.censo.silver --year 2023 --verbose
```

### 3. Gerar o gold

```bash
python -m pipelines.censo.gold
```

O gold relê todos os silvers disponíveis e recalcula `dim_ies`, `hist_ies` e `fact_censo_ies`.

## Variáveis de ambiente

| Variável | Padrão (dev) | Descrição |
|----------|-------------|-----------|
| `INEX_DATA_DIR` | `../inex-ingest/data` | Raiz dos dados bronze |
| `PIPELINES_DATA_DIR` | `./data` | Raiz dos dados silver/gold |

## Decisões técnicas

**Por que Polars?** Os CSVs do INEP são grandes. Polars é significativamente mais rápido que Pandas para leitura e transformação, e tem suporte nativo a tipos estritos.

**Por que Parquet com zstd?** Boa compressão, leitura rápida, suportado por DuckDB e praticamente qualquer ferramenta analítica.

**Por que pipeline por indicador?** Cada indicador do INEP tem suas próprias especificidades de schema, encoding e estrutura de arquivos. Um pipeline genérico forçaria abstrações desnecessárias e frágeis.

**Por que 2009 em diante no Censo?** O `CO_IES` (identificador único da IES) só existe a partir de 2009. Anos anteriores usam outro schema, sem chave estável.

**Por que `pl.concat(..., how="diagonal")`?** O INEP adiciona e remove colunas entre anos sem aviso. O modo `diagonal` preenche com nulo as colunas ausentes em anos mais antigos — comportamento correto para dados históricos com schema evolutivo.

**Por que `co_curso` é nulo no CPC antes de 2017?** O INEP não publicava o código do curso nos arquivos do CPC anteriores a 2017. A granularidade nesses anos é `(co_ies, co_area, ano)`. Imputar `co_curso` via join com o censo não é viável: ~61% dos pares `(co_ies, co_area)` são ambíguos — a mesma IES oferece 2 ou mais cursos da mesma área. Joins no nível do curso com o censo estão disponíveis apenas a partir de 2017.

## Licença

[MIT](LICENSE)
