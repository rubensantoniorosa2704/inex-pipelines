# inex-pipelines

Pipelines ETL que transformam os microdados brutos do INEP em dados limpos, padronizados e prontos para análise. Parte de um ecossistema maior para democratizar o acesso aos indicadores do ensino superior brasileiro.

## 1. Contexto

Os dados brutos são baixados pelo [`inex-ingest`](https://github.com/rubensantoniorosa2704/inex-ingest), um CLI em Go que extrai os microdados do INEP em `data/{indicador}/{ano}/`. Este repo é responsável pelas camadas **silver** e **gold**.

```
inex-ingest     → bronze: arquivos extraídos
inex-pipelines  → silver: Parquets limpos por ano
                → gold: modelos analíticos prontos para consumo
```

## 2. Uso

### 2.1 Gerar o silver

```bash
# CPC (2007–2023, com lookup automático de co_curso)
python -m pipelines.cpc.silver --year 2007-2023

# IDD
python -m pipelines.idd.silver --year 2016-2023

# Censo
python -m pipelines.censo.silver --year 2009-2024
python -m pipelines.censo.silver_cursos --year 2009-2024
```

Flags disponíveis: `--force` (reprocessa tudo), `--verbose` (log detalhado).

### 2.2 Gerar o gold

```bash
python -m pipelines.cpc.gold
python -m pipelines.idd.gold
python -m pipelines.censo.gold
python -m pipelines.censo.gold_cursos
```

## 3. Variáveis de ambiente

| Variável | Padrão (dev) | Descrição |
|----------|-------------|-----------|
| `INEX_DATA_DIR` | `../inex-ingest/data` | Raiz dos dados bronze |
| `PIPELINES_DATA_DIR` | `./data` | Raiz dos dados silver/gold |

## 4. Licença

[MIT](LICENSE)
