# queries/

Scripts de consulta analítica sobre os dados gold do inex-pipelines.

Todas as queries usam **DuckDB** e leem diretamente os arquivos Parquet — sem necessidade de banco de dados.

## Como rodar

```bash
# Instalar DuckDB (se ainda não tiver)
pip install duckdb

# Rodar uma query
python queries/cpc_idd_censo.py

# Ou via DuckDB CLI (para exploração interativa)
duckdb
> .read queries/cpc_idd_censo.sql
```

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `PIPELINES_DATA_DIR` | `./data` | Raiz dos dados gold |

## Queries disponíveis

| Arquivo | Descrição |
|---------|-----------|
| `cpc_idd_censo.py` | Visão por curso: CPC + IDD + contexto da IES (censo) |

## Estrutura das joins

```
fact_cpc   ─┐
fact_idd   ─┼─ (co_ies, co_curso, ano) ─→ visão por curso
hist_ies   ─┘
             └─ co_ies ─→ dim_ies (localização)
                       ─→ fact_censo_ies (docentes, técnicos)
```
