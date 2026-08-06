# inex-pipelines — Padrões e Convenções

## Estrutura obrigatória de um pipeline

Cada indicador em `pipelines/{indicador}/` deve ter exatamente três arquivos:

```
pipelines/{indicador}/
  __init__.py
  schema.py   → tipos, mapeamento de colunas, decodificadores
  silver.py   → lê bronze, padroniza, salva Parquet silver
  gold.py     → lê silvers, constrói tabelas analíticas, salva Parquet gold
```

**Ordem de implementação obrigatória: schema → silver → gold.**
Nunca escreva o gold antes de ter o silver rodando com dados reais.

---

## Formato de arquivo: Parquet com zstd

Todo dado produzido por este repo é Parquet. Sem exceções.

- Compressão: **zstd** (melhor ratio que snappy, leitura rápida, suportado pelo DuckDB)
- Sempre usar `shared.io.write_parquet()` — nunca `df.write_parquet()` diretamente
- Nunca commitar arquivos de dados (`data/` está no `.gitignore`)

---

## Leitura de CSV do INEP

Sempre usar `shared.io.read_csv_inep()`. Nunca `pl.read_csv()` diretamente.

Motivos:
- Separador é `;` (não `,`)
- Encoding é **Latin-1** (iso-8859-1) — padrão histórico dos arquivos INEP
- Null values precisam incluir string vazia, `"NA"`, `"N/A"`, `"nan"`

```python
from shared.io import read_csv_inep
df = read_csv_inep(csv_path)
```

**Atenção:** anos muito antigos (ex: Censo 1995–2008) usam separador `|`. Esses arquivos
têm schema incompatível e devem ser tratados em pipelines separados se necessário.

---

## Chaves primárias universais

| Campo    | Tipo      | Regra |
|----------|-----------|-------|
| `co_ies` | `Int64`   | Código INEP da IES. **Nunca String.** Chave estável mesmo quando nome muda. |
| `ano`    | `Int32`   | Ano de referência do censo/indicador. |

Todo silver e todo gold **deve ter** `co_ies` e `ano` sem nulos. Validar com
`shared.validate.assert_no_nulls(df, ["co_ies", "ano"])` antes de salvar.

---

## Colunas canônicas comuns

Após renomear no silver, usar sempre estes nomes:

| Nome canônico     | Tipo original INEP | Tipo canônico | Descrição |
|-------------------|--------------------|---------------|-----------|
| `co_ies`          | `CO_IES`           | `Int64`       | Código INEP da IES |
| `ano`             | `NU_ANO_CENSO`     | `Int32`       | Ano de referência |
| `no_ies`          | `NO_IES`           | `String`      | Nome da IES naquele ano |
| `sg_ies`          | `SG_IES`           | `String`      | Sigla (pode ser nula — ~18% das IES não têm) |
| `sg_uf`           | `SG_UF_IES`        | `String`      | Sigla da UF |
| `co_uf`           | `CO_UF_IES`        | `Int32`       | Código da UF |
| `co_municipio`    | `CO_MUNICIPIO_IES` | `Int64`       | Código IBGE do município |
| `no_municipio`    | `NO_MUNICIPIO_IES` | `String`      | Nome do município |
| `tp_categoria_adm`| `TP_CATEGORIA_ADMINISTRATIVA` | `Int32` | Código numérico da categoria |
| `tp_org_academica`| `TP_ORGANIZACAO_ACADEMICA`    | `Int32` | Código numérico da organização |
| `tp_rede`         | `TP_REDE`          | `Int32`       | 1=Pública, 2=Privada |
| `categoria_adm`   | decodificado       | `String`      | Label legível de `tp_categoria_adm` |
| `org_academica`   | decodificado       | `String`      | Label legível de `tp_org_academica` |
| `rede`            | decodificado       | `String`      | Label legível de `tp_rede` |

**Nomes originais do INEP só existem no `schema.py` e no `silver.py`.
A partir do silver, tudo usa nomes canônicos.**

---

## Colunas com variação histórica conhecida

Algumas colunas não existem em todos os anos. O comportamento correto é deixar nulo —
não remover a coluna nem imputar valor.

| Coluna          | Disponibilidade | Observação |
|-----------------|-----------------|------------|
| `tp_rede`       | 2023+           | Anos anteriores: derivar de `tp_categoria_adm` (≤3 = Pública, >3 = Privada) |
| `qt_doc_total`  | 2010+           | 2009 não publicou dados de docentes |
| `in_repositorio_inst` | 2015+     | Nulo para 2009–2014 |
| `in_busca_integrada`  | 2015+     | Nulo para 2009–2014 |
| `qt_periodico_eletronico` | 2015+ | Nulo para 2009–2014 |
| `sg_ies`        | todos os anos   | Mas ~18% das IES não têm sigla — nulo esperado |

---

## Concatenação multi-ano: usar `how="diagonal"`

Quando empilhar Parquets de anos diferentes, sempre usar `pl.concat(frames, how="diagonal")`.

```python
# CORRETO — tolera colunas ausentes entre anos
pl.concat(frames, how="diagonal")

# ERRADO — falha se qualquer coluna estiver ausente em algum ano
pl.concat(frames, how="vertical")
```

`diagonal` preenche com nulo as colunas ausentes em anos mais antigos, que é o
comportamento correto para dados históricos com schema evolutivo.

---

## Modelo de tabelas gold

### Tabelas de dimensão (uma linha por entidade)

| Tabela    | Chave PK | Descrição |
|-----------|----------|-----------|
| `dim_ies` | `co_ies` | Estado atual da IES (ano mais recente disponível) |

### Tabelas de histórico (uma linha por entidade × ano)

| Tabela     | Chave PK          | Descrição |
|------------|-------------------|-----------|
| `hist_ies` | `(co_ies, ano)`   | Atributos que mudam ao longo do tempo (nome, categoria, org. acadêmica) |

### Tabelas de fato (uma linha por entidade × ano, métricas)

| Tabela           | Chave PK        | Descrição |
|------------------|-----------------|-----------|
| `fact_censo_ies` | `(co_ies, ano)` | Métricas anuais do Censo: docentes, técnicos, biblioteca |
| `fact_enade`     | `(co_ies, ano)` | Métricas do ENADE por IES/ano *(a implementar)* |
| `fact_cpc`       | `(co_ies, ano)` | Conceito Preliminar de Curso *(a implementar)* |
| `fact_idd`       | `(co_ies, ano)` | Indicador de Diferença entre os Desempenhos *(a implementar)* |
| `fact_igc`       | `(co_ies, ano)` | Índice Geral de Cursos *(a implementar)* |

**Todo `fact_*` deve ter `co_ies` e `ano` como chave composta, sem nulos.**
Isso garante que qualquer join com `dim_ies` ou `hist_ies` funcione sem surpresas.

---

## Cobertura temporal por indicador

| Indicador       | Cobertura suportada | Observação |
|-----------------|---------------------|------------|
| Censo (IES)     | **2009–2024**       | Antes de 2009: sem `CO_IES`, schema incompatível |
| ENADE           | a definir           | |
| CPC             | a definir           | |
| IDD             | a definir           | |
| IGC             | a definir           | |

---

## Silver incremental (idempotência)

O silver só reprocessa um ano se:
1. O Parquet silver não existe ainda, ou
2. O arquivo bronze (CSV) foi modificado depois do Parquet silver

Use `--force` para forçar reprocessamento independente do timestamp.

O gold **sempre** recalcula tudo a partir dos silvers disponíveis. É rápido
porque lê Parquets, não CSVs brutos.

---

## Caminhos de dados

Controlados pelas variáveis de ambiente:

| Variável             | Padrão (dev local)      | Docker       |
|----------------------|-------------------------|--------------|
| `INEX_DATA_DIR`      | `../inex-ingest/data`   | `/data`      |
| `PIPELINES_DATA_DIR` | `./data`                | `/pipelines-data` |

Estrutura dentro de `PIPELINES_DATA_DIR`:

```
data/
  bronze/          ← criado pelo inex-ingest (via INEX_DATA_DIR)
  silver/
    censo_ies/     ← {ano}.parquet
    censo_fact/    ← {ano}.parquet
    enade/         ← {ano}.parquet  (a implementar)
  gold/
    dim_ies.parquet
    hist_ies.parquet
    fact_censo_ies.parquet
    fact_enade.parquet   (a implementar)
```

---

## Idioma

| Contexto | Idioma |
|----------|--------|
| Código, variáveis, funções, módulos | **inglês** |
| Comentários, docstrings, logs | **português** |
| Documentação `.md` | **português** |

---

## Git

- Nunca commitar dados (`data/` no `.gitignore`)
- Branches: `feat/{nome}` para features, `fix/{nome}` para correções
- Commits em português, imperativo: `"Adiciona silver do ENADE"`, `"Corrige derivação de tp_rede"`
