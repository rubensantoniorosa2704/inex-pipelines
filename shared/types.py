"""
shared/types.py — Tipos canônicos das colunas comuns entre indicadores.

Todos os pipelines devem usar estes nomes após renomear suas colunas no silver.
"""

import polars as pl

# Tipos canônicos das colunas compartilhadas por múltiplos indicadores
COMMON_SCHEMA: dict[str, pl.DataType] = {
    "co_ies": pl.Int64,
    "ano": pl.Int32,
    "no_ies": pl.String,
    "sg_ies": pl.String,
    "sg_uf": pl.String,
    "co_municipio": pl.Int64,
    "no_municipio": pl.String,
    "tp_categoria_adm": pl.Int32,
    "tp_org_academica": pl.Int32,
    "tp_rede": pl.Int32,
}
