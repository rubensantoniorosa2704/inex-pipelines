"""
shared/paths.py — Resolução de caminhos baseada em variáveis de ambiente.

Em dev local:
  INEX_DATA_DIR      padrão: ../inex-ingest/data
  PIPELINES_DATA_DIR padrão: ./data

Em Docker:
  INEX_DATA_DIR=/data
  PIPELINES_DATA_DIR=/pipelines-data
"""

import os
from pathlib import Path

# Raiz dos dados bronze (produzidos pelo inex-ingest)
BRONZE_ROOT = Path(os.environ.get("INEX_DATA_DIR", "../inex-ingest/data"))

# Raiz dos dados silver/gold deste repo
DATA_ROOT = Path(os.environ.get("PIPELINES_DATA_DIR", "./data"))

SILVER_ROOT = DATA_ROOT / "silver"
GOLD_ROOT = DATA_ROOT / "gold"


def bronze_dir(dataset: str, year: int) -> Path:
    """Retorna o diretório bronze de um dataset/ano (criado pelo inex-ingest)."""
    return BRONZE_ROOT / dataset / str(year)


def silver_path(indicator: str, year: int) -> Path:
    """Retorna o caminho do Parquet silver de um indicador/ano."""
    return SILVER_ROOT / indicator / f"{year}.parquet"


def gold_path(indicator: str) -> Path:
    """Retorna o caminho do Parquet gold de um indicador."""
    return GOLD_ROOT / f"{indicator}.parquet"
