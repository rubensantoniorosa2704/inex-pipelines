"""
shared/io.py — Leitura e escrita padronizada de Parquet e CSV.

Toda pipeline usa estas funções para garantir consistência
de encoding, separador e compressão entre indicadores.
"""

from pathlib import Path

import polars as pl


def read_parquet(path: Path | str) -> pl.DataFrame:
    return pl.read_parquet(path)


def write_parquet(df: pl.DataFrame, path: Path | str) -> None:
    """Salva DataFrame como Parquet com compressão zstd, criando diretórios se necessário."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd")


def read_csv_inep(path: Path | str, **kwargs) -> pl.DataFrame:
    """
    Lê CSVs do INEP com as configurações padrão:
    - separador: ponto-e-vírgula
    - encoding: Latin-1 (iso-8859-1) — padrão histórico dos arquivos INEP
    - null values: string vazia, 'NA', 'N/A', 'nan'
    """
    return pl.read_csv(
        path,
        separator=";",
        encoding="latin1",
        null_values=["", "NA", "N/A", "nan"],
        infer_schema_length=10_000,
        **kwargs,
    )
