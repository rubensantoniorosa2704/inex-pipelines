"""
shared/validate.py — Checagens reutilizáveis de qualidade dos dados.

Chamar antes de salvar qualquer Parquet silver ou gold.
"""

import polars as pl


class ValidationError(Exception):
    pass


def assert_not_empty(df: pl.DataFrame, label: str = "") -> None:
    """Garante que o DataFrame não está vazio."""
    if df.is_empty():
        raise ValidationError(f"DataFrame vazio{f' ({label})' if label else ''}")


def assert_required_columns(df: pl.DataFrame, columns: list[str], label: str = "") -> None:
    """Garante que todas as colunas obrigatórias estão presentes."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValidationError(
            f"Colunas obrigatórias ausentes{f' em {label}' if label else ''}: {missing}"
        )


def assert_no_nulls(df: pl.DataFrame, columns: list[str], label: str = "") -> None:
    """Garante que as colunas-chave não têm valores nulos."""
    for col in columns:
        null_count = df[col].null_count()
        if null_count > 0:
            raise ValidationError(
                f"Coluna '{col}' tem {null_count} nulo(s){f' em {label}' if label else ''}"
            )


def assert_schema(df: pl.DataFrame, expected: dict[str, pl.DataType], label: str = "") -> None:
    """Garante que as colunas têm os tipos esperados."""
    for col, expected_type in expected.items():
        if col not in df.columns:
            raise ValidationError(f"Coluna '{col}' ausente{f' em {label}' if label else ''}")
        actual_type = df[col].dtype
        if actual_type != expected_type:
            raise ValidationError(
                f"Coluna '{col}': esperado {expected_type}, encontrado {actual_type}"
                f"{f' em {label}' if label else ''}"
            )
