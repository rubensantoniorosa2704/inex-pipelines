"""
shared/years.py — Parsing de especificações de anos para os pipelines.

Suporta formatos: '2023', '2020-2023', '2018,2020-2022'
"""

import re


def parse_years(year_spec: str) -> list[int]:
    """
    Converte especificação de anos em lista de inteiros.
    Suporta: '2023', '2020-2023', '2018,2020-2022'
    """
    years = []
    for part in year_spec.split(","):
        part = part.strip()
        if re.match(r"^\d{4}-\d{4}$", part):
            start, end = part.split("-")
            years.extend(range(int(start), int(end) + 1))
        elif re.match(r"^\d{4}$", part):
            years.append(int(part))
        else:
            raise ValueError(f"Formato de ano inválido: '{part}'. Use '2023' ou '2020-2023'")
    return sorted(set(years))
