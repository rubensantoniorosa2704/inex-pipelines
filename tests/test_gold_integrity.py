"""
tests/test_gold_integrity.py — Testes de integridade das tabelas gold.

Validam que os dados processados estão corretos, consistentes e completos.
Rodam sobre os parquets gerados (precisam existir em data/gold/).

  pytest tests/test_gold_integrity.py -v
"""

import polars as pl
import pytest
from pathlib import Path

GOLD_DIR = Path("data/gold")


def _read(name: str) -> pl.DataFrame:
    path = GOLD_DIR / f"{name}.parquet"
    if not path.exists():
        pytest.skip(f"{path} não encontrado")
    return pl.read_parquet(path)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Chaves primárias únicas
# ─────────────────────────────────────────────────────────────────────────────


class TestPKsUnicas:
    def test_fact_enade(self):
        df = _read("fact_enade")
        assert df.select(["co_ies", "co_curso", "ano"]).is_unique().all()

    def test_fact_igc(self):
        df = _read("fact_igc")
        assert df.select(["co_ies", "ano"]).is_unique().all()

    def test_fact_idd(self):
        df = _read("fact_idd")
        # IDD pode ter duplicatas por (co_ies, ano, area) em anos antigos
        # Verificar apenas que não há linhas 100% duplicadas
        assert df.height == df.unique().height

    def test_dim_ies(self):
        df = _read("dim_ies")
        assert df["co_ies"].is_unique().all()

    def test_hist_ies(self):
        df = _read("hist_ies")
        assert df.select(["co_ies", "ano"]).is_unique().all()


# ─────────────────────────────────────────────────────────────────────────────
# 2. Chaves estrangeiras válidas
# ─────────────────────────────────────────────────────────────────────────────


class TestFKsValidas:
    @pytest.fixture(autouse=True)
    def _load_dim(self):
        self.dim_ies = _read("dim_ies")
        self.ies_validas = set(self.dim_ies["co_ies"].to_list())

    def test_fact_enade_co_ies_existe(self):
        df = _read("fact_enade")
        ies_enade = set(df["co_ies"].to_list())
        orphans = ies_enade - self.ies_validas
        # Tolerância: IES de 2004-2008 que fecharam antes do Censo 2009 (<10%)
        pct_orphans = len(orphans) / len(ies_enade) * 100
        assert pct_orphans < 10, f"{len(orphans)} IES órfãs ({pct_orphans:.1f}%)"

    def test_fact_igc_co_ies_existe(self):
        df = _read("fact_igc")
        ies_igc = set(df["co_ies"].to_list())
        orphans = ies_igc - self.ies_validas
        pct_orphans = len(orphans) / len(ies_igc) * 100
        assert pct_orphans < 1, f"{len(orphans)} IES órfãs ({pct_orphans:.1f}%)"

    def test_fact_censo_cursos_co_ies_existe(self):
        df = _read("fact_censo_cursos")
        ies_censo = set(df["co_ies"].to_list())
        orphans = ies_censo - self.ies_validas
        pct_orphans = len(orphans) / len(ies_censo) * 100
        assert pct_orphans < 1, f"{len(orphans)} IES órfãs ({pct_orphans:.1f}%)"


# ─────────────────────────────────────────────────────────────────────────────
# 3. Valores na escala correta
# ─────────────────────────────────────────────────────────────────────────────


class TestEscalas:
    def test_enade_notas_0_100(self):
        df = _read("fact_enade")
        for col in ["nt_ger_media", "nt_fg_media", "nt_ce_media"]:
            vals = df[col].drop_nulls()
            assert vals.min() >= 0, f"{col} tem valor < 0"
            assert vals.max() <= 100, f"{col} tem valor > 100"

    def test_igc_0_5(self):
        df = _read("fact_igc")
        vals = df["igc_continuo"].drop_nulls()
        assert vals.min() >= 0, "IGC contínuo < 0"
        assert vals.max() <= 5, "IGC contínuo > 5"

    def test_igc_faixa_1_5(self):
        df = _read("fact_igc")
        vals = df["igc_faixa"].drop_nulls()
        assert vals.min() >= 1, "IGC faixa < 1"
        assert vals.max() <= 5, "IGC faixa > 5"

    def test_enade_percentil_0_100(self):
        df = _read("fact_enade")
        vals = df["percentil_grupo"].drop_nulls()
        assert vals.min() >= 0, "Percentil < 0"
        assert vals.max() <= 100, "Percentil > 100"

    def test_enade_tx_participacao_0_1(self):
        df = _read("fact_enade")
        vals = df["tx_participacao"].drop_nulls()
        assert vals.min() >= 0, "Taxa participação < 0"
        assert vals.max() <= 1.0, "Taxa participação > 1"

    def test_perfil_proporcoes_0_1(self):
        df = _read("fact_enade_perfil")
        prop_cols = [c for c in df.columns if c.startswith("qe_i") and "respondentes" not in c]
        for col in prop_cols[:20]:  # Amostra de 20 colunas
            vals = df[col].drop_nulls()
            if vals.len() == 0:
                continue
            assert vals.min() >= 0, f"{col} tem proporção < 0"
            assert vals.max() <= 1.0, f"{col} tem proporção > 1"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Regras de negócio
# ─────────────────────────────────────────────────────────────────────────────


class TestRegrasNegocio:
    def test_presentes_menor_igual_inscritos(self):
        df = _read("fact_enade")
        violacoes = df.filter(pl.col("qt_presentes") > pl.col("qt_inscritos"))
        assert violacoes.height == 0, f"{violacoes.height} cursos com presentes > inscritos"

    def test_todos_anos_enade(self):
        """19 anos: 2004-2023 exceto 2020."""
        df = _read("fact_enade")
        anos = sorted(df["ano"].unique().to_list())
        assert 2004 in anos
        assert 2023 in anos
        assert 2020 not in anos
        assert len(anos) == 19

    def test_nenhum_null_em_chaves(self):
        df = _read("fact_enade")
        for col in ["co_ies", "co_curso", "ano"]:
            assert df[col].null_count() == 0, f"{col} tem nulls"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Cruzamento com dados oficiais (CPC)
# ─────────────────────────────────────────────────────────────────────────────


class TestCruzamentoCPC:
    def test_notas_batem_com_cpc(self):
        """Nota FG/CE do fact_enade deve ser próxima da nota_bruta do CPC."""
        enade = _read("fact_enade")
        cpc = _read("fact_cpc")

        cpc_com_curso = cpc.filter(
            (pl.col("co_curso").is_not_null())
            & (pl.col("co_curso") > 0)
            & (pl.col("ano") >= 2017)
        ).select(["co_ies", "co_curso", "ano", "nota_bruta_fg"])

        check = cpc_com_curso.join(
            enade.select(["co_ies", "co_curso", "ano", "nt_fg_media"]),
            on=["co_ies", "co_curso", "ano"],
            how="inner",
        ).filter(pl.col("nota_bruta_fg").is_not_null() & pl.col("nt_fg_media").is_not_null())

        if check.height == 0:
            pytest.skip("Sem dados cruzados")

        diff = (check["nota_bruta_fg"] - check["nt_fg_media"]).abs()
        assert diff.mean() < 0.5, f"Diferença média muito alta: {diff.mean():.4f}"
        assert diff.max() < 1.0, f"Diferença máxima muito alta: {diff.max():.4f}"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Silvers existem e não estão vazios
# ─────────────────────────────────────────────────────────────────────────────


SILVER_DIR = Path("data/silver")


class TestSilvers:
    def test_enade_microdados_19_anos(self):
        silver_dir = SILVER_DIR / "enade_microdados"
        if not silver_dir.exists():
            pytest.skip("Silver enade_microdados não encontrado")
        parquets = list(silver_dir.glob("*.parquet"))
        assert len(parquets) == 19, f"Esperado 19 silvers, encontrado {len(parquets)}"

    def test_silvers_nao_vazios(self):
        silver_dir = SILVER_DIR / "enade_microdados"
        if not silver_dir.exists():
            pytest.skip("Silver enade_microdados não encontrado")
        for f in silver_dir.glob("*.parquet"):
            df = pl.read_parquet(f)
            assert df.height > 0, f"{f.name} está vazio"

    def test_silver_tem_colunas_obrigatorias(self):
        silver_dir = SILVER_DIR / "enade_microdados"
        if not silver_dir.exists():
            pytest.skip("Silver enade_microdados não encontrado")
        required = ["ano", "co_ies", "co_curso", "tp_pres", "nt_ger"]
        for f in silver_dir.glob("*.parquet"):
            df = pl.read_parquet(f)
            for col in required:
                assert col in df.columns, f"{f.name} faltando coluna {col}"


# ─────────────────────────────────────────────────────────────────────────────
# 7. Consistência entre camadas
# ─────────────────────────────────────────────────────────────────────────────


class TestConsistenciaEntreComadas:
    def test_total_presentes_bate(self):
        """Soma de presentes no gold deve bater com soma dos silvers."""
        gold = _read("fact_enade")
        total_gold = gold["qt_presentes"].sum()

        silver_dir = SILVER_DIR / "enade_microdados"
        if not silver_dir.exists():
            pytest.skip("Silver não encontrado")

        total_silver = 0
        for f in silver_dir.glob("*.parquet"):
            df = pl.read_parquet(f)
            total_silver += df.filter(pl.col("tp_pres") == 555).height

        assert total_gold == total_silver, (
            f"Gold ({total_gold:,}) != Silver ({total_silver:,})"
        )
