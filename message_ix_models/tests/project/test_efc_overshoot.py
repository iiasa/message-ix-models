from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pytest

from message_ix_models.project.efc_overshoot import workflow as wf_mod
from message_ix_models.project.efc_overshoot.workflow import (
    MTC_PER_GTCO2,
    budget_mtc,
    check_trajectory,
    configure_context,
    gtco2_to_mtc,
    load_config,
    tax_from_price,
    window_years,
)

MODEL_YEARS = [2030, 2035, 2040, 2045, 2050, 2055, 2060, 2070, 2080, 2090, 2100, 2110]


def test_gtco2_to_mtc() -> None:
    assert gtco2_to_mtc(44 / 12) == pytest.approx(1000)
    assert MTC_PER_GTCO2 == pytest.approx(272.727, abs=1e-3)


def test_window_years() -> None:
    assert window_years(MODEL_YEARS, 2030, 2100) == MODEL_YEARS[:-1]
    with pytest.raises(ValueError, match="No model years"):
        window_years(MODEL_YEARS, 2101, 2105)


def test_budget_mtc() -> None:
    # 2030 period (2026–2030) to 2100: 7 five-year + 4 ten-year periods = 75 years
    duration = pd.Series([5.0] * 7 + [10.0] * 4)
    value = budget_mtc(400, duration)
    assert value == pytest.approx(400 * MTC_PER_GTCO2 / 75)
    # Inverse: the bound times the window length gives back F in Gt CO₂
    assert value * duration.sum() / MTC_PER_GTCO2 == pytest.approx(400)

    with pytest.raises(ValueError, match="total duration"):
        budget_mtc(400, pd.Series(dtype=float))
    with pytest.raises(ValueError, match="total duration"):
        budget_mtc(400, pd.Series([float("nan")]))


def _emiss(years, lvl=1.0, **kw) -> pd.DataFrame:
    base = dict(node="World", emission="TCE_CO2", type_tec="all", mrg=0.0)
    base.update(kw)
    return pd.DataFrame([dict(base, year=y, lvl=lvl) for y in years])


def test_check_trajectory() -> None:
    check_trajectory(_emiss(MODEL_YEARS), MODEL_YEARS)

    with pytest.raises(ValueError, match=r"\[2110\]"):
        check_trajectory(_emiss(MODEL_YEARS[:-1]), MODEL_YEARS)
    # Rows for the wrong node or emission do not count
    with pytest.raises(ValueError, match="lacks"):
        check_trajectory(_emiss(MODEL_YEARS, node="R12_CHN"), MODEL_YEARS)
    with pytest.raises(ValueError, match="lacks"):
        check_trajectory(_emiss(MODEL_YEARS, emission="TCE"), MODEL_YEARS)
    # A NaN level is a missing year
    nan = _emiss(MODEL_YEARS)
    nan.loc[nan.year == 2050, "lvl"] = float("nan")
    with pytest.raises(ValueError, match=r"\[2050\]"):
        check_trajectory(nan, MODEL_YEARS)
    with pytest.raises(ValueError, match="lacks"):
        check_trajectory(_emiss([]).reindex(columns=_emiss([2030]).columns), [2030])


def _price() -> pd.DataFrame:
    rows = [
        ("World", "TCE_CO2", "all", 2030, 100.0),
        ("World", "TCE_CO2", "all", 2040, 200.0),
        # Must not become non-CO₂ taxes
        ("World", "TCE_CO2", "other", 2030, 999.0),
        ("R12_CHN", "TCE_CO2", "all", 2030, 999.0),
        ("R12_GLB", "CO2_shipping_IMO", "all", 2030, 999.0),
        ("World", "TCE_non-CO2", "all", 2030, 999.0),
    ]
    return pd.DataFrame(
        rows, columns=["node", "type_emission", "type_tec", "year", "lvl"]
    ).assign(mrg=0.0)


def test_tax_from_price() -> None:
    tax = tax_from_price(_price())
    assert tax.to_dict("list") == dict(
        node=["World", "World"],
        type_emission=["TCE_non-CO2", "TCE_non-CO2"],
        type_tec=["all", "all"],
        type_year=["2030", "2040"],
        value=[100.0, 200.0],
        unit=["USD/tC", "USD/tC"],
    )


@pytest.mark.parametrize(
    "mutate, match",
    [
        (lambda df: df[df.node != "World"], "No PRICE_EMISSION"),
        (lambda df: df.assign(lvl=0.0), "zero in every year"),
        (lambda df: df.assign(lvl=float("nan")), "NaN"),
    ],
)
def test_tax_from_price_raises(mutate, match) -> None:
    with pytest.raises(ValueError, match=match):
        tax_from_price(mutate(_price()))


def test_load_config_default() -> None:
    config = load_config()
    assert config["platform"] == "message-jdbc-pg"
    assert config["budget"]["type_year"] != "cumulative"
    assert set(config["levels"]) == {"p450", "p550", "p650"}


@pytest.mark.parametrize(
    "text, match",
    [
        ("model: m\n", "lacks"),
        (
            "model: m\nparent: {}\nfirst_model_year: 2030\nlevels: {}\n"
            "budget: {first_year: 2100, last_year: 2030}\n",
            "is empty",
        ),
        (
            "model: m\nparent: {}\nfirst_model_year: 2030\nlevels: {}\n"
            "budget: {first_year: 2030, last_year: 2100}\n",
            "no peak levels",
        ),
        (
            "model: m\nparent: {}\nfirst_model_year: 2030\n"
            "budget: {first_year: 2030, last_year: 2100}\n"
            "levels: {p1: {F: 400}}\n",
            "needs both",
        ),
        (
            "model: m\nparent: {}\nfirst_model_year: 2030\n"
            "budget: {first_year: 2030, last_year: 2100}\n"
            "levels: {p1: {F: 400, floor: 2}}\n",
            "must be ≤ 0",
        ),
    ],
)
def test_load_config_raises(tmp_path, text, match) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(text)
    with pytest.raises(ValueError, match=match):
        load_config(path)


def test_configure_context_requires_platform() -> None:
    context = SimpleNamespace(platform_info={}, model=SimpleNamespace())
    with pytest.raises(ValueError, match="No platform"):
        configure_context(cast(Any, context), dict(platform=None))


def test_generate(test_context) -> None:
    config = dict(load_config(), platform="pg-test")

    wf = wf_mod.generate(test_context, config)

    assert test_context.platform_info["name"] == "pg-test"
    assert test_context.run_reporting_only is False

    def step(name):
        return wf.graph[name][0]

    assert step("parent").scenario_info == dict(
        model="MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12 NGFS C3",
        scenario="baseline_MT",
        version=1,
    )
    assert step("parent").platform_info["name"] == "pg-test"
    assert step("baseline fmy").clone == dict(
        keep_solution=True, shift_first_model_year=2030
    )
    for level in config["levels"]:
        # EN2 and EN3 read the previous solution; base and EN1 must not carry one
        assert step(f"{level} base").clone == dict(keep_solution=False)
        assert step(f"{level} EN1").clone == dict(keep_solution=False)
        assert step(f"{level} EN2").clone == dict(keep_solution=True)
        assert step(f"{level} EN3").clone == dict(keep_solution=True)
        assert step(f"{level} EN3").kwargs == dict(level=level)
        assert step(f"{level} EN3").scenario_info["scenario"] == f"os_{level}_EN3"
        assert wf.graph[f"{level} EN1"][2] == f"{level} base"


class FakeScenario:
    """Records calls in order; returns canned data."""

    def __init__(self, events: list, var=None, par=None) -> None:
        self.events = events
        self._var = var or {}
        self._par = par or {}

    def var(self, name, filters=None):
        self.events.append(f"var:{name}")
        return self._var[name]

    def par(self, name, filters=None):
        self.events.append(f"par:{name}")
        return self._par.get(name, pd.DataFrame())

    def remove_solution(self):
        self.events.append("remove_solution")

    @contextmanager
    def transact(self, message):
        self.events.append("transact")
        yield

    def add_par(self, name, df):
        self.events.append(f"add_par:{name}:{len(df)}")

    def remove_par(self, name, df):
        self.events.append(f"remove_par:{name}:{len(df)}")

    def set_meta(self, data):
        self.events.append("set_meta")


@pytest.fixture
def no_solve(monkeypatch):
    events: list = []
    monkeypatch.setattr(wf_mod, "_solve", lambda c, s: events.append("solve"))
    monkeypatch.setattr(wf_mod, "code_revision", lambda: "rev")
    return events


def test_en3_reads_price_before_removing_solution(no_solve) -> None:
    events = no_solve
    s = FakeScenario(events, var={"PRICE_EMISSION": _price()})

    wf_mod.en3(cast(Any, SimpleNamespace()), cast(Any, s), level="p550")

    assert events == [
        "var:PRICE_EMISSION",
        "remove_solution",
        "transact",
        "add_par:tax_emission:2",
        "solve",
        "set_meta",
    ]


def test_en2_refuses_incomplete_trajectory(monkeypatch, no_solve) -> None:
    import message_ix_models
    from message_ix_models.project.engage import workflow as engage

    called = []
    monkeypatch.setattr(engage, "step_2", lambda *a: called.append(a))
    monkeypatch.setattr(
        message_ix_models, "ScenarioInfo", lambda s: SimpleNamespace(Y=MODEL_YEARS)
    )
    context = SimpleNamespace(
        model=SimpleNamespace(relation_global_co2=wf_mod.GLOBAL_CO2_RELATION),
        efc_overshoot=dict(budget=dict(type_year="cumulative_os")),
    )
    s = FakeScenario(no_solve, var={"EMISS": _emiss(MODEL_YEARS[:-1])})

    with pytest.raises(ValueError, match=r"\[2110\]"):
        wf_mod.en2(cast(Any, _ItemNamespace(context)), cast(Any, s), level="p550")
    assert called == []


def test_en2_refuses_other_floor_relation(no_solve) -> None:
    context = _ItemNamespace(
        SimpleNamespace(
            model=SimpleNamespace(relation_global_co2="CO2_Emission"),
            efc_overshoot=dict(budget=dict(type_year="cumulative_os")),
        )
    )
    with pytest.raises(ValueError, match="would remove 'CO2_Emission'"):
        wf_mod.en2(cast(Any, context), cast(Any, FakeScenario(no_solve)), level="p550")


class _ItemNamespace:
    """Attribute and item access, as on :class:`.Context`."""

    def __init__(self, ns: SimpleNamespace) -> None:
        self.__dict__.update(vars(ns))

    def __getitem__(self, name):
        return getattr(self, name)


def _context(**config) -> _ItemNamespace:
    base = dict(
        budget=dict(type_year="cumulative_os", first_year=2030, last_year=2100),
        levels=dict(p550=dict(F=400, floor=-3.3)),
        bound_after_window=0.0,
    )
    base.update(config)
    return _ItemNamespace(
        SimpleNamespace(
            model=SimpleNamespace(relation_global_co2=wf_mod.GLOBAL_CO2_RELATION),
            efc_overshoot=base,
        )
    )


def test_step_0_removes_only_climate_policy(monkeypatch, no_solve) -> None:
    from message_ix_models.tools import add_CO2_emission_constraint

    calls = []
    monkeypatch.setattr(
        add_CO2_emission_constraint, "main", lambda s, **kw: calls.append(kw)
    )
    monkeypatch.setattr(wf_mod, "identify_nodes", lambda s: "R12")

    class Scen(FakeScenario):
        def par(self, name, filters=None):
            # The filter, not the data, decides what is removed
            assert filters == {"type_emission": list(wf_mod.CLIMATE_TYPE_EMISSION)}
            return super().par(name, filters)

    bound = pd.DataFrame([dict(node="World", type_emission="TCE")] * 2)
    s = Scen(no_solve, par={"bound_emission": bound})

    wf_mod.step_0(cast(Any, _context()), cast(Any, s), level="p550")

    assert "remove_par:bound_emission:2" in no_solve
    assert not any(e.startswith("remove_par:tax_emission") for e in no_solve)
    assert calls == [
        dict(
            relation_name="CO2_Emission_Global_Total",
            constraint_value=pytest.approx(-3.3 * MTC_PER_GTCO2),
            type_rel="lower",
            reg="R12_GLB",
        )
    ]


def test_en1_budget_and_after_window(monkeypatch, no_solve) -> None:
    import message_ix_models
    from message_ix_models.tools import add_budget

    budgets = []
    monkeypatch.setattr(add_budget, "main", lambda s, v, **kw: budgets.append((v, kw)))
    monkeypatch.setattr(
        message_ix_models, "ScenarioInfo", lambda s: SimpleNamespace(Y=MODEL_YEARS)
    )
    added = {}

    class Scen(FakeScenario):
        def par(self, name, filters=None):
            assert name == "duration_period"
            assert filters == {"year": MODEL_YEARS[:-1]}
            return pd.DataFrame(
                dict(year=filters["year"], value=[5.0] * 7 + [10.0] * 4)
            )

        def add_set(self, name, data):
            added[name] = data

        def add_par(self, name, df):
            added[name] = df

    wf_mod.en1(cast(Any, _context()), cast(Any, Scen(no_solve)), level="p550")

    ((value, kw),) = budgets
    assert value == pytest.approx(400 * MTC_PER_GTCO2 / 75)
    assert kw == dict(
        type_emission="TCE_CO2", type_year="cumulative_os", unit="Mt C/yr"
    )
    assert added["type_year"] == "cumulative_os"
    assert added["cat_year"]["year"].tolist() == MODEL_YEARS[:-1]
    assert added["bound_emission"][["type_year", "value"]].to_dict("records") == [
        dict(type_year="2110", value=0.0)
    ]
    assert no_solve[-2:] == ["solve", "set_meta"]


@pytest.mark.parametrize(
    "covered, ok", [(MODEL_YEARS, True), (MODEL_YEARS[:-1], False)]
)
def test_en2_removes_window_budget(monkeypatch, no_solve, covered, ok) -> None:
    import message_ix_models
    from message_ix_models.project.engage import workflow as engage

    monkeypatch.setattr(engage, "step_2", lambda c, s, cfg: no_solve.append("step_2"))
    monkeypatch.setattr(
        message_ix_models, "ScenarioInfo", lambda s: SimpleNamespace(Y=MODEL_YEARS)
    )

    class Scen(FakeScenario):
        def par(self, name, filters=None):
            if filters == {"type_year": ["cumulative_os"]}:
                return pd.DataFrame([dict(type_year="cumulative_os")])
            # Annual bounds as step_2 would have written them
            return pd.DataFrame(dict(type_year=[str(y) for y in covered]))

    s = Scen(no_solve, var={"EMISS": _emiss(MODEL_YEARS)})
    if ok:
        wf_mod.en2(cast(Any, _context()), cast(Any, s), level="p550")
        i = no_solve.index("step_2")
        assert no_solve[i + 1 : i + 3] == ["transact", "remove_par:bound_emission:1"]
        assert no_solve[-2:] == ["solve", "set_meta"]
    else:
        with pytest.raises(ValueError, match="not the model years"):
            wf_mod.en2(cast(Any, _context()), cast(Any, s), level="p550")
        assert "solve" not in no_solve
