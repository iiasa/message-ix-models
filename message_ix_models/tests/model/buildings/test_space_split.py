"""Tests for :mod:`message_ix_models.model.buildings.space_split`."""

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pytest

from message_ix_models.model.buildings.space_split import (
    BRIDGE_TEC,
    SPLIT_MAP,
    _demand_to_final_energy_iamc,
    aeei_factor,
    bridge_parameters,
    rebase_aeei,
    space_split_shares,
    split_demand,
)

if TYPE_CHECKING:
    import ixmp
    from message_ix import Scenario

SPLIT_YEARS = [2030, 2035, 2040, 2050]


def shares_frame(nodes: list[str], values: dict[int, float]) -> pd.DataFrame:
    """Retained share ``s`` per year, the same for both parents and all nodes."""
    rows = [(n, y, s) for n in nodes for y, s in values.items()]
    df = pd.DataFrame(rows, columns=["node", "year", "s"])
    for parent in SPLIT_MAP:
        df[f"s_{parent}"] = df["s"]
        df[f"f_{parent}"] = 1.0 - df["s"]
    return df.drop(columns="s")


@pytest.mark.parametrize("ssp", ["SSP1", "SSP2", "SSP3"])
def test_space_split_shares(ssp: str) -> None:
    result = space_split_shares(ssp, SPLIT_YEARS)

    assert result["node"].nunique() == 12
    assert sorted(result["year"].unique()) == SPLIT_YEARS
    for parent in SPLIT_MAP:
        f, s = result[f"f_{parent}"], result[f"s_{parent}"]
        np.testing.assert_allclose(f + s, 1.0)
        assert ((f > 0) & (s > 0)).all()


def test_space_split_shares_before_calibration_raises() -> None:
    with pytest.raises(ValueError, match="starts later"):
        space_split_shares("SSP2", [2020, 2030])


def test_split_demand_conserves_parent_demand() -> None:
    demand = pd.DataFrame(
        {
            "node": ["R12_AFR"] * 4,
            "commodity": ["rc_spec", "rc_spec", "rc_therm", "rc_therm"],
            "level": ["useful"] * 4,
            "year": [2025, 2030, 2025, 2030],
            "time": ["year"] * 4,
            "value": [10.0, 20.0, 30.0, 40.0],
            "unit": ["GWa"] * 4,
        }
    )
    shares = shares_frame(["R12_AFR"], {2030: 0.75})

    residual, carved = split_demand(demand, shares, [2030])

    assert list(residual.columns) == list(demand.columns)
    # Years before the split are unchanged
    before = residual[residual["year"] == 2025].set_index("commodity")["value"]
    assert before.to_dict() == {"rc_spec": 10.0, "rc_therm": 30.0}
    # In split years, residual + carved equals the parent demand
    after = residual[residual["year"] == 2030].set_index("commodity")["value"]
    moved = carved.set_index("commodity")["value"]
    assert after.to_dict() == {"rc_spec": 15.0, "rc_therm": 30.0}
    assert moved.to_dict() == {"space_cooling": 5.0, "space_heating": 10.0}


def test_split_demand_missing_share_raises() -> None:
    demand = pd.DataFrame(
        {"node": ["R12_AFR"] * 2, "commodity": ["rc_spec", "rc_therm"]}
    ).assign(year=2030, value=1.0)
    shares = shares_frame(["R12_CHN"], {2030: 0.5})

    with pytest.raises(ValueError, match="Missing shares"):
        split_demand(demand, shares, [2030])


def test_bridge_parameters_are_identity() -> None:
    nodes = ["R12_AFR", "R12_CHN"]
    inp, out = bridge_parameters(nodes, SPLIT_YEARS)

    assert len(inp) == len(out) == len(SPLIT_MAP) * len(nodes) * len(SPLIT_YEARS)
    assert (inp["value"] == 1.0).all() and (out["value"] == 1.0).all()
    assert (inp["year_vtg"] == inp["year_act"]).all()
    for parent, child in SPLIT_MAP.items():
        tec = BRIDGE_TEC[child]
        assert set(inp.loc[inp["technology"] == tec, "commodity"]) == {parent}
        assert set(out.loc[out["technology"] == tec, "commodity"]) == {child}


@pytest.fixture
def aeei() -> pd.DataFrame:
    """``aeei`` for one node: the base period 2025, then the split years."""
    years = [2025] + SPLIT_YEARS
    rows = [
        ("R12_AFR", sector, y, 0.01 + 0.002 * i)
        for sector in ["rc_spec", "rc_therm", "i_spec"]
        for i, y in enumerate(years)
    ]
    return pd.DataFrame(rows, columns=["node", "sector", "year", "value"]).assign(
        unit="-"
    )


@pytest.fixture
def duration_period() -> pd.DataFrame:
    return pd.DataFrame(
        {"year": [2025, 2030, 2035, 2040, 2050], "value": [5, 5, 5, 5, 10]}
    )


def test_rebase_aeei_scales_factor_by_retained_share(
    aeei: pd.DataFrame, duration_period: pd.DataFrame
) -> None:
    share = {2030: 0.8, 2035: 0.7, 2040: 0.75, 2050: 0.6}
    rebased = rebase_aeei(
        aeei, shares_frame(["R12_AFR"], share), duration_period, SPLIT_YEARS
    )

    keys = ["node", "sector", "year"]
    after = pd.concat(
        [
            aeei.set_index(keys).drop(rebased.set_index(keys).index),
            rebased.set_index(keys),
        ]
    ).reset_index()
    before_f = aeei_factor(aeei, duration_period).set_index(keys)["factor"]
    after_f = aeei_factor(after, duration_period).set_index(keys)["factor"]

    for parent in SPLIT_MAP:
        # The base period is untouched; split years scale by the retained share
        assert after_f[("R12_AFR", parent, 2025)] == before_f[("R12_AFR", parent, 2025)]
        for year, s in share.items():
            np.testing.assert_allclose(
                after_f[("R12_AFR", parent, year)],
                s * before_f[("R12_AFR", parent, year)],
            )
    # Other sectors are not rebased
    assert set(rebased["sector"]) == set(SPLIT_MAP)


def test_rebase_aeei_requires_every_split_year(
    aeei: pd.DataFrame, duration_period: pd.DataFrame
) -> None:
    partial = aeei[aeei["year"] != 2040]
    with pytest.raises(ValueError, match="do not cover"):
        rebase_aeei(
            partial,
            shares_frame(["R12_AFR"], dict.fromkeys(SPLIT_YEARS, 0.8)),
            duration_period,
            SPLIT_YEARS,
        )


def test_demand_to_final_energy_iamc_converts_gwa_to_ej_per_year() -> None:
    demand = pd.DataFrame(
        {"node": ["R12_AFR", "R12_AFR"], "year": [2030, 2035], "value": [1.0, 2.0]}
    )

    result = _demand_to_final_energy_iamc(
        demand, "Final Energy|Residential and Commercial|Cooling"
    )

    assert result["unit"].unique().tolist() == ["EJ/yr"]
    assert result["variable"].unique().tolist() == [
        "Final Energy|Residential and Commercial|Cooling"
    ]
    np.testing.assert_allclose(result["value"], [0.0315576, 0.0631152])


@pytest.fixture
def scenario(request: pytest.FixtureRequest, test_mp: "ixmp.Platform") -> "Scenario":
    """A one-node scenario with rc_spec/rc_therm demand and MACRO items."""
    from message_ix import Scenario
    from message_ix.models import MACRO

    node, years = "R12_AFR", [2025, 2030, 2035]
    test_mp.add_region(node, "region", "World")
    for unit in ("GWa", "-"):
        test_mp.add_unit(unit)

    s = Scenario(test_mp, "space_split", request.node.name, version="new")
    MACRO.initialize(s)
    s.add_horizon(year=years, firstmodelyear=2030)
    s.add_spatial_sets({"region": [node]})
    s.add_set("commodity", list(SPLIT_MAP))
    s.add_set("level", "useful")
    s.add_set("mode", "M1")
    s.add_set("technology", "rc_supply")
    s.add_set("sector", list(SPLIT_MAP))
    s.add_set("mapping_macro_sector", [[c, c, "useful"] for c in SPLIT_MAP])

    base = pd.DataFrame(
        [(node, c, y) for c in SPLIT_MAP for y in years],
        columns=["node", "commodity", "year"],
    )
    s.add_par(
        "demand",
        base.assign(level="useful", time="year", value=100.0, unit="GWa"),
    )
    s.add_par(
        "aeei",
        base.rename(columns={"commodity": "sector"}).assign(value=0.02, unit="-"),
    )
    s.commit("Initial")
    return s


def test_apply_space_demand_split(scenario: "Scenario") -> None:
    from message_ix_models.model.buildings.space_split import apply_space_demand_split

    record = apply_space_demand_split(scenario, reference_scenario="SSP2")

    assert record["verification"]["passed"], record["verification"]["checks"]
    assert record["split_years"] == [2030, 2035]
    assert set(SPLIT_MAP.values()) <= set(scenario.set("commodity"))
    # MACRO sectors are untouched
    assert sorted(scenario.set("sector")) == sorted(SPLIT_MAP)

    demand = scenario.par("demand").pivot_table(
        index="year", columns="commodity", values="value"
    )
    np.testing.assert_allclose(demand.loc[2025, "rc_spec"], 100.0)
    np.testing.assert_allclose(
        demand.loc[2030, "rc_spec"] + demand.loc[2030, "space_cooling"], 100.0
    )

    with pytest.raises(ValueError, match="already on the scenario"):
        apply_space_demand_split(scenario, reference_scenario="SSP2")


def test_apply_space_cids(scenario: "Scenario") -> None:
    from message_ix_models.model.buildings.space_split import (
        apply_space_cids,
        apply_space_demand_split,
    )

    with pytest.raises(ValueError, match="split not applied"):
        apply_space_cids(scenario, pd.DataFrame(), pd.DataFrame())

    apply_space_demand_split(scenario, reference_scenario="SSP2")
    residual = scenario.par("demand", filters={"commodity": list(SPLIT_MAP)})
    cid = pd.DataFrame({"node": "R12_AFR", "year": [2030, 2035], "value": 7.0})

    with pytest.raises(ValueError, match="does not cover"):
        apply_space_cids(scenario, cid.iloc[:1], cid)

    record = apply_space_cids(scenario, cid, cid.assign(value=9.0))

    demand = scenario.par("demand").set_index(["commodity", "year"])["value"]
    assert demand.loc[("space_cooling", 2030)] == 7.0
    assert demand.loc[("space_heating", 2035)] == 9.0
    assert record["space_cooling"]["cid_total_GWa"] == 14.0
    # The rc_* residual is not touched
    pd.testing.assert_frame_equal(
        scenario.par("demand", filters={"commodity": list(SPLIT_MAP)}), residual
    )
    ts = scenario.timeseries(variable="Final Energy|Residential and Commercial|Cooling")
    assert sorted(ts["year"]) == [2030, 2035]
