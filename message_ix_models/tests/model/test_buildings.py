from collections import namedtuple
from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd
import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.buildings import Config, _mpd, sturm
from message_ix_models.model.buildings.build import (
    get_spec,
    get_tech_groups,
    get_techs,
    main,
    prepare_data_B,
)
from message_ix_models.model.buildings.report import (
    configure_legacy_reporting,
    report2,
    report3,
)
from message_ix_models.testing import bare_res

# TODO Avoid cross-imports from test modules; move these items to a common location.
from message_ix_models.tests.model.test_bmt import (
    _add_buildings_tech_set,
    _add_materials_commodities,
    _add_minimal_rc_pars,
    _minimal_buildings_data,
    bmt_context,  # noqa: F401
    bmt_context_with_materials,  # noqa: F401
)
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from pathlib import Path

    from message_ix import Scenario

    from message_ix_models import Context

MARK = {
    0: pytest.mark.xfail(
        reason="Code needs update to work with message_ix_models.report.legacy"
    )
}


@pytest.fixture(scope="function")
def buildings_context(test_context: "Context") -> Generator["Context", None, None]:
    """A version of :func:`.test_context` with a :class:`.buildings.Config` stored."""
    test_context["buildings"] = Config(sturm_scenario="")

    yield test_context

    test_context.pop("buildings")


@pytest.mark.parametrize("commodity", [None, "gas"])
def test_get_techs(buildings_context: "Context", commodity: str | None) -> None:
    ctx = buildings_context
    ctx.regions = "R12"
    spec = get_spec(ctx)
    result = get_techs(spec, commodity)

    # Generated technologies with buildings sector and end-use
    assert "gas_resid_cook" in result

    # Generated technologies for residuals of corresponding *_rc in the base model spec
    assert "gas_afofi" in result or "gas_afofio" in result


@pytest.mark.parametrize(
    "args, present, absent",
    (
        # Default values of arguments, i.e. include="commodity enduse", legacy=False
        (dict(), {"rc", "comm hydrogen", "resid hotwater"}, set()),
        # As used e.g. in buildings reporting
        (dict(include="enduse"), {"resid hotwater", "rc"}, {"comm coal"}),
        # As used e.g. in legacy reporting. Assert that names like "h2" are used instead
        # of "hydrogen", and that end-use groups are not included.
        pytest.param(
            dict(include="commodity", legacy=True),
            {"afofi", "comm h2", "resid heat"},
            {"comm other_uses"},
            marks=MARK[0],
        ),
    ),
)
def test_get_tech_groups(
    test_context: "Context", args: dict, present: set[str], absent: set[str]
) -> None:
    test_context.buildings = Config(sturm_scenario="")
    test_context.regions = "R12"

    spec = get_spec(test_context)

    # Function runs
    result = get_tech_groups(spec, **args)

    # # For debugging
    # for k in sorted(result.keys()):
    #     print(f"{k}:")
    #     print("  " + "\n  ".join(sorted(result[k])))

    # Certain keys are present
    assert set() == present - set(result)

    # Certain keys are absent
    assert set() == absent & set(result)


@MARK[0]
def test_configure_legacy_reporting(buildings_context: "Context") -> None:
    config: dict[str, Any] = dict()

    configure_legacy_reporting(config)

    # Generated technology names are added to the appropriate sets
    assert {"meth_afofi"} < set(config["rc meth"])
    assert "h2_fc_afofi" in config["rc h2"]


def test_mpd() -> None:
    columns = ["node", "commodity", "year", "value"]

    # Function runs
    a = pd.DataFrame([["n1", "c1", "y1", 1.0]], columns=columns)
    b = pd.DataFrame([["n1", "c1", "y1", 1.1]], columns=columns)
    assert np.isclose(0.1 / (0.5 * 2.1), _mpd(a, b, "value"))

    # Returns NaN for various empty data frames
    c = pd.DataFrame()
    d = pd.DataFrame(columns=columns)
    assert np.isnan(_mpd(a, c, "value"))
    assert np.isnan(_mpd(a, d, "value"))
    assert np.isnan(_mpd(c, c, "value"))


def test_prepare_data_B_returns_structure(
    request: pytest.FixtureRequest,
    bmt_context: "Context",  # noqa: F811
) -> None:
    """prepare_data_B runs and returns a dict with expected keys (demand, etc.)."""
    scenario = bare_res(request, bmt_context)
    info = ScenarioInfo(scenario)
    prices, sturm_r, sturm_c, demand_static = _minimal_buildings_data()

    result = prepare_data_B(
        scenario,
        info,
        prices,
        sturm_r,
        sturm_c,
        demand_static=demand_static,
        with_materials=False,
        relations=[],
    )

    assert isinstance(result, dict)
    assert "demand" in result
    assert isinstance(result["demand"], pd.DataFrame)


def test_prepare_data_B_with_rc_tech_data(
    request: pytest.FixtureRequest,
    bmt_context: "Context",  # noqa: F811
) -> None:
    """prepare_data_B produces buildings tech data when scenario has rc techs."""
    scenario = bare_res(request, bmt_context)
    _add_minimal_rc_pars(scenario)
    info = ScenarioInfo(scenario)
    prices, sturm_r, sturm_c, demand_static = _minimal_buildings_data()

    result = prepare_data_B(
        scenario,
        info,
        prices,
        sturm_r,
        sturm_c,
        demand_static=demand_static,
        with_materials=True,
        relations=[],
    )

    assert "demand" in result
    # With elec_rc and resid_heat_electr in demand we expect some generated tech data
    assert not result["demand"].empty
    if "input" in result and not result["input"].empty:
        techs = result["input"].get("technology", pd.Series())
        assert any("electr_" in str(t) for t in techs)


def test_main_B_runs_with_minimal_data(
    request: pytest.FixtureRequest,
    bmt_context: "Context",  # noqa: F811
) -> None:
    """build_B runs without error with buildings config and minimal rc scenario."""
    scenario = bare_res(request, bmt_context)
    _add_minimal_rc_pars(scenario)
    _add_buildings_tech_set(scenario)

    main(bmt_context, scenario)

    # Scenario should still be usable and have been modified
    assert scenario is not None


def test_main_B_runs_with_materials(
    request: pytest.FixtureRequest,
    bmt_context_with_materials: "Context",  # noqa: F811
) -> None:
    """build_B runs with with_materials=True (materials linkage path)."""
    scenario = bare_res(request, bmt_context_with_materials)
    _add_minimal_rc_pars(scenario)
    _add_materials_commodities(scenario)
    _add_buildings_tech_set(scenario)

    main(bmt_context_with_materials, scenario)

    assert scenario is not None


def test_report3() -> None:
    # Mock contents of the Reporter
    s = cast("Scenario", namedtuple("Scenario", "scenario")("baseline"))
    config = {"sturm output path": package_data_path("test", "buildings", "sturm")}

    sturm_rep = report2(s, config)
    result = report3(s, sturm_rep)

    # TODO add assertions
    del result


@pytest.mark.skip(reason="Slow")
@pytest.mark.parametrize("sturm_method", [sturm.METHOD.RPY2, sturm.METHOD.RSCRIPT_A])
def test_sturm_run(
    tmp_path: "Path",
    test_context: "Context",
    test_data_path: "Path",
    sturm_method: sturm.METHOD,
) -> None:
    """Test that STURM can be run by either method."""
    test_context.model.regions = "R12"
    test_context.buildings = Config(
        sturm_method=sturm_method,
        sturm_scenario="NAV_Dem-NPi-ref",
        _output_path=tmp_path,
    )

    prices = pd.read_csv(
        test_data_path.joinpath("buildings", "prices.csv"), comment="#"
    )

    sturm.run(test_context, prices, True)


@pytest.mark.parametrize(
    "expected, input",
    [
        ("SSP2", "baseline"),
        ("NAV_Dem-NPi-act", "NAV_Dem-NPi-act"),
        ("NAV_Dem-NPi-act", "NAV_Dem-20C-act_u"),
        ("NAV_Dem-NPi-act", "NAV_Dem-20C-act_u + ENGAGE step 2"),
        # Without "NAV_Dem-"
        ("NAV_Dem-NPi-act", "20C-act_u + ENGAGE step 2"),
        # New naming as of 2022-11-22
        ("NAV_Dem-NPi-ref", "NPi-ref_EN1_1000_Gt"),
        ("NAV_Dem-NPi-all", "NAV_Dem-NPi-all"),
        ("NAV_Dem-NPi-ele", "NAV_Dem-NPi-ele"),
        ("NAV_Dem-NPi-ref", "NAV_Dem-NPi-ref"),
        ("NAV_Dem-NPi-ref", "NAV_Dem-NPi-ref"),
        ("NAV_Dem-NPi-ref", "NPi-ref_ENGAGE_20C_step-3+B"),
        ("NAV_Dem-NPi-ref", "Ctax-ref+B"),
        # WP6 scenarios
        # First pass
        ("NAV_Dem-NPi-ref", "NPi-Default"),
        ("NAV_Dem-NPi-ele", "NPi-AdvPE"),
        ("NAV_Dem-NPi-act-tec", "NPi-LowCE"),
        # Scenario name after climate policy steps
        # Current
        ("NAV_Dem-NPi-ref", "20C-ref ENGAGE_20C_step-3+B"),
        ("NAV_Dem-NPi-ref", "2C-Default ENGAGE_20C_step-3+B"),
        # Older
        ("NAV_Dem-NPi-ref", "NPi-Default_ENGAGE_20C_step-3+B"),
        ("NAV_Dem-NPi-ele", "NPi-AdvPE_ENGAGE_20C_step-3+B"),
        ("NAV_Dem-NPi-act-tec", "NPi-LowCE_ENGAGE_20C_step-3+B"),
    ],
)
def test_sturm_scenario_name(input: str, expected: str) -> None:
    assert expected == sturm.scenario_name(input)


@pytest.fixture
def sturm_context(test_context: "Context") -> "Context":
    """Context with R12 regions so bare RES nodes match MIXB demand CSVs."""
    test_context.model.regions = "R12"
    test_context.regions = "R12"
    return test_context


@pytest.fixture
def mixb_sturm_tree(tmp_path):
    """Minimal MESSAGEix-Buildings install tree for MIXB STURM tests."""
    install_root = tmp_path
    sturm_dir = install_root / "message_ix_buildings" / "sturm"
    data_dir = sturm_dir / "data"
    linking_dir = sturm_dir / "message_linking"
    data_dir.mkdir(parents=True)
    linking_dir.mkdir(parents=True)

    # MIXB reference prices use R11 node codes; call_sturm maps R12 scenario prices
    # to R11 before merging with this file.
    # TODO: discuss with Buildings colleagues to save the conversion
    pd.DataFrame(
        {
            "node": ["R11_AFR", "R11_AFR"],
            "commodity": ["gas", "electr"],
            "level": ["final", "final"],
            "year": [2020, 2020],
            "time": ["year", "year"],
            "lvl": [10.0, 20.0],
        }
    ).to_csv(data_dir / "input_prices_R12_default.csv", index=False)

    for name in (
        "run_STURM_Circular_resid_glo.R",
        "run_STURM_Circular_comm_glo.R",
        "run_GLANCE_placeholder.R",
        "run_MIXB_aligner.R",
    ):
        sturm_dir.joinpath(name).touch()

    demand = pd.DataFrame(
        {
            "node": ["R12_AFR"],
            "commodity": ["electr"],
            "year": [2100],
            "time": ["year"],
            "value": [1.5],
            "unit": ["GWa"],
        }
    )
    for pattern in sturm._MIXB_DEMAND_CSV:
        demand.to_csv(linking_dir / pattern.format(code="R"), index=False)

    return install_root, sturm_dir, linking_dir


def test_pass_scen_config_to_mixb(tmp_path):
    """_pass_scen_config_to_mixb writes scenario_config.yaml."""
    import yaml

    sturm._pass_scen_config_to_mixb(tmp_path, ["R", "SSP2"])

    path = tmp_path / "scenario_config.yaml"
    assert path.is_file()
    with open(path, encoding="utf-8") as f:
        assert {"scenarios": ["R", "SSP2"]} == yaml.safe_load(f)


def test_call_sturm(sturm_context, request, mixb_sturm_tree, monkeypatch):
    """call_sturm merges scenario prices and invokes STURM R scripts."""
    install_root, sturm_dir, _ = mixb_sturm_tree
    scenario = bare_res(request, sturm_context)
    sturm_context.buildings = Config(
        sturm_scenario="NONE", code="R", code_dir=install_root / "message_ix_buildings"
    )

    price_df = pd.DataFrame(
        {
            "node": ["R12_AFR", "R12_AFR", "R12_CHN"],
            "commodity": ["gas", "electr", "lightoil"],
            "level": ["final", "final", "final"],
            "year": [2020, 2020, 2020],
            "time": ["year", "year", "year"],
            "lvl": [5.0, 15.0, 8.0],
        }
    )
    original_var = scenario.var

    def fake_var(name, filters=None):
        if name == "PRICE_COMMODITY":
            return price_df
        return original_var(name, filters=filters)

    monkeypatch.setattr(scenario, "var", fake_var)
    monkeypatch.setattr(
        "message_ix_models.model.buildings.sturm._message_buildings_install_dir",
        lambda: install_root,
    )
    rscript_calls = []
    monkeypatch.setattr(
        "message_ix_models.model.buildings.sturm.subprocess.run",
        lambda *args, **kwargs: rscript_calls.append(args) or None,
    )

    result = sturm.call_sturm(sturm_context, scenario)

    assert result is scenario
    assert len(rscript_calls) == 4
    price_input = sturm_dir / "data" / "input_prices_R12.csv"
    assert price_input.is_file()
    updated = pd.read_csv(price_input)
    gas_row = updated.query("commodity == 'gas'").iloc[0]
    assert gas_row["lvl"] == 10.0  # floored at reference (scenario below reference)
    electr_row = updated.query("commodity == 'electr'").iloc[0]
    assert electr_row["lvl"] == 15.0  # electr not floored
    import yaml

    with open(sturm_dir / "scenario_config.yaml", encoding="utf-8") as f:
        assert {"scenarios": ["R"]} == yaml.safe_load(f)


def test_call_sturm_missing_reference_prices(
    sturm_context, request, mixb_sturm_tree, monkeypatch
):
    """call_sturm raises if the default price file is absent."""
    install_root, sturm_dir, _ = mixb_sturm_tree
    (sturm_dir / "data" / "input_prices_R12_default.csv").unlink()
    scenario = bare_res(request, sturm_context)
    sturm_context.buildings = Config(
        sturm_scenario="NONE", code="R", code_dir=install_root / "message_ix_buildings"
    )
    monkeypatch.setattr(
        "message_ix_models.model.buildings.sturm._message_buildings_install_dir",
        lambda: install_root,
    )

    with pytest.raises(FileNotFoundError, match="input_prices_R12_default"):
        sturm.call_sturm(sturm_context, scenario)


def test_call_buildings_demand(sturm_context, request, mixb_sturm_tree, monkeypatch):
    """call_buildings_demand reads MIXB CSVs and adds demand to the scenario."""
    install_root, _, _ = mixb_sturm_tree
    scenario = bare_res(request, sturm_context)
    sturm_context.buildings = Config(
        sturm_scenario="NONE", code="R", code_dir=install_root / "message_ix_buildings"
    )
    monkeypatch.setattr(
        "message_ix_models.model.buildings.sturm._message_buildings_install_dir",
        lambda: install_root,
    )

    result = sturm.call_buildings_demand(sturm_context, scenario)

    assert result is scenario
    demand = scenario.par("demand")
    assert len(demand) > 0
    assert {"electr"} <= set(demand["commodity"])
    assert (demand["level"] == "useful").all()
    assert 2110 in demand["year"].values


def test_call_buildings_demand_excludes_materials(
    sturm_context, request, mixb_sturm_tree, monkeypatch
):
    """call_buildings_demand drops commodities matching the exclude expression."""
    install_root, _, linking_dir = mixb_sturm_tree
    extra = pd.DataFrame(
        {
            "node": ["R12_AFR"],
            "commodity": ["electr_mat_floor"],
            "year": [2100],
            "time": ["year"],
            "value": [9.9],
            "unit": ["GWa"],
        }
    )
    path = linking_dir / sturm._MIXB_DEMAND_CSV[0].format(code="R")
    pd.concat([pd.read_csv(path), extra]).to_csv(path, index=False)

    scenario = bare_res(request, sturm_context)
    scenario.check_out()
    scenario.add_set("commodity", "electr_mat_floor")
    scenario.add_set("level", "useful")
    scenario.commit("Add commodity for exclude test")
    sturm_context.buildings = Config(
        sturm_scenario="NONE", code="R", code_dir=install_root / "message_ix_buildings"
    )
    monkeypatch.setattr(
        "message_ix_models.model.buildings.sturm._message_buildings_install_dir",
        lambda: install_root,
    )

    sturm.call_buildings_demand(sturm_context, scenario)

    commodities = set(scenario.par("demand")["commodity"])
    assert "electr" in commodities
    assert "electr_mat_floor" not in commodities
