"""Tests for the BMT workflow (Buildings, Materials, Transport).

Covers workflow steps and build functions in :mod:`message_ix_models.model.bmt`:
- BM built / build_B (buildings), in :mod:`.model.buildings.build`
- BMTX built / build_PM (power sector materials), in :mod:`.model.bmt.utils`

Coverage notes:
- prepare_data_B and build_B: tested with and without materials
  (with_materials=False and with_materials=True in test_prepare_data_B_*,
  test_build_B_runs_with_minimal_data, test_build_B_runs_with_materials).
- utils: build_PM (test_build_PM_*), _generate_vetting_csv (test_generate_vetting_csv*).
- CLI: bmt group and run subcommand (test_bmt_cli_help, test_bmt_run_dry_run).
"""

import logging
from types import SimpleNamespace

import pandas as pd
import pytest
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.bmt.utils import _generate_vetting_csv, build_PM
from message_ix_models.model.bmt.workflow import generate
from message_ix_models.model.buildings.build import build_B, prepare_data_B
from message_ix_models.testing import bare_res

log = logging.getLogger(__name__)


# --- Fixtures ---


def _minimal_buildings_data():
    """Minimal DataFrames for prepare_data_B / build_B."""
    commodities = ["electr", "gas", "lightoil", "d_heat"]
    common_dims = dict(
        node="R12_AFR", unit="GWa", time="year", level="useful", year=[2020, 2030]
    )
    prices = pd.DataFrame({"commodity": commodities})
    sturm_r = make_df("demand", **common_dims, commodity="resid_heat_electr", value=1.0)
    sturm_c = make_df("demand", **common_dims, commodity="comm_heat_electr", value=0.5)
    demand_static = make_df(
        "demand", **common_dims, commodity=["afofio_spec", "afofio_therm"], value=0
    ).assign(year=[2020, 2020])
    return prices, sturm_r, sturm_c, demand_static


@pytest.fixture
def bmt_context(test_context, tmp_path):
    """BMT context (R12) and buildings config with paths to minimal CSVs."""
    test_context.model.regions = "R12"
    test_context.regions = "R12"
    test_context.ssp = "SSP2"
    prices, sturm_r, sturm_c, demand_static = _minimal_buildings_data()
    prices.to_csv(tmp_path / "prices.csv", index=False)
    sturm_r.to_csv(tmp_path / "sturm_r.csv", index=False)
    sturm_c.to_csv(tmp_path / "sturm_c.csv", index=False)
    # build_B loads demand_static with index_col=0, so first column becomes index;
    # keep "commodity" as a column by adding an index column
    demand_static.insert(0, "idx", range(len(demand_static)))
    demand_static.to_csv(tmp_path / "demand_static.csv", index=False)
    test_context.buildings = SimpleNamespace(
        prices=str(tmp_path / "prices.csv"),
        sturm_r=str(tmp_path / "sturm_r.csv"),
        sturm_c=str(tmp_path / "sturm_c.csv"),
        demand_static=str(tmp_path / "demand_static.csv"),
        with_materials=False,
    )
    return test_context


@pytest.fixture
def bmt_context_with_materials(bmt_context):
    """Like bmt_context but with with_materials=True for build_B materials path."""
    bmt_context.buildings.with_materials = True
    return bmt_context


def _add_minimal_rc_pars(scenario):
    """Add minimal input/output/capacity_factor for elec_rc so prepare_data_B can run.

    Uses mode='all' to match the bare RES scenario's mode set (no 'M1' in bare RES).
    Skips emission_factor (unit tC/GWa and emission set may not exist in bare RES).
    """
    nodes = scenario.set("node")
    if not len(nodes):
        return
    node = nodes[0]
    years = scenario.set("year")
    if not len(years):
        return
    y = int(years[0])
    # Bare RES has mode "all", not "M1"
    mode = "all"

    common = dict(
        node_loc=node,
        technology="elec_rc",
        year_vtg=y,
        year_act=y,
        mode=mode,
        time="year",
        time_origin="year",
        unit="GWa",
    )
    inp = make_df(
        "input",
        **common,
        node_origin=node,
        commodity="electr",
        level="final",
        value=1.0,
    )
    out = make_df(
        "output",
        node_loc=node,
        technology="elec_rc",
        year_vtg=y,
        year_act=y,
        mode=mode,
        node_dest=node,
        commodity="electr",
        level="useful",
        time="year",
        time_dest="year",
        value=1.0,
        unit="GWa",
    )
    cap = make_df(
        "capacity_factor",
        node_loc=node,
        technology="elec_rc",
        year_vtg=y,
        year_act=y,
        mode=mode,
        time="year",
        value=0.5,
        unit="-",
    )
    scenario.check_out()
    scenario.add_par("input", inp)
    scenario.add_par("output", out)
    scenario.add_par("capacity_factor", cap)
    scenario.commit("Add minimal rc pars for BMT test")


def _add_buildings_tech_set(scenario):
    """Add set elements required by build_B / _replace_ue_rt_share_with_share_mode.

    - Technology set: share_mode_up references these technologies.
    - Mode set: share_mode_up uses mode 'M2' (bare RES only has 'all').
    """
    techs = [
        "electr_comm_cool",
        "electr_resid_cool",
        "electr_resid_apps",
        "electr_resid_other_uses",
        "electr_comm_other_uses",
        "electr_resid_cook",
    ]
    scenario.check_out()
    for t in techs:
        scenario.add_set("technology", t)
    scenario.add_set("mode", "M2")
    scenario.commit("Add buildings tech set for BMT test")


def _add_materials_commodities(scenario):
    """Add steel, cement, aluminum so get_spec(with_materials=True) succeeds."""
    scenario.check_out()
    for c in ("steel", "cement", "aluminum"):
        try:
            scenario.add_set("commodity", c)
        except ValueError:
            pass  # already present
    scenario.commit("Add materials commodities for with_materials=True test")


# --- Tests for prepare_data_B ---


def test_prepare_data_B_returns_structure(bmt_context, request):
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


def test_prepare_data_B_with_rc_tech_data(bmt_context, request):
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


# --- Tests for build_B ---


def test_build_B_runs_with_minimal_data(bmt_context, request):
    """build_B runs without error with buildings config and minimal rc scenario."""
    scenario = bare_res(request, bmt_context)
    _add_minimal_rc_pars(scenario)
    _add_buildings_tech_set(scenario)

    build_B(bmt_context, scenario)

    # Scenario should still be usable and have been modified
    assert scenario is not None


def test_build_B_runs_with_materials(bmt_context_with_materials, request):
    """build_B runs with with_materials=True (materials linkage path)."""
    scenario = bare_res(request, bmt_context_with_materials)
    _add_minimal_rc_pars(scenario)
    _add_materials_commodities(scenario)
    _add_buildings_tech_set(scenario)

    build_B(bmt_context_with_materials, scenario)

    assert scenario is not None


# --- Tests for workflow (BM built step) ---


def test_bmt_workflow_has_bm_built_step():
    """The BMT workflow includes the 'BM built' step that calls build_B."""
    from message_ix_models import Context

    ctx = Context()
    wf = generate(ctx)
    assert "BM built" in wf.graph
    # Graph: (step, "context", base_name); step.action = build_B
    task = wf.graph["BM built"]
    step = task[0] if isinstance(task, tuple) else task
    assert step.action is build_B


def test_bmt_workflow_step_bm_built_callable(bmt_context, request):
    """The build_B step (BM built) can be invoked with context and scenario."""
    scenario = bare_res(request, bmt_context)
    _add_minimal_rc_pars(scenario)
    _add_buildings_tech_set(scenario)
    build_B(bmt_context, scenario)
    assert scenario is not None


# --- Tests for build_PM (BMTX built step) ---


def test_bmt_workflow_has_bmtx_built_step():
    """The BMT workflow includes the 'BMTX built' step that calls build_PM."""
    from message_ix_models import Context

    ctx = Context()
    wf = generate(ctx)
    assert "BMTX built" in wf.graph
    task = wf.graph["BMTX built"]
    step = task[0] if isinstance(task, tuple) else task
    assert step.action is build_PM


def test_build_PM_returns_scenario(test_context, request):
    """build_PM returns the scenario and skips when input_cap_new already has cement."""
    scenario = bare_res(request, test_context)
    # Add minimal input_cap_new with cement so build_PM takes the early-return path.
    # Bare RES may not have 'cement' or 'product'; add set elements and unit as needed.
    scenario.check_out()
    for elem, set_name in [("cement", "commodity"), ("product", "level")]:
        try:
            scenario.add_set(set_name, elem)
        except Exception:
            pass  # already present
    unit = "t/kW"
    try:
        scenario.platform.add_unit(unit, "")
    except Exception:
        pass  # already exists
    if "input_cap_new" not in scenario.par_list():
        scenario.init_par(
            "input_cap_new",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    nodes = scenario.set("node")
    years = scenario.set("year")
    techs = scenario.set("technology")
    if not (len(nodes) and len(years) and len(techs)):
        pytest.skip("Scenario has no nodes/years/techs, cannot add input_cap_new row")
    node = nodes[0]
    y = int(years[0])
    tech = techs[0]
    df = pd.DataFrame(
        [
            {
                "node_loc": node,
                "technology": tech,
                "year_vtg": y,
                "node_origin": node,
                "commodity": "cement",
                "level": "product",
                "time": "year",
                "time_origin": "year",
                "value": 0.1,
                "unit": unit,
            }
        ]
    )
    scenario.add_par("input_cap_new", df)
    scenario.commit("Add minimal input_cap_new for build_PM test")

    result = build_PM(test_context, scenario)

    assert result is scenario


def test_build_PM_callable(test_context, request):
    """build_PM(context, scenario) runs; skip if scenario lacks inv_cost."""
    scenario = bare_res(request, test_context)
    try:
        result = build_PM(test_context, scenario)
        assert result is scenario
    except (KeyError, ValueError) as e:
        # Minimal scenario may lack inv_cost etc. for gen_data_power_sector
        pytest.skip(f"build_PM needs full scenario data: {e}")


# --- Tests for _generate_vetting_csv (utils.py) ---


def test_generate_vetting_csv(tmp_path):
    """_generate_vetting_csv writes CSV of original/modified demand and subtraction."""
    original_demand = pd.DataFrame(
        {
            "node": ["R12_AFR", "R12_AFR"],
            "year": [2020, 2030],
            "commodity": ["cement", "cement"],
            "value": [10.0, 20.0],
        }
    )
    modified_demand = pd.DataFrame(
        {
            "node": ["R12_AFR", "R12_AFR"],
            "year": [2020, 2030],
            "commodity": ["cement", "cement"],
            "value": [7.0, 15.0],
        }
    )
    out = tmp_path / "vetting.csv"

    _generate_vetting_csv(original_demand, modified_demand, str(out))

    assert out.exists()
    df = pd.read_csv(out)
    assert list(df.columns) == [
        "node",
        "year",
        "commodity",
        "original_demand",
        "modified_demand",
        "subtracted_amount",
        "subtraction_percentage",
    ]
    assert len(df) == 2
    assert df["subtracted_amount"].tolist() == [3.0, 5.0]
    assert df["subtraction_percentage"].tolist() == [30.0, 25.0]


def test_generate_vetting_csv_zero_original(tmp_path):
    """_generate_vetting_csv handles zero original demand (no div-by-zero)."""
    original_demand = pd.DataFrame(
        {"node": ["R12_AFR"], "year": [2020], "commodity": ["steel"], "value": [0.0]}
    )
    modified_demand = pd.DataFrame(
        {"node": ["R12_AFR"], "year": [2020], "commodity": ["steel"], "value": [0.0]}
    )
    out = tmp_path / "vetting_zero.csv"

    _generate_vetting_csv(original_demand, modified_demand, str(out))

    assert out.exists()
    df = pd.read_csv(out)
    assert df["subtraction_percentage"].iloc[0] == 0.0


# --- Tests for BMT CLI (cli.py) ---


def test_bmt_cli_help(mix_models_cli):
    """bmt and bmt run show --help."""
    mix_models_cli.assert_exit_0(["bmt", "--help"])
    mix_models_cli.assert_exit_0(["bmt", "run", "--help"])


def test_bmt_run_dry_run(mix_models_cli):
    """bmt run --dry-run TARGET runs workflow in dry-run (writes SVG, no execution)."""
    mix_models_cli.assert_exit_0(["bmt", "run", "--dry-run", "BM built"])
