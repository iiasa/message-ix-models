import pandas as pd
import pytest

from message_ix_models.model.water.config import Config
from message_ix_models.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)
from message_ix_models.tests.model.water.conftest import water_params

DESAL_TECS = ("membrane", "distillation")

_DESAL_PARAMS = [
    water_params("R12", RCP="2p6", ssp="SSP2"),
    water_params("R11", RCP="6p0", ssp="SSP2"),
    water_params("R12", reduced_basin=True, RCP="2p6", ssp="SSP2"),
]
_DESAL_PROJECTION_PARAMS = [
    water_params("R12", RCP="2p6", ssp="SSP2"),
    water_params("R12", reduced_basin=True, RCP="2p6", ssp="SSP2"),
]


def _expected_basin_years(water_context) -> set[tuple[str, int]]:
    cfg = Config.from_context(water_context)
    info = water_context["water build info"]
    firstyear = water_context.get_scenario().firstmodelyear
    basins = {f"B{b}" for b in cfg.valid_basins}
    years = {y for y in info.Y if y >= firstyear}
    return {(nl, y) for nl in basins for y in years}


def _assert_complete_coverage(rows, year_col: str, expected, label: str) -> None:
    keys = ["node_loc", year_col]
    index = pd.MultiIndex.from_frame(rows[keys]).sort_values()
    duplicate = index[index.duplicated()].unique()
    assert duplicate.empty, (
        f"{label} has duplicate (basin, year) rows: {list(duplicate[:5])}"
    )
    pd.testing.assert_index_equal(
        index,
        pd.MultiIndex.from_tuples(sorted(expected), names=keys),
        obj=label,
    )


@pytest.mark.parametrize(
    "water_context",
    [
        water_params("R11", SDG="baseline"),
        water_params("R11", SDG="not_baseline"),
        water_params("R12", SDG="baseline"),
        water_params("R12", SDG="not_baseline"),
        water_params("ZMB", SDG="baseline"),
        water_params("ZMB", SDG="not_baseline"),
        water_params("R12", reduced_basin=True, SDG="baseline"),
    ],
    indirect=True,
)
def test_add_infrastructure_techs(
    water_context, water_scenario, assert_message_params, assert_input_output_structure
):
    """Test add_infrastructure_techs with global and country model configurations.

    Also tests start_creating_input_dataframe() and prepare_input_dataframe()
    since they are called by add_infrastructure_techs().
    """
    result = add_infrastructure_techs(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input", "output"])
    assert_input_output_structure(result)


@pytest.mark.parametrize(
    "water_context",
    [
        water_params("R11", RCP="6p0", ssp="SSP2"),
        water_params("R12", RCP="7p0", ssp="SSP2"),
        water_params("ZMB", RCP="7p0", ssp="SSP2"),
        water_params("R12", reduced_basin=True, RCP="7p0", ssp="SSP2"),
    ],
    indirect=True,
)
def test_add_desalination(
    water_context, water_scenario, assert_message_params, assert_input_output_structure
):
    """Test add_desalination with global and country model configurations."""
    result = add_desalination(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input", "output"])
    assert_input_output_structure(result)


@pytest.mark.parametrize("water_context", _DESAL_PARAMS, indirect=True)
def test_shared_extraction_cap_check(
    water_context, water_scenario, assert_message_params
):
    firstyear = water_context.get_scenario().firstmodelyear
    result = add_desalination(context=water_context)
    assert_message_params(result)

    blo = result["bound_activity_lo"]
    bup = result["bound_total_capacity_up"]

    assert (blo["year_act"] >= firstyear).all(), (
        f"bound_activity_lo includes pre-firstmodelyear rows: "
        f"{sorted(blo.loc[blo['year_act'] < firstyear, 'year_act'].unique())}"
    )

    desal_lo = blo[blo["technology"].isin(DESAL_TECS)]
    extract_up = bup[bup["technology"] == "extract_salinewater_basin"]

    lo_sum = (
        desal_lo.groupby(["node_loc", "year_act"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "lo_sum"})
    )
    cap = extract_up[["node_loc", "year_act", "value"]].rename(columns={"value": "cap"})
    joined = lo_sum.merge(cap, on=["node_loc", "year_act"], how="left")
    missing_cap = joined[joined["cap"].isna()]
    assert missing_cap.empty, (
        "bound_activity_lo[desal] has no extract_salinewater_basin cap at:\n"
        f"{missing_cap.to_string(index=False)}"
    )
    headroom = joined["cap"] - joined["lo_sum"]
    bad = joined[headroom < -1e-9]
    assert bad.empty, (
        "sum(bound_activity_lo[desal]) exceeds bound_total_capacity_up"
        f"[extract_salinewater_basin] at:\n{bad.to_string(index=False)}"
    )


@pytest.mark.parametrize("water_context", _DESAL_PROJECTION_PARAMS, indirect=True)
def test_desal_projection_coverage(
    water_context, water_scenario, assert_message_params
):
    result = add_desalination(context=water_context)
    assert_message_params(result)

    extract_up = result["bound_total_capacity_up"]
    extract_up = extract_up[extract_up["technology"] == "extract_salinewater_basin"]

    _assert_complete_coverage(
        extract_up,
        "year_act",
        _expected_basin_years(water_context),
        "extract_salinewater_basin bound_total_capacity_up",
    )
    assert (extract_up["value"] >= 0).all(), (
        "negative bound_total_capacity_up rows for extract_salinewater_basin"
    )
