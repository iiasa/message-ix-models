import numpy as np
import pytest
from iam_units import registry

from message_ix_models.model.transport import build, testing
from message_ix_models.model.transport.CHN_IND import get_chn_ind_data, get_chn_ind_pop
from message_ix_models.model.transport.roadmap import get_roadmap_data
from message_ix_models.model.transport.testing import MARK, assert_units
from message_ix_models.project.navigate import T35_POLICY


@MARK[5]("RoadmapResults_2017.xlsx")
@pytest.mark.parametrize(
    "region, length",
    [
        (("Africa", "R11_AFR"), 224),
    ],
)
def test_get_afr_data(test_context, region, length):
    ctx = test_context

    df = get_roadmap_data(ctx, region)

    # Data covers all historical periods from the Roadmap model
    assert sorted(df["year"].unique()) == [2000, 2005, 2010, 2015]
    # Modes match the list below
    assert list(df["mode/vehicle type"].unique()) == [
        "2W_3W",
        "Bus",
        "Cars/light trucks",
        "Domestic passenger airplanes",
        "Freight trains",
        "Freight trucks",
        "Passenger trains",
    ]

    # Data have the correct size and format
    assert len(df["mode/vehicle type"]) == length
    assert list(df.columns) == [
        "mode/vehicle type",
        "year",
        "value",
        "variable",
        "units",
        "region",
    ]


@build.get_computer.minimum_version
def test_get_freight_data(test_context, regions="R12", years="B"):
    ctx = test_context
    c, info = testing.configure_build(ctx, regions=regions, years=years)

    # Code runs
    result = c.get("transport F::ixmp")

    # Data are provided for these parameters
    assert {
        "capacity_factor",
        "input",
        "output",
        "technical_lifetime",
    } == set(result.keys())


@build.get_computer.minimum_version
@pytest.mark.parametrize("regions", ["R11", "R12"])
def test_get_non_ldv_data(test_context, regions, years="B"):
    """:mod:`.non_ldv` returns the expected data."""
    # TODO realign with test_ikarus.py
    # TODO add options
    ctx = test_context
    c, _ = testing.configure_build(ctx, regions=regions, years=years)

    # Code runs
    data = c.get("transport nonldv::ixmp")

    # Data are provided for the these parameters
    exp_pars = {
        "bound_activity_lo",  # From .non_ldv.other(). For R11 this is empty.
        "bound_activity_up",  # From act-non_ldv.csv via .non_ldv.bound_activity()
        "capacity_factor",
        "emission_factor",
        "fix_cost",
        "growth_activity_lo",
        "growth_activity_up",
        "growth_new_capacity_up",
        "initial_activity_up",
        "initial_new_capacity_up",
        "input",
        "inv_cost",
        "output",
        "relation_activity",
        "technical_lifetime",
        "var_cost",
    }
    assert exp_pars == set(data.keys())

    # Input data have expected units
    mask0 = data["input"]["technology"].str.endswith(" usage")
    mask1 = data["input"]["technology"].str.startswith("transport other")

    assert_units(data["input"][mask0], registry("Gv km"))
    if mask1.any():
        assert_units(data["input"][mask1], registry("GWa"))
    assert_units(data["input"][~(mask0 | mask1)], registry("1.0 GWa / (Gv km)"))

    # Output data exist for all non-LDV modes
    modes = list(filter(lambda m: m != "LDV", ctx.transport.demand_modes))
    obs = set(data["output"]["commodity"].unique())
    assert len(modes) * 2 == len(obs)

    # Output data have expected units
    mask = data["output"]["technology"].str.endswith(" usage")
    assert_units(data["output"][~mask], {"[vehicle]": 1, "[length]": 1})
    assert_units(data["output"][mask], {"[passenger]": 1, "[length]": 1})


@pytest.mark.skip("Pending https://github.com/transportenergy/database/issues/75")
def test_get_chn_ind_data():
    df = get_chn_ind_data()

    # Data covers all historical periods from NBSC
    assert list(df["Year"].unique()) == list(range(2000, 2019, 1))
    # Modes match the list below
    assert list(df["Mode/vehicle type"].unique()) == [
        "Civil Aviation",
        "Highways",
        "Ocean",
        "Railways",
        "Total freight transport",
        "Waterways",
        "Total passenger transport",
        np.nan,
        "Civil Vehicles",
        "Heavy Trucks",
        "Large Passenger Vehicles",
        "Light Trucks",
        "Medium Passenger Vehicles",
        "Medium Trucks",
        "Mini Passenger Vehicles",
        "Mini Trucks",
        "Other Vehicles",
        "Passenger Vehicles",
        "Small Passenger Vehicles",
        "Trucks",
        "Rail",
        "Road",
        "_T",
        "Shipping",
        "Inland",
        "Inland ex. pipeline",
        "Pipeline",
    ]

    # Data have the correct size and format
    assert len(df["Mode/vehicle type"]) == 683
    assert list(df.columns) == [
        "ISO_code",
        "Variable",
        "Mode/vehicle type",
        "Units",
        "Year",
        "Value",
    ]
    # Check unit conversions
    assert df.loc[0, "Units"] == "gigatkm"
    assert df.loc[0, "Value"] == 5.027


def test_get_chn_ind_pop():
    df = get_chn_ind_pop()

    # Data covers all historical periods from NBSC
    assert list(df["Year"].unique()) == list(range(2000, 2019, 1))
    # Data have the correct size and format
    assert (
        df[(df["ISO_code"] == "CHN") & (df["Year"] == 2001)]["Value"].values
        == 1290937649
    )
    assert list(df.columns) == [
        "ISO_code",
        "Year",
        "Value",
        "Variable",
    ]


@build.get_computer.minimum_version
@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions", [pytest.param("ISR", marks=MARK[3]), "R11", "R12", "R14"]
)
@pytest.mark.parametrize("options", [{}, dict(navigate_scenario=T35_POLICY.ELE)])
def test_navigate_ele(test_context, regions, years, options):
    """Test genno-based IKARUS data prep."""
    ctx = test_context
    c, info = testing.configure_build(
        ctx, regions=regions, years=years, options=options
    )

    k = "navigate_ele::ixmp"

    # Computation runs without error
    result = c.get(k)

    if 0 == len(options):
        assert 0 == len(result)
        return

    # Result contains data for 1 parameter
    assert {"bound_new_capacity_up"} == set(result)
    bncu = result["bound_new_capacity_up"]

    # Constraint values are only generated for 2040 onwards
    assert 2040 == np.min(bncu.year_vtg)

    # Certain fossil fueled technologies are constrained
    techs = set(bncu["technology"].unique())
    print(f"{techs = }")
    assert {"ICAe_ffv", "ICE_nga", "IGH_ghyb", "FR_ICE_M", "FR_ICE_L"} <= techs

    # Electric technologies are not constrained
    assert {"ELC_100", "FR_FCH"}.isdisjoint(techs)
