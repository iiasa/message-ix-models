import numpy as np
import pandas as pd
import pytest
from genno import Key, Quantity
from iam_units import registry
from message_ix import make_df
from message_ix_models.util import broadcast, same_node

from message_data.model.transport import Config, DataSourceConfig, files, testing
from message_data.model.transport.CHN_IND import get_chn_ind_data, get_chn_ind_pop
from message_data.model.transport.emission import ef_for_input, get_emissions_data
from message_data.model.transport.roadmap import get_roadmap_data
from message_data.model.transport.testing import MARK
from message_data.projects.navigate import T35_POLICY
from message_data.testing import assert_units
from message_data.tools.gfei_fuel_economy import get_gfei_data


@pytest.mark.parametrize("file", files.FILES, ids=lambda f: "-".join(f.parts))
def test_data_files(test_context, file):
    """Input data can be read."""
    c, _ = testing.configure_build(test_context, regions="R12", years="B")

    # Task runs
    result = c.get(file.key)

    # Quantity is loaded
    assert isinstance(result, Quantity)

    # Dimensions are as expected
    assert set(Key(result).dims) == set(file.key.dims)


def test_ef_for_input(test_context):
    # Generate a test "input" data frame
    _, info = testing.configure_build(test_context, regions="R11", years="B")
    years = info.yv_ya
    data = (
        make_df(
            "input",
            year_vtg=years.year_vtg,
            year_act=years.year_act,
            technology="t",
            mode="m",
            commodity=None,
            level="final",
            time="year",
            time_origin="year",
            value=0.05,
            unit="GWa / (Gv km)",
        )
        .pipe(broadcast, node_loc=info.N)
        .pipe(same_node)
    )

    # Generate random commodity values
    c = ("electr", "ethanol", "gas", "hydrogen", "lightoil", "methanol")
    splitter = np.random.choice(np.arange(len(c)), len(data))
    data = data.assign(
        commodity=pd.Categorical.from_codes(splitter, categories=c),
    )
    assert not data.isna().any().any(), data

    # Function runs successfully on these data
    result = ef_for_input(test_context, data)

    # Returns data for two parameters if transport.Config.emission_relations is True
    # (the default)
    assert {"emission_factor", "relation_activity"} == set(result)
    ef = result["emission_factor"]

    # Data is complete
    assert not ef.isna().any().any(), ef

    ra = result["relation_activity"]
    assert not ra.isna().any(axis=None), ra

    assert int == ra.dtypes["year_act"]

    # print(ra.to_string())

    # TODO test specific values


@pytest.mark.parametrize("source, rows", (("1", 4717), ("2", 5153)))
@pytest.mark.parametrize("regions", ["R11"])
def test_get_emissions_data(test_context, source, rows, regions):
    # Set the value; don't need to read_config()
    test_context.model.regions = regions
    test_context.transport = Config(data_source=DataSourceConfig(emissions=source))

    data = get_emissions_data(test_context)
    assert {"emission_factor"} == set(data.keys())
    assert rows == len(data["emission_factor"])


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


def test_get_freight_data(test_context, regions="R12", years="B"):
    ctx = test_context
    c, info = testing.configure_build(ctx, regions=regions, years=years)

    # Code runs
    result = c.get("transport freight::ixmp")

    # Data are provided for these parameters
    assert {
        "capacity_factor",
        "input",
        "output",
        "technical_lifetime",
    } == set(result.keys())


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
        "capacity_factor",
        "emission_factor",
        "fix_cost",
        "input",
        "inv_cost",
        "output",
        "relation_activity",
        "technical_lifetime",
        "var_cost",
    }
    if regions == "R12":
        exp_pars |= {"bound_activity_lo"}  # From .non_ldv.other()
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


def test_get_gfei_data(test_context):
    test_context.model.regions = "R11"

    df = get_gfei_data(test_context)

    # Data have the expected size
    assert 307 == len(df)

    # Data covers all historical periods from the Roadmap model
    assert {2017} == set(df["year"].unique())
    # Modes match the list below
    assert {
        "ICAe_ffv",
        "ICAm_ptrp",
        "ICH_chyb",
        "ICE_conv",
        "ELC_100",
        "ICE_lpg",
        "PHEV_ptrp",
        "ICE_nga",
        "HFC_ptrp",
    } == set(df["technology"].unique())

    # Data have the expected dimensions
    assert {
        "technology",
        "value",
        "ISO_code",
        "region",
        "year",
        "units",
        "variable",
    } == set(df.columns)


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
