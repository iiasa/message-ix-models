import numpy as np
import pandas as pd
import pytest
from genno import Quantity
from iam_units import registry
from message_ix import make_df
from message_ix_models.util import broadcast, same_node

from message_data.model.transport import Config, DataSourceConfig, computations
from message_data.model.transport import data as data_module
from message_data.model.transport.CHN_IND import get_chn_ind_data, get_chn_ind_pop
from message_data.model.transport.emission import ef_for_input, get_emissions_data
from message_data.model.transport.freight import get_freight_data
from message_data.model.transport.non_ldv import get_non_ldv_data
from message_data.model.transport.roadmap import get_roadmap_data
from message_data.model.transport.util import path_fallback
from message_data.testing import assert_units
from message_data.tests.model.transport import configure_build
from message_data.tools.gfei_fuel_economy import get_gfei_data


def test_advance_fv():
    result = computations.advance_fv(dict(regions="R12"))

    assert ("n",) == result.dims
    # Results only for R12
    assert 12 == len(result.coords["n"])
    assert {"[mass]": 1, "[length]": 1} == result.units.dimensionality, result


@pytest.mark.parametrize("parts", data_module.DATA_FILES)
def test_data_files(test_context, parts):
    """Input data can be read."""
    from genno.computations import load_file

    test_context.model.regions = "R11"

    result = load_file(path_fallback(test_context, *parts))
    assert isinstance(result, Quantity)


def test_ef_for_input(test_context):
    # Generate a test "input" data frame
    info = configure_build(test_context, regions="R11", years="B")
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
    ctx.update(regions=regions)

    configure_build(ctx, regions, years)

    info = ctx["transport build info"]

    # Code runs
    data = get_freight_data(info.N[1:], info.Y, ctx)

    # Data are provided for these parameters
    assert {
        "capacity_factor",
        "input",
        "output",
        "technical_lifetime",
    } == set(data.keys())


@pytest.mark.parametrize("regions", ["R11", "R12"])
def test_get_non_ldv_data(test_context, regions, years="B"):
    """:func:`.get_non_ldv_data` returns the expected data."""
    ctx = test_context
    configure_build(ctx, regions, years)

    # Code runs
    data = get_non_ldv_data(ctx)

    # Data are provided for the these parameters
    assert {
        "capacity_factor",
        "emission_factor",
        "fix_cost",
        "input",
        "inv_cost",
        "output",
        "relation_activity",
        "technical_lifetime",
        "var_cost",
    } == set(data.keys())

    # Input data have expected units
    assert_units(data["input"], registry("1.0 GWa / (Gv km)"))

    # Output data exist for all non-LDV modes
    modes = list(filter(lambda m: m != "LDV", ctx.transport.demand_modes))
    assert len(modes) == len(data["output"]["commodity"].unique())

    # Output data have expected units
    assert_units(data["output"], {"[passenger]": 1, "[vehicle]": -1})


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
