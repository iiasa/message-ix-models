from typing import Mapping, Union

import numpy as np
import pandas as pd
import pint
import pytest
from genno import Quantity
from iam_units import registry
from message_ix import make_df
from message_ix_models import testing
from message_ix_models.model import bare
from message_ix_models.util import broadcast, same_node
from pandas.testing import assert_series_equal
from pytest import param

from message_data.model.transport import (
    Config,
    DataSourceConfig,
    build,
    computations,
    configure,
)
from message_data.model.transport import data as data_module
from message_data.model.transport.data import ldv
from message_data.model.transport.data.CHN_IND import get_chn_ind_data, get_chn_ind_pop
from message_data.model.transport.data.emissions import ef_for_input, get_emissions_data
from message_data.model.transport.data.freight import get_freight_data
from message_data.model.transport.data.ikarus import get_ikarus_data
from message_data.model.transport.data.non_ldv import get_non_ldv_data
from message_data.model.transport.data.roadmap import get_roadmap_data
from message_data.model.transport.utils import path_fallback
from message_data.tools.gfei_fuel_economy import get_gfei_data


def assert_units(
    df: pd.DataFrame, expected: Union[str, dict, pint.Unit, pint.Quantity]
):
    """Assert that `df` has the unique, `expected` units."""
    all_units = df["unit"].unique()
    assert 1 == len(all_units), f"Non-unique {all_units = }"

    # Convert the unique value to the same class as `expected`
    if isinstance(expected, pint.Quantity):
        assert expected == expected.__class__(1.0, all_units[0])
    elif isinstance(expected, Mapping):
        # Compare dimensionality of the units, rather than exact match
        assert expected == registry.Quantity(all_units[0] or "0").dimensionality
    else:
        assert expected == expected.__class__(all_units[0])


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

    # Returns data for one parameter
    assert {"emission_factor"} == set(result)
    ef = result["emission_factor"]

    # Data is complete
    assert not ef.isna().any().any(), ef

    # TODO test specific values


def configure_build(context, regions, years):
    context.update(regions=regions, years=years)

    configure(context)

    # Information about the corresponding base model
    info = bare.get_spec(context)["add"]
    context["transport build info"] = info
    context["transport spec"] = build.get_spec(context)

    return info


@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions, N_node", [("R11", 11), ("R12", 12), ("R14", 14), ("ISR", 1)]
)
def test_get_ikarus_data(test_context, regions, N_node, years):
    ctx = test_context
    info = configure_build(ctx, regions, years)

    # get_ikarus_data() succeeds on the bare RES
    data = get_ikarus_data(ctx)

    # Returns a mapping
    assert {
        "capacity_factor",
        "fix_cost",
        "input",
        "inv_cost",
        "output",
        "technical_lifetime",
    } == set(data.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data.values()))

    # Retrieve DataFrame for par e.g. 'inv_cost' and tech e.g. 'rail_pub'
    inv = data["inv_cost"]
    inv_rail_pub = inv[inv["technology"] == "rail_pub"]

    # NB: *prep_years* is created to accommodate prepended years before than
    # *firstmodelyear*. See ikarus.py to check how/why those are prepended.
    prep_years = (1 if years == "A" else 2) + len(info.Y)
    # Regions * 13 years (inv_cost has 'year_vtg' but not 'year_act' dim)
    rows_per_tech = N_node * prep_years
    N_techs = 18

    # Data have been loaded with the correct shape and magnitude:
    assert inv_rail_pub.shape == (rows_per_tech, 5), inv_rail_pub
    assert inv.shape == (rows_per_tech * N_techs, 5)

    # Magnitude for year e.g. 2020
    values = inv_rail_pub[inv_rail_pub["year_vtg"] == 2020]["value"]
    value = values.iloc[0]
    assert round(value, 3) == 3.233

    # Units of each parameter have the correct dimensionality
    dims = {
        "capacity_factor": {},  # always dimensionless
        "inv_cost": {"[currency]": 1, "[vehicle]": -1},
        "fix_cost": {"[currency]": 1, "[vehicle]": -1, "[time]": -1},
        "output": {"[passenger]": 1, "[vehicle]": -1},
        "technical_lifetime": {"[time]": 1},
    }
    for par, dim in dims.items():
        assert_units(data[par], dim)

    # Specific magnitudes of other values to check
    checks = [
        # commented (PNK 2022-06-17): corrected abuse of caapacity_factor to include
        # unrelated concepts
        # dict(par="capacity_factor", year_vtg=2010, value=0.000905),
        # dict(par="capacity_factor", year_vtg=2050, value=0.000886),
        dict(par="technical_lifetime", year_vtg=2010, value=15.0),
        dict(par="technical_lifetime", year_vtg=2050, value=15.0),
    ]
    defaults = dict(node_loc=info.N[-1], technology="ICG_bus", time="year")

    for check in checks:
        # Create expected data
        par_name = check.pop("par")
        check["year_act"] = check["year_vtg"]
        exp = make_df(par_name, **defaults, **check)
        assert len(exp) == 1, "Single row for expected value"

        # Use merge() to find data with matching column values
        columns = sorted(set(exp.columns) - {"value", "unit"})
        result = exp.merge(data[par_name], on=columns, how="inner")

        # Single row matches
        assert len(result) == 1, result

        # Values match
        assert_series_equal(
            result["value_x"],
            result["value_y"],
            check_exact=False,
            check_names=False,
            atol=1e-4,
        )


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
    "source, regions, years",
    [
        param(
            None,
            "R11",
            "A",
            marks=pytest.mark.xfail(
                raises=AssertionError, reason="Returns extra var_cost data"
            ),
        ),
        ("US-TIMES MA3T", "R11", "A"),
        ("US-TIMES MA3T", "R11", "B"),
        ("US-TIMES MA3T", "R12", "B"),
        ("US-TIMES MA3T", "R14", "A"),
        # Not implemented
        param("US-TIMES MA3T", "ISR", "A", marks=testing.NIE),
    ],
)
def test_get_ldv_data(test_context, source, regions, years):
    # Info about the corresponding RES
    ctx = test_context

    info = configure_build(ctx, regions, years)
    ctx.transport.data_source.LDV = source

    # Method runs without error

    data = ldv.get_ldv_data(ctx)

    # Data are returned for the following parameters
    assert {
        "capacity_factor",
        "emission_factor",
        "fix_cost",
        "input",
        "inv_cost",
        "output",
        "technical_lifetime",
    } == set(data.keys())

    # Input data is returned and has the correct units
    assert_units(data["input"], registry("1.0 GWa / (Gv km)"))

    # Output data is returned and has the correct units
    assert_units(data["output"], registry.Unit("Gv km"))

    # Historical periods from 2010 + all model periods
    i = info.set["year"].index(2010)
    exp = info.set["year"][i:]

    # Remaining data have the correct size
    for par_name, df in data.items():
        # Data covers these periods
        assert exp == sorted(df["year_vtg"].unique())

        # Total length of data: # of regions × (11 technology × # of periods; plus 1
        # technology (historical ICE) for only 2010)
        assert len(info.N[1:]) * ((11 * len(exp)) + 1) == len(df)


@pytest.mark.parametrize(
    "source, regions, years",
    [
        (None, "R11", "A"),
        ("US-TIMES MA3T", "R11", "A"),
        ("US-TIMES MA3T", "R11", "B"),
        ("US-TIMES MA3T", "R12", "B"),
        ("US-TIMES MA3T", "R14", "A"),
        # Not implemented
        param("US-TIMES MA3T", "ISR", "A", marks=testing.NIE),
    ],
)
def test_ldv_constraint_data(test_context, source, regions, years):
    # Info about the corresponding RES
    ctx = test_context

    info = configure_build(ctx, regions, years)
    ctx.transport.data_source.LDV = source

    # Method runs without error

    data = ldv.constraint_data(ctx)

    # Data are returned for the following parameters
    assert {"growth_activity_lo", "growth_activity_up"} == set(data.keys())

    for bound in ("lo", "up"):
        # Constraint data are returned. Use .pop() to exclude from the next assertions
        df = data.pop(f"growth_activity_{bound}")

        # Usage technologies are included
        assert "ELC_100 usage by URLMM" in df["technology"].unique()

        # Data covers all periods except the first
        assert info.Y[1:] == sorted(df["year_act"].unique())


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
