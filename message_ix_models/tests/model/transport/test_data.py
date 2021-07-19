import numpy as np
import pytest
from genno import Quantity, computations
from iam_units import registry
from message_ix import make_df
from message_ix_models.model import bare
from pandas.testing import assert_series_equal
from pytest import param

from message_data import testing
from message_data.model.transport import data as data_module, read_config
from message_data.model.transport.data.CHN_IND import get_chn_ind_data, get_chn_ind_pop
from message_data.model.transport.data.emissions import get_emissions_data
from message_data.model.transport.data.groups import (
    get_consumer_groups,
    get_urban_rural_shares,
)
from message_data.model.transport.data.ikarus import get_ikarus_data
from message_data.model.transport.data.ldv import get_ldv_data
from message_data.model.transport.data.roadmap import get_roadmap_data
from message_data.model.transport.utils import path_fallback
from message_data.tools.gfei_fuel_economy import get_gfei_data


@pytest.mark.parametrize("parts", data_module.DATA_FILES)
def test_data_files(test_context, parts):
    """Input data can be read."""
    test_context.regions = "R11"

    result = computations.load_file(path_fallback(test_context, *parts))
    assert isinstance(result, Quantity)


@pytest.mark.parametrize("regions", ["R11", "R14", param("ISR", marks=testing.NIE)])
def test_demand(test_context, regions):
    """Test :func:`.transport.data.demand` that returns demand data."""
    ctx = test_context
    ctx["regions"] = regions
    ctx["transport build info"] = info = bare.get_spec(ctx)["add"]

    read_config(ctx)

    # Function runs
    data = data_module.demand(ctx)

    # Returns a dict with a single key/DataFrame
    demand = data.pop("demand")
    assert 0 == len(data)

    # Demand is expressed for the expected quantities
    assert {"transport pax RUEMF", "transport pax air"} < set(demand["commodity"])

    # Demand covers the model horizon
    assert info.Y[-1] == max(
        demand["year"].unique()
    ), "`demand` does not cover the model horizon"


@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize("regions, N_node", [("R11", 11), ("R14", 14), ("ISR", 1)])
def test_ikarus(test_context, regions, N_node, years):
    ctx = test_context
    ctx.regions = regions
    ctx.years = years

    # Information about the corresponding base model
    s_info = bare.get_spec(ctx)["add"]
    ctx["transport build info"] = s_info

    # get_ikarus_data() succeeds on the bare RES
    data = get_ikarus_data(ctx)

    # Returns a mapping
    # Retrieve DataFrame for par e.g. 'inv_cost' and tech e.g. 'rail_pub'
    inv = data["inv_cost"]
    inv_rail_pub = inv[inv["technology"] == "rail_pub"]

    # Regions * 13 years (inv_cost has 'year_vtg' but not 'year_act' dim)
    rows_per_tech = N_node * (len(s_info.Y) + 1)
    N_techs = 18

    # Data have been loaded with the correct shape, unit and magnitude:
    # 1. Shape
    assert inv_rail_pub.shape == (rows_per_tech, 5), inv_rail_pub
    assert inv.shape == (rows_per_tech * N_techs, 5)

    # 2. Units
    units = inv_rail_pub["unit"].unique()
    assert len(units) == 1, "Units for each (par, tec) must be unique"

    # Unit is parseable by pint
    pint_unit = registry(units[0])

    # Unit has the correct dimensionality
    assert pint_unit.dimensionality == {"[currency]": 1, "[vehicle]": -1}

    # 3. Magnitude for year e.g. 2020
    values = inv_rail_pub[inv_rail_pub["year_vtg"] == 2020]["value"]
    value = values.iloc[0]
    assert round(value, 3) == 3.233

    dims = {
        "technical_lifetime": {"[time]": 1},
        # Output units are in (passenger km) / energy, that's why mass and
        # time dimensions have to be checked.
        "output": {"[passenger]": 1, "[length]": -1, "[mass]": -1, "[time]": 2},
        "capacity_factor": {
            "[passenger]": 1,
            "[length]": 1,
            "[vehicle]": -1,
            "[time]": -1,
        },
        "fix_cost": {"[currency]": 1, "[vehicle]": -1, "[time]": -1},
    }
    # Check dimensionality of ikarus pars with items in dims:
    for par, dim in dims.items():
        units = data[par]["unit"].unique()
        assert len(units) == 1, "Units for each (par, tec) must be unique"
        # Unit is parseable by pint
        pint_unit = registry(units[0])
        # Unit has the correct dimensionality
        assert pint_unit.dimensionality == dim

    # Specific magnitudes of other values to check
    checks = [
        dict(par="capacity_factor", year_vtg=2010, value=0.000905),
        dict(par="technical_lifetime", year_vtg=2010, value=14.7),
        dict(par="capacity_factor", year_vtg=2050, value=0.000886),
        dict(par="technical_lifetime", year_vtg=2050, value=14.7),
    ]
    defaults = dict(node_loc=s_info.N[-1], technology="ICG_bus", time="year")

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
    test_context.update(
        {
            "transport config": {"data source": {"emissions": source}},
            "regions": regions,
        }
    )

    data = get_emissions_data(test_context)
    assert {"emission_factor"} == set(data.keys())
    assert rows == len(data["emission_factor"])


@pytest.mark.parametrize(
    "source, regions, years",
    [
        (None, "R11", "A"),
        ("US-TIMES MA3T", "R11", "A"),
        ("US-TIMES MA3T", "R14", "A"),
        # Not implemented
        ("US-TIMES MA3T", "R11", "B"),
        param("US-TIMES MA3T", "ISR", "A", marks=testing.NIE),
    ],
)
def test_get_ldv_data(test_context, source, regions, years):
    ctx = test_context

    # Info about the corresponding RES
    ctx.regions = regions
    ctx.years = years

    read_config()

    info = bare.get_spec(ctx)["add"]

    ctx["transport build info"] = info

    # Method runs without error
    ctx["transport config"]["data source"]["LDV"] = source
    data = get_ldv_data(ctx)

    # Output data is returned
    assert "output" in data

    for bound in ("lo", "up"):
        # Constraint data are returned. Use .pop() to exclude from the next assertions
        df = data.pop(f"growth_activity_{bound}")

        # Data covers all periods except the first
        assert info.Y[1:] == sorted(df["year_act"].unique())

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


@pytest.mark.parametrize("regions", ["R11", "R14", param("ISR", marks=testing.NIE)])
@pytest.mark.parametrize("pop_scen", ["GEA mix"])
def test_groups(test_context, regions, pop_scen):
    ctx = test_context
    ctx.regions = regions
    ctx["transport population scenario"] = pop_scen

    read_config(ctx)

    ctx["transport build info"] = bare.get_spec(ctx)["add"]

    result = get_consumer_groups(ctx)

    # Data have the correct size
    exp = dict(n=11, y=15, cg=27)

    # NB as of genno 1.3.0, can't use .sizes on AttrSeries:
    # assert result.sizes == exp
    obs = {dim: len(result.coords[dim]) for dim in exp.keys()}
    assert exp == obs, result.coords

    # Data sum to 1 across the consumer_group dimension, i.e. constitute a discrete
    # distribution
    assert (result.sum("cg") - 1.0 < 1e-08).all()


@pytest.mark.parametrize(
    "regions,years,pop_scen",
    [
        ("R11", "A", "GEA mix"),
        ("R11", "A", "GEA supply"),
        ("R11", "A", "GEA eff"),
        # Different years
        ("R11", "B", "GEA mix"),
        # Different regions & years
        ("R14", "B", "SSP1"),
        ("R14", "B", "SSP2"),
        ("R14", "B", "SSP3"),
        param("ISR", "B", "SSP2", marks=testing.NIE),
    ],
)
def test_urban_rural_shares(test_context, regions, years, pop_scen):
    ctx = test_context
    ctx.regions = regions
    ctx.years = years

    read_config(ctx)

    ctx["transport config"] = {"data source": {"population": pop_scen}}
    ctx["transport build info"] = info = bare.get_spec(ctx)["add"]

    # Shares can be retrieved
    result = get_urban_rural_shares(ctx)

    assert ("n", "y", "area_type") == result.dims
    assert set(info.N[1:]) == set(result.coords["n"].values)
    assert set(info.Y) <= set(result.coords["y"].values)
    assert set(["UR+SU", "RU"]) == set(result.coords["area_type"].values)


@pytest.mark.parametrize(
    "region, length",
    [
        (("Africa", "R11_AFR"), 224),
    ],
)
def test_get_afr_data(transport_context_f, region, length):
    ctx = transport_context_f

    df = get_roadmap_data(ctx, region)

    # Data covers all historical periods from the Roadmap model
    assert sorted(df["Year"].unique()) == [2000, 2005, 2010, 2015]
    # Modes match the list below
    assert list(df["Mode/vehicle type"].unique()) == [
        "2W_3W",
        "Bus",
        "Cars/light trucks",
        "Domestic passenger airplanes",
        "Freight trains",
        "Freight trucks",
        "Passenger trains",
    ]

    # Data have the correct size and format
    assert len(df["Mode/vehicle type"]) == length
    assert list(df.columns) == [
        "Mode/vehicle type",
        "Year",
        "Value",
        "Variable",
        "Units",
        "Region",
    ]


def test_get_gfei_data(transport_context_f):
    ctx = transport_context_f
    ctx.regions = "R11"

    df = get_gfei_data(ctx)

    # Data covers all historical periods from the Roadmap model
    assert list(df["Year"].unique()) == [2017]
    # Modes match the list below
    assert list(df["Mode/vehicle type"].unique()) == [
        "ICAe_ffv",
        "ICAm_ptrp",
        "ICH_chyb",
        "ICE_conv",
        "ELC_100",
        "ICE_lpg",
        "PHEV_ptrp",
        "ICE_nga",
        "HFC_ptrp",
    ]

    # Data have the correct size and format
    assert len(df["Mode/vehicle type"]) == 307
    assert list(df.columns) == [
        "Country",
        "Mode/vehicle type",
        "Value",
        "ISO_code",
        "Region",
        "Year",
        "Variable",
        "Units",
    ]


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
