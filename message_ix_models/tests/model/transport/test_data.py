import numpy as np
import pytest

from message_ix_models.model.transport import build, testing
from message_ix_models.model.transport.CHN_IND import get_chn_ind_data, get_chn_ind_pop
from message_ix_models.model.transport.data import collect_structures, read_structures
from message_ix_models.model.transport.roadmap import get_roadmap_data
from message_ix_models.model.transport.testing import MARK, make_mark
from message_ix_models.project.navigate import T35_POLICY


@MARK["sdmx#230"]
def test_collect_structures():
    sm1 = collect_structures()

    sm2 = read_structures()

    # Structures are retrieved from file successfully
    # The value is either 30 or 31 depending on whether .build.add_exogenous_data() has
    # run
    assert 30 <= len(sm1.dataflow) == len(sm2.dataflow)


@make_mark[5]("RoadmapResults_2017.xlsx")
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
@MARK[10]
@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions",
    [
        pytest.param("ISR", marks=MARK[3]),
        "R11",
        "R12",
        "R14",
    ],
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
    # print(f"{techs = }")
    assert {"ICAe_ffv", "ICE_nga", "IGH_ghyb", "FR_ICE_M", "FR_ICE_L"} <= techs

    # Electric technologies are not constrained
    assert {"ELC_100", "FR_FCH"}.isdisjoint(techs)
