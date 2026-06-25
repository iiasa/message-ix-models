import logging

import numpy as np
import pytest
from pytest import mark
from sdmx.model.common import Code

from message_ix_models import Context
from message_ix_models.model.transport import CL_SCENARIO, Config, build, testing
from message_ix_models.model.transport.CHN_IND import get_chn_ind_data, get_chn_ind_pop
from message_ix_models.model.transport.data import (
    LABEL_SUBS,
    LoadFactorLDV,
    MultiFile,
    collect_structures,
    read_structures,
)
from message_ix_models.model.transport.roadmap import get_roadmap_data
from message_ix_models.project.circeular.structure import (
    CL_SCENARIO as CL_SCENARIO_CIRCEULAR,
)
from message_ix_models.project.navigate import T35_POLICY


@pytest.fixture(scope="module")
def codes() -> list[tuple[Code, Code]]:
    """All possible tuples of (:attr:`.Config.code`, :attr:`.project_scenario_code`).

    These include:

    1. Each of :class:`.transport.config.CL_SCENARIO` with no project-specific code.
    2. "M SSP2" with each code from :class:`.circeular.structure.CL_SCENARIO`.
    """
    cl = CL_SCENARIO.get()
    result = [(code, None) for code in cl] + [
        (cl["M SSP2"], code) for code in CL_SCENARIO_CIRCEULAR.get()
    ]
    assert 392 == len(result)
    return result


class TestMultiFile:
    def test_filename(self) -> None:
        with pytest.raises(NotImplementedError):
            MultiFile().filename


class TestLoadFactorLDV:
    def test_filename(
        self, caplog: pytest.LogCaptureFixture, codes: list[tuple[Code, Code]]
    ) -> None:
        """:attr:`LoadFactorLDV.filename` works for all defined scenario codes."""
        cfg: Config = Config()

        caplog.set_level(logging.INFO + 1, "message_ix_models.model.transport.data")
        for cfg.code, cfg.project_scenario_code in codes:
            try:
                obj = LoadFactorLDV(config=cfg, nodes="R12")
            except Exception:  # pragma: no cover
                print(cfg.code, cfg.project_scenario_code)
                raise
            assert obj.filename.endswith(".csv")


@mark.sdmx_230
def test_collect_structures():
    sm1 = collect_structures()

    sm2 = read_structures()

    # Structures are retrieved from file successfully
    # The value is either 30 or 31 depending on whether .build.add_exogenous_data() has
    # run
    assert 30 <= len(sm1.dataflow) == len(sm2.dataflow)


@mark.non_public_data("RoadmapResults_2017.xlsx")
@mark.parametrize(
    "region, length",
    [
        (("Africa", "R11_AFR"), 224),
    ],
)
def test_get_afr_data(test_context: Context, region: str, length: int) -> None:
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


@mark.skip("Pending https://github.com/transportenergy/database/issues/75")
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


@pytest.mark.parametrize(
    "subs, exp_all, exp",
    (
        (
            "A",
            # Existing files in R12/load-factor-ldv/*.csv
            set(
                """DIGSY-BEST-C DIGSY-BEST-S DIGSY-WORST-S DIGSY-WORST-C EDITS-CA
                EDITS-HA LED SSP_2024_1 SSP_2024_2 SSP_2024_3 SSP_2024_4
                SSP_2024_5""".split()
            ),
            {"CircEUlar-C": "SSP_2024_2", "CircEUlar-N": "DIGSY-BEST-C"},
        ),
        (
            "B",
            # Appearing in lifetime.csv
            {"*", "CircEUlar-A", "CircEUlar-N", "CircEUlar-S"},
            {"CircEUlar-C": "*", "CircEUlar-E": "CircEUlar-A", "SSP_2024.1": "*"},
        ),
        (
            "C",
            # Appearing in activity-vehicle.csv
            {"*", "CircEUlar-A", "CircEUlar-N"},
            {"CircEUlar-S": "*", "CircEUlar-E": "CircEUlar-A", "SSP_2024.2": "*"},
        ),
    ),
)
def test_label_subs(
    codes: tuple[Code, Code],
    subs: str,
    exp_all: set[str],  # Resulting values must appear in this set
    exp: dict[str, str],  # Specific mappings from Config.label to result
) -> None:
    """:any:`LABEL_SUBS` set `subs` maps to one of the expected values."""
    cfg = Config()

    for c in codes:
        cfg.code, cfg.project_scenario_code = c
        result = LABEL_SUBS[subs](cfg.label)
        # Result is mapped to one of the expected set, and any specific result matches
        assert result in exp_all and exp.pop(cfg.label, result) == result, c

    assert not exp  # All expected values were seen


@build.get_computer.minimum_version
@mark.transport_build_data
@mark.parametrize("years", ["A", "B"])
@mark.parametrize(
    "regions",
    [
        pytest.param("ISR", marks=mark.ISR_no_data),
        "R11",
        "R12",
        "R14",
    ],
)
@mark.parametrize("options", [{}, dict(navigate_scenario=T35_POLICY.ELE)])
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
