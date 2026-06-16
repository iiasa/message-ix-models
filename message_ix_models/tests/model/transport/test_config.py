from collections.abc import Iterator

import pytest
import xarray as xr
from genno.operator import as_quantity
from genno.testing import assert_qty_equal
from iam_units import registry

from message_ix_models import Context
from message_ix_models.model.transport.config import (
    CL_SCENARIO,
    Config,
    DataSourceConfig,
    iter_price_emission,
)
from message_ix_models.project.navigate import T35_POLICY
from message_ix_models.project.ssp import SSP_2017, SSP_2024

NAVIGATE = (
    ("", T35_POLICY.REF),
    ("act", T35_POLICY.ACT),
    ("ele", T35_POLICY.ELE),
    ("tec", T35_POLICY.TEC),
    ("act+ele+tec", T35_POLICY.ALL),
    pytest.param("foo+act+tec", None, marks=pytest.mark.xfail(raises=ValueError)),
)

SSP = (
    ("1", SSP_2017["1"]),
    ("2", SSP_2017["2"]),
    ("3", SSP_2017["3"]),
    ("4", SSP_2017["4"]),
    ("5", SSP_2017["5"]),
    (SSP_2024["2"], SSP_2024["2"]),
)


class TestConfig:
    @pytest.fixture
    def c(self) -> Iterator[Config]:
        yield Config()

    @pytest.mark.parametrize(
        "regions",
        [
            None,  # Default per message_ix_models.model.Config
            "R11",
            "R12",
            "R14",
            pytest.param("ISR", marks=pytest.mark.xfail(raises=AssertionError)),
        ],
    )
    def test_from_context0(self, test_context, regions) -> None:
        """Configuration can be read from files.

        This exercises :meth:`.Config.from_context`.
        """
        # Set the regional aggregation to be used
        ctx = test_context
        if regions:
            ctx.model.regions = regions

        # Returns the same object stored as Context["transport"]
        cfg = Config.from_context(ctx)

        assert cfg is ctx["transport"]

        # Attributes have the correct types
        assert isinstance(cfg.data_source, DataSourceConfig)

        # Scalar parameters are loaded
        assert cfg.scaling
        assert_qty_equal(
            as_quantity("200 * 8 hours / passenger / year"), cfg.work_hours
        )

        # Codes for the consumer_group set are generated
        codes = cfg.spec.add.set["consumer_group"]
        RUEAA = codes[codes.index("RUEAA")]
        assert "Rural, or “Outside MSA”, Early Adopter, Average" == str(RUEAA.name)

        # xarray objects are generated for advanced indexing
        indexers = cfg.spec.add.set["consumer_group indexers"]
        assert all(isinstance(da, xr.DataArray) for da in indexers.values())  # type: ignore [attr-defined]

        # Codes for commodities are generated
        codes = cfg.spec.add.set["commodity"]
        RUEAA = codes[codes.index("transport pax RUEAA")]
        assert RUEAA.eval_annotation("demand") is True

        # …with expected units
        r = dict(registry=registry)
        assert registry.Unit("Gp km") == RUEAA.eval_annotation("units", r)

        # Codes for technologies are generated, with annotations giving their units
        codes = cfg.spec.add.set["technology"]
        ELC_100 = codes[codes.index("ELC_100")]
        assert registry.Unit("Gv km") == ELC_100.eval_annotation("units", r)

        # If "ISR" was given as 'regions', then the corresponding config file was loaded
        if regions == "ISR":
            # Check one config value to confirm
            assert {"Israel"} == set(cfg.node_to_census_division.keys())

    @pytest.mark.parametrize(
        "options",
        [
            {},
            pytest.param(
                {"mode-share": "default"}, marks=pytest.mark.xfail(raises=TypeError)
            ),
            {"mode_share": "default"},
            {"mode_share": "INVALID"},
        ],
    )
    def test_from_context1(self, test_context, options) -> None:
        """:meth:`.Config.from_context` operates with various options."""
        Config.from_context(test_context, options=options)

    def test_fields(self, c: Config) -> None:
        """Settable class property included in :meth:`.ConfigHelper._fields`."""
        assert {"code"} & c._fields()

    @pytest.mark.parametrize("input, expected", SSP)
    def test_ssp0(self, input, expected):
        """Set SSP through the constructor."""
        c = Config(ssp=input)  # Call succeeds
        assert expected == c.ssp  # The expected enum value is stored

    @pytest.mark.parametrize("input, expected", SSP)
    def test_ssp1(self, c, input, expected):
        """Set SSP on an existing instance."""
        c.ssp = input
        assert expected == c.ssp

    @pytest.mark.parametrize("input, expected", NAVIGATE)
    def test_navigate_scenario0(self, input, expected):
        """Set NAVIGATE scenario through the constructor."""
        c = Config(navigate_scenario=input)
        assert expected == c.project["navigate"]

    @pytest.mark.parametrize("input, expected", NAVIGATE)
    def test_navigate_scenario1(self, c, input, expected):
        """Set NAVIGATE scenario on an existing instance."""
        c.set_navigate_scenario(input)
        assert expected == c.project["navigate"]


class TestCL_SCENARIO:
    def test_get(self, test_context: Context) -> None:
        result = CL_SCENARIO.get(force=True)

        # Code list has the expected length
        assert 386 == len(result)

        # Code list contains codes with the expected IDs
        assert {
            "DIGSY-BEST-C",
            "DIGSY-BEST-S",
            "DIGSY-WORST-C",
            "DIGSY-WORST-S",
            "EDITS-CA",
            "EDITS-HA",
            "LED-SSP1",
            "LED-SSP2",
            "SSP1 tax",
            "SSP1",
            "SSP2 tax",
            "SSP2",
            "SSP3 tax",
            "SSP3",
            "SSP4 tax",
            "SSP4",
            "SSP5 tax",
            "SSP5",
        } <= set(result.items.keys())

        # Codes for material-enabled scenarios are present
        c = result["M SSP2"]

        # Config created using these codes has the 'material' module enabled
        cfg = Config.from_context(test_context, dict(code=c))
        assert "material" in cfg.modules

        # Codes with policies discovered in the data dir are present
        c = result["LED-SSP2 exo price 1aa5"]
        c = result["M SSP2 exo price 2e17"]

        assert "SSP_SSP2_v5.3.1/SSP2 - Low Emissions#2" in str(
            c.get_annotation(id="policy").text
        )

        # Codes with project-specific scenario information can be inspected
        cfg = Config()
        cfg.code = result["DIGSY-BEST-C"]

        assert cfg.project_scenario_code is not None
        assert "BEST-C" == cfg.project_scenario_code.id
        assert cfg.project_scenario_code.parent is not None
        assert "CL_SCENARIO_DIGSY" == cfg.project_scenario_code.parent.id


@pytest.mark.parametrize(
    "ssp_or_led, N_exp",
    (
        ("SSP1", 10),
        ("SSP2", 27),
        ("SSP3", 3),
        ("SSP4", 7),
        ("SSP5", 10),
    ),
)
def test_iter_price_emission(ssp_or_led: str, N_exp: int, regions="R12") -> None:
    # Currently only data available for R12
    result = list(iter_price_emission(regions, ssp_or_led))
    assert N_exp == len(result)
