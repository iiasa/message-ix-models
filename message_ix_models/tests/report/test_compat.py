import logging

from genno import Computer
from ixmp.testing import assert_logs

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.report import prepare_reporter
from message_ix_models.report.compat import (
    TECH_FILTERS,
    callback,
    get_techs,
    prepare_techs,
)
from message_ix_models.report.sim import to_simulate

from ..test_report import simulated_solution_reporter


@to_simulate.minimum_version
def test_compat(tmp_path, test_context):
    import numpy.testing as npt

    rep = simulated_solution_reporter()
    prepare_reporter(test_context, reporter=rep)

    rep.add("scenario", ScenarioInfo(model="Model name", scenario="Scenario name"))

    # Tasks can be added to the reporter
    callback(rep, test_context)

    # Select a key
    key = (
        "transport emissions full::iamc"  # IAMC structure
        # "Transport"  # Top level
        # "Hydrogen_trp"  # Second level
        # "inp_nonccs_gas_tecs_wo_CCSRETRO"  # Third level
        # "_26"  # Fourth level
    )

    # commented: Show what would be done
    # print(rep.describe(key))
    # rep.visualize(tmp_path.joinpath("visualize.svg"), key)

    # Calculation runs
    result = rep.get(key)

    # print(result.to_string())  # For intermediate results

    df = result.as_pandas()  # For pyam.IamDataFrame, which doesn't have .to_string()

    # commented: Display or save output
    # print(df.to_string())
    # df.to_csv(tmp_path.joinpath("transport-emissions-full.csv"), index=False)

    # Check a specific value
    # TODO Expand set of expected values
    npt.assert_allclose(
        df.query("region == 'R11_AFR' and year == 2020")["value"].item(), 54.0532
    )


def test_prepare_techs(test_context):
    from message_ix_models.model.bare import get_spec

    # Retrieve a spec with the default set of technologies
    spec = get_spec(test_context)
    technologies = spec.add.set["technology"]

    c = Computer()
    prepare_techs(c, technologies)

    # Expected sets of technologies based on the default technology.yaml
    assert {
        "gas all": [
            "gas_cc",
            "gas_ct",
            "gas_fs",
            "gas_hpl",
            "gas_htfc",
            "gas_i",
            "gas_ppl",
            "gas_rc",
            "gas_t_d",
            "gas_t_d_ch4",
            "gas_trp",
            "hp_gas_i",
            "hp_gas_rc",
        ],
        "gas extra": [],
        # Residential and commercial
        "rc gas": ["gas_rc", "hp_gas_rc"],
        # Transport
        "trp coal": ["coal_trp"],
        "trp foil": ["foil_trp"],
        "trp gas": ["gas_trp"],
        "trp loil": ["loil_trp"],
        "trp meth": ["meth_fc_trp", "meth_ic_trp"],
    } == {k: get_techs(c, k) for k in TECH_FILTERS}


def test_prepare_techs_filter_error(caplog, monkeypatch):
    """:func:`.prepare_techs` logs warnings for invalid filters."""
    monkeypatch.setitem(TECH_FILTERS, "foo", "not a filter")

    with assert_logs(caplog, "SyntaxError('invalid syntax", at_level=logging.WARNING):
        prepare_techs(Computer(), get_codes("technology"))
