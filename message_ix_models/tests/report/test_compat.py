from genno import Computer

from message_ix_models import ScenarioInfo
from message_ix_models.report import prepare_reporter
from message_ix_models.report.compat import callback, get_techs, prepare_techs

from ..test_report import MARK, ss_reporter


@MARK[0]
def test_compat(test_context):
    import numpy.testing as npt

    rep = ss_reporter()
    prepare_reporter(test_context, reporter=rep)

    rep.add("scenario", ScenarioInfo(model="Model name", scenario="Scenario name"))

    # Tasks can be added to the reporter
    callback(rep, test_context)

    key = "transport emissions full::iamc"  # IAMC structure
    # key = "Transport"  # Top level
    # key = "Hydrogen_trp"  # Second level
    # key = "inp_nonccs_gas_tecs_wo_CCSRETRO"  # Third level
    # key = "_26"  # Fourth level

    # print(rep.describe(key))
    # rep.visualize("transport-emissions-full-iamc.svg", key)

    # Calculation runs
    result = rep.get(key)

    # print(result.to_string())
    # print(result.as_pandas().to_string())

    # Check a specific value
    # TODO Expand set of expected values
    npt.assert_allclose(
        result.as_pandas()
        .query("region == 'R11_AFR' and year == 2020")["value"]
        .item(),
        54.0532,
    )


def test_prepare_techs(test_context):
    from message_ix_models.model.bare import get_spec
    from message_ix_models.report.compat import TECH_FILTERS

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
