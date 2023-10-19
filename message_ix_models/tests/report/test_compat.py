from message_ix_models import ScenarioInfo
from message_ix_models.report import prepare_reporter
from message_ix_models.report.compat import callback, prepare_techs

from ..test_report import MARK, ss_reporter


@MARK[0]
def test_compat(test_context):
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

    # Calculation runs
    result = rep.get(key)

    # print(result.to_string())
    # print(result.as_pandas().to_string())
    del result


def test_prepare_techs(test_context):
    from message_ix_models.model.bare import get_spec
    from message_ix_models.report.compat import TECHS

    spec = get_spec(test_context)

    prepare_techs(spec.add.set["technology"])

    # Expected sets of technologies based on the default technology.yaml
    assert {
        "gas extra": [],
        # Residential and commercial
        "rc gas": ["gas_rc", "hp_gas_rc"],
        # Transport
        "trp coal": ["coal_trp"],
        "trp foil": ["foil_trp"],
        "trp gas": ["gas_trp"],
        "trp loil": ["loil_trp"],
        "trp meth": ["meth_fc_trp", "meth_ic_trp"],
    } == TECHS
