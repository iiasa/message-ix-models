from message_ix_models import ScenarioInfo
from message_ix_models.report import prepare_reporter
from message_ix_models.report.compat import callback

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
