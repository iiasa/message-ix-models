import logging
from typing import TYPE_CHECKING

from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix_models.util.context import Context

log = logging.getLogger(__name__)


def generate(context: "Context") -> Workflow:
    """Create the CircEUlar workflow."""
    from message_ix_models.model.bmt.config import apply_bmt_config
    from message_ix_models.model.bmt.workflow import add_steps

    from .structure import CL_SCENARIO

    ### Same as .bmt.workflow.generate() until ###
    wf = Workflow(context)

    # Configure
    context.ssp = "SSP2"
    context.model.regions = "R12"
    apply_bmt_config(context)
    log.info(repr(context.asdict()))

    # TODO Move this to a .Config.base_url setting on an appropriate config class
    #      (probably .model.workflow.Config).
    base_url = "ixmp://ixmp-dev/SSP_SSP2_v6.5/baseline_DEFAULT_step_14"

    ###

    # Load the base scenario
    base_step = wf.add_step("M", None, target=base_url)

    # Iterate over all CircEUlar scenario IDs
    for scenario_code in CL_SCENARIO.get():
        add_steps(wf, base_step, prefix=f"{scenario_code.id} ")

    return wf
