import logging
from pathlib import Path
from typing import TYPE_CHECKING

from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix_models.util.context import Context

log = logging.getLogger(__name__)


def configure(context: "Context") -> None:
    """Configure for the CircEUlar BMT workflow.

    This function **should** have an effect identical to
    :func:`.bmt.config.apply_bmt_config`.
    """
    from message_ix_models.model.buildings.config import METHOD
    from message_ix_models.model.buildings.config import Config as BuildingsConfig
    from message_ix_models.model.transport.config import Config as TransportConfig

    context.model.regions = "R12"

    # Expected by .bmt.workflow.add_steps()
    context.bmt = dict(model_name="MESSAGEix-GLOBIOM 2.1-BMT-R12 CircEUlar")

    context.buildings = BuildingsConfig(
        sturm_scenario="NONE",
        data_paths=dict(
            # Input file names under private_data_path("buildings")
            prices=Path("input_prices_R12.csv"),
            sturm_r=Path("resid_sturm_aligned_{code}.csv"),
            sturm_c=Path("comm_sturm_aligned_{code}.csv"),
            demand_static=Path("resid_comm_glance_aligned_R.csv"),
            sturm_c_ref=Path("comm_sturm_aligned_R.csv"),
            sturm_r_ref=Path("resid_sturm_aligned_R.csv"),
        ),
        method=METHOD.C,
        with_materials=True,
    )

    # Expected by .bmt.workflow.add_macro()
    context.macro = "macro_calibration_input.xlsx"

    TransportConfig.from_context(context, options=dict(code="SSP2"))

    # log.info(repr(context.asdict()))  # DEBUG


def generate(context: "Context") -> Workflow:
    """Create the CircEUlar workflow."""
    from message_ix_models.model.bmt.workflow import add_steps

    from .structure import CL_SCENARIO

    # Configure
    configure(context)

    # Create the workflow
    wf = Workflow(context)

    # TODO Move this to a .Config.base_url setting on an appropriate config class
    #      (probably .model.workflow.Config).
    p = context.core.platform_info["name"]
    base_url = f"ixmp://{p}/SSP_SSP2_v6.5/baseline_DEFAULT_step_14"

    # Load the base scenario
    base_step = wf.add_step("M", None, target=base_url)

    # Iterate over all CircEUlar scenario IDs
    for scenario_code in CL_SCENARIO.get():
        add_steps(wf, base_step, prefix=f"{scenario_code.id} ")

    return wf
