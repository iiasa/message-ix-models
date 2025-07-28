# The workflow mainly contains the steps to build bmt baseline,
# as well as the steps to apply policy scenario settings. See bmt-workflow.svg. 
# Example cli command:
# mix-models bmt run --from="base" "glasgow+" --dry-run

import logging
import message_ix
import pandas as pd
import logging
import os
import genno
import re

from typing import Optional
from itertools import product
from message_ix import Scenario
from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.build import apply_spec
from message_ix_models.util import (
    package_data_path,
    nodes_ex_world, 
    make_io, 
    add_par_data,
)
from message_ix_models.workflow import Workflow

from message_ix_models.model.buildings.build import build_B as build_B
from message_ix_models.model.material.build import build_M as build_M
# from message_ix_models.model.transport.build import build as build_T

log = logging.getLogger(__name__)

# Functions for individual workflow steps

def solve(
    context: Context, 
    scenario: message_ix.Scenario, 
    model="MESSAGE"
    ) -> message_ix.Scenario:

    """Plain solve."""
    message_ix.models.DEFAULT_CPLEX_OPTIONS = {
    "advind": 0,
    "lpmethod": 4,
    "threads": 4,
    "epopt": 1e-6,
    "scaind": -1,
    # "predual": 1, 
    "barcrossalg": 0,
    }

    # scenario.solve(model, gams_args=["--cap_comm=0"])
    scenario.solve(model)
    scenario.set_as_default()

    return scenario

def check_context(
    context: Context, 
    scenario: message_ix.Scenario, 
    ) -> message_ix.Scenario:

    context.print_contents()

    return scenario

# Main BMT workflow
def generate(context: Context) -> Workflow:
    """Create the BMT-run workflow."""
    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

    # Define model name
    model_name = "ixmp://ixmp-dev/MESSAGEix-GLOBIOM BMT-R12"

    wf.add_step(
        "base",
        None,
        target="ixmp://ixmp-dev/SSP_SSP2_v6.1/baseline_DEFAULT_step_4", 
        # target = f"{model_name}/baseline",
    )

    wf.add_step(
        "base cloned",
        "base",
        check_context,
        # target="ixmp://ixmp-dev/SSP_SSP2_v4.0/baseline_DEFAULT_step_4", 
        target = f"{model_name}/baseline",
        clone = dict(keep_solution=False),
    )

    wf.add_step(
        "base solved",
        "base cloned",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline", 
        clone = False,
    )


    wf.add_step(
        "M built",  
        "base solved", 
        build_M,
        target = f"{model_name}/baseline_M",
        clone = dict(keep_solution=False),
    )

    wf.add_step(
        "M solved",
        "M built",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_M", 
        clone = False,
    )

    wf.add_step(
        "B built",
        "M solved",
        build_B,
        target = f"{model_name}/baseline_BM", #BM later
        clone = dict(keep_solution=False),
    )

    wf.add_step(
        "BM solved",
        "B built",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_BM", #BM later
        clone = dict(keep_solution=False),
    )

    wf.add_step(
        "T built",
        "BM solved",
        build_T,
        target = f"{model_name}/baseline_BMT",
        clone = dict(keep_solution=False),
    )    

    wf.add_step(
        "BMT baseline solved",
        "T built",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_BMT",
        clone = False,
    )

    wf.add_step(
        "NPi2030",
        "BMT baseline solved",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_BMT",
        clone = False,
    )

    wf.add_step(
        "NPi_forever",
        "NPi2030",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_BMT",
        clone = False,
    )

    wf.add_step(
        "NDC2030",
        "BMT baseline solved",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_BMT",
        clone = False,
    )

    wf.add_step(
        "glasgow",
        "NDC2030",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_BMT",
        clone = False,
    )

    wf.add_step(
        "glasgow+",
        "NDC2030",
        solve,
        model = "MESSAGE",
        target = f"{model_name}/baseline_BMT",
        clone = False,
    )

    return wf
