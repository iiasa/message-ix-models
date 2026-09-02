"""
Workflow for developing baseline scenarios and bilateralizing them for fuel security project
"""
import logging
import os
from ixmp import Platform

# Import tools
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.weu_security.liquefaction_calibration import *
from message_ix_models.project.weu_security.adjust_reexports import *

from message_ix_models import Context
from message_ix_models.util import private_data_path
from message_ix_models.workflow import Workflow

from message_ix_models.tools.policy import (
    add_NPi2030,
)

log = logging.getLogger(__name__)
