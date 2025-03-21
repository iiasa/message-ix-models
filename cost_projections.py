import pandas as pd
import ixmp
import message_ix
import sys
import os

from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections
from message_ix_models.util.common import package_data_path


cfg = Config()

res_r12_energy = create_cost_projections(cfg)
