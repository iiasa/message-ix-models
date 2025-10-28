# -*- coding: utf-8 -*-
"""
Utility functions for the bilateralize tool
"""
# Import packages
import logging
import os
import sys
from typing import TypedDict

import yaml

from message_ix_models.util import package_data_path


def get_logger(name: str):

    # Set the logging level to INFO (will show INFO and above messages)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    # Define the format of log messages:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

    # Apply the format to the handler
    handler.setFormatter(formatter)

    # Add the handler to the logger
    log.addHandler(handler)

    return log

#%% Load config yaml
def load_config(project_name:str | None = None,
                config_name:str | None = None,
                load_tec_config:bool = False):
    """
    Load config file and optional trade-specific config files.

    Args:
        project_name: Name of the project (message_ix_models/project/[THIS])
        config_name: Name of the base config file (e.g., config.yaml)
        load_tec_config: If True, load the trade-specific config files

    Returns:
        config: Config dictionary (base config)
        config_path: Path to the config file
        tec_config_dict: Dictionary of trade-specific config file
    """
    # Load config
    if project_name is None:
        config_path = os.path.abspath(os.path.join(os.path.dirname(
            package_data_path("bilateralize")),
            "bilateralize", "configs", "base_config.yaml"))
    if project_name is not None:
        if config_name is None:
            config_name = "config.yaml"
        config_path = os.path.abspath(os.path.join(os.path.dirname(
            package_data_path(project_name)),
            os.path.pardir, "project", project_name, config_name))

    with open(config_path, "r") as f:
        config = yaml.safe_load(f) # safe_load is recommended over load for security

    if not load_tec_config:
        return config, config_path
    else:
        tec_config_dict = {}
        for tec in config['covered_trade_technologies']:
            tec_config_path = os.path.abspath(os.path.join(
                os.path.dirname(package_data_path("bilateralize")),
                "bilateralize", "configs", tec + ".yaml"))
            with open(tec_config_path, "r") as f:
                tec_config = yaml.safe_load(f)
            tec_config_dict[tec] = tec_config
        return config, config_path, tec_config_dict
