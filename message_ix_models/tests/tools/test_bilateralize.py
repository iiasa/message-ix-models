"""
Test the bilateralize tool
"""

# Import packages
import os

import pytest

from message_ix_models import Context, testing
from message_ix_models.tools.bilateralize.bare_to_scenario import bare_to_scenario
from message_ix_models.tools.bilateralize.load_and_solve import (
    add_trade_parameters,
    add_trade_sets,
    remove_pao_coal_constraint,
    remove_trade_tech,
    update_additional_parameters,
    update_bunker_fuels,
    update_relation_parameters,
)
from message_ix_models.tools.bilateralize.prepare_edit import (
    prepare_edit_files,
)
from message_ix_models.tools.bilateralize.utils import get_logger, load_config
from message_ix_models.util import package_data_path

# Get logger
log = get_logger(__name__)


def test_generate_edit_files(access_to_p: bool = False):
    """
    Checks that required files are generated in both
    edit_files and bare_files directories.
    """
    config_base, config_path, config_tec = load_config(
        project_name=None, config_name=None, load_tec_config=True
    )
    data_path = package_data_path("bilateralize")
    data_path = os.path.join(os.path.dirname(data_path), "bilateralize")

    prepare_edit_files(project_name=None, config_name=None, P_access=access_to_p)

    req_files = ["input", "output", "technical_lifetime", "capacity_factor"]

    for tec in config_tec.keys():
        for file in req_files:
            assert os.path.isfile(
                os.path.join(data_path, tec, "edit_files", file + ".csv")
            )
            assert os.path.isfile(
                os.path.join(data_path, tec, "bare_files", file + ".csv")
            )


def test_build_parameter_sheets():
    """
    Checks that dictionary of parameter dataframes is built correctly.
    """
    config_base, config_path, config_tec = load_config(
        project_name=None, config_name=None, load_tec_config=True
    )

    test_dict = bare_to_scenario(
        project_name=None, config_name=None, p_drive_access=False
    )

    assert isinstance(test_dict, dict)
    assert len(test_dict) == len(config_tec)
    for tec in config_tec.keys():
        assert isinstance(test_dict[tec], dict)
        assert "trade" in test_dict[tec].keys()
        assert "flow" in test_dict[tec].keys()


def test_bilat_scenario(request: pytest.FixtureRequest, test_context: Context):
    """
    Test bilateralization on a scenario.
    """
    config_base, config_path, config_tec = load_config(
        project_name=None, config_name=None, load_tec_config=True
    )

    # Set up test scenario
    test_context.model.regions = "R12"
    scen = testing.bare_res(request, test_context)

    # Add unit
    mp = scen.platform
    mp.add_unit("Mt-km")

    # Add global node
    with scen.transact("Add global node"):
        scen.add_set("node", "R12_GLB")

    # Set up bilateralization dictionary
    bilat_dict = bare_to_scenario(
        project_name=None, config_name=None, p_drive_access=False
    )

    covered_tec = config_base.get("covered_trade_technologies")
    for tec in covered_tec:
        # Remove existing technologies related to trade
        remove_trade_tech(scen=scen, log=log, config_tec=config_tec, tec=tec)

        # Add to sets: technology, level, commodity, mode
        add_trade_sets(scen=scen, log=log, trade_dict=bilat_dict, tec=tec)

        # Add parameters
        add_trade_parameters(scen=scen, log=log, trade_dict=bilat_dict, tec=tec)

        # Relation activity, upper, and lower
        update_relation_parameters(scen=scen, log=log, trade_dict=bilat_dict, tec=tec)

        # Update bunker fuels
        update_bunker_fuels(scen=scen, tec=tec, log=log, config_tec=config_tec)

    # Update additional parameters
    update_additional_parameters(scen=scen, extra_parameter_updates=None)

    # Remove PAO coal and gas constraints on MESSAGEix-GLOBIOM (won't run in test)
    remove_pao_coal_constraint(scen=scen, log=log, MESSAGEix_GLOBIOM=False)

    assert True
