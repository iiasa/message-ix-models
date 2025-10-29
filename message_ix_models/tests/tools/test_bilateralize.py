"""
Test the bilateralize tool
"""
# Import packages
import ixmp
import pytest
import yaml

from message_ix_models.tools.bilateralize.utils import get_logger, load_config
from message_ix_models.tools.bilateralize.bare_to_scenario import clone_and_update
from message_ix_models.tools.bilateralize.prepare_edit import generate_bare_sheets, build_parameter_sheets
from message_ix_models.util import package_data_path

from message_ix_models import ScenarioInfo, testing, Context

# Connect to ixmp
mp = ixmp.Platform()

# Get logger
log = get_logger(__name__)

# Test generate_bare_sheets()
@pytest.fixture
def test_generate_bare_sheets():
    """
    Checks that required files are generated in both edit_files and bare_files directories.
    """
    config_base, config_path, config_tec = load_config()
    data_path = package_data_path("bilateralize")
    data_path = os.path.join(os.path.dirname(data_path), "bilateralize")
    
    generate_bare_sheets(log=log)
    
    req_files = ['input', 'output', 'technical_lifetime', 'capacity_factor', 'var_cost', 'inv_cost', 'fix_cost']

    for tec in config_tec.keys():
        for file in req_files:
            assert os.path.isfile(os.path.join(data_path, tec, "edit_files", file + ".csv"))
            assert os.path.isfile(os.path.join(data_path, tec, "bare_files", file + ".csv"))

# Test build_parameter_sheets()
@pytest.fixture
def test_build_parameter_sheets():
    """
    Checks that dictionary of parameter dataframes is built correctly.
    """
    config_base, config_path, config_tec = load_config()

    test_dict = build_parameter_sheets(log=log)

    assert isinstance(test_dict, dict)
    assert len(test_dict) == len(config_tec)
    for tec in config_tec.keys():
        assert isinstance(test_dict[tec], dict)
        assert 'trade' in test_dict[tec].keys()
        assert 'flow' in test_dict[tec].keys()

# Test on scenario
@pytest.fixture
def test_bilat_scenario(
    request: "pytest.FixtureRequest",
    test_context: "Context"):

    test_dict = build_parameter_sheets(log=log)
    clone_and_update(trade_dict=test_dict,
                    log=log,
                    solve=False,
                    to_gdx=False,
                    update_scenario_name='test_bilat_scenario',
                    additional_parameter_updates=None,
                    remove_pao_coal_constraint=True)

    assert True