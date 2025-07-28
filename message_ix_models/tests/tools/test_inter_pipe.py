

import pytest

from message_ix_models.util import package_data_path

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from message_ix import Scenario
    from message_ix_models import Context


from message_ix_models import testing

from message_ix_models.tools.inter_pipe import inter_pipe_bare, inter_pipe_build



@pytest.fixture
def scenario(request: "pytest.FixtureRequest", test_context: "Context") -> "Scenario":
    # Code only functions with R12
    test_context.model.regions = "R12"
    return testing.bare_res(request, test_context)

def test_config_yaml_exists() -> None:
    """Test that the default config.yaml file exists for inter_pipe."""
    config_path = package_data_path("inter_pipe", "config.yaml")
    assert config_path.exists(), f"Config file not found at: {config_path}"
    print(f"Config file found at: {config_path}")


