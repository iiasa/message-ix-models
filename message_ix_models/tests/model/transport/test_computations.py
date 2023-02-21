import pytest

from message_data.model.transport import Config
from message_data.model.transport.computations import factor_input
from message_data.model.transport.config import ScenarioFlags
from message_data.model.transport.util import get_techs


@pytest.mark.parametrize(
    "options, any_change",
    (
        ({}, False),
        (dict(flags=ScenarioFlags.TEC), True),
        (dict(flags=ScenarioFlags.ACT), False),
        (dict(flags=ScenarioFlags.NAVIGATE), True),  # i.e. all
    ),
)
def test_factor_input(test_context, options, any_change):
    cfg = Config.from_context(test_context, options=options)

    # Simulate inputs appearing in a Computer
    y = [2020, 2045, 2050, 2060, 2110]
    spec, techs, t_groups = get_techs(test_context)

    # Function runs
    result = factor_input(y, techs, dict(t=t_groups), dict(transport=cfg))

    # No change to 2020 values
    assert all(1.0 == result.sel(y=2020))

    # Check intermediate values
    k = 5 if any_change else 0
    t = "ELC_100"
    assert all((1 - 0.015) ** k == result.sel(y=2050, t=t) / result.sel(y=2045, t=t))
    t = "FR_ICE_L"
    assert all((1 - 0.02) ** k == result.sel(y=2050, t=t) / result.sel(y=2045, t=t))
    t = "con_ar"
    assert all((1 - 0.013) ** k == result.sel(y=2050, t=t) / result.sel(y=2045, t=t))

    # No change after 2050
    assert all(1.0 == result.sel(y=2060) / result.sel(y=2050))
    assert all(1.0 == result.sel(y=2110) / result.sel(y=2050))
