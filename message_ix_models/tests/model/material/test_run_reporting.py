import pytest
from pytest import param

from message_ix_models.model.material.report.run_reporting import load_config, run
from message_ix_models.tests.test_tools import scenario


@pytest.mark.parametrize(
    "config_name, exp_len",
    [
        param("fe", 705),
        param("fe_methanol_ammonia", 74),
        param("fs1", 46),
        param("fs2", 38),
        param("prod", 197),
        param("ch4_emi", 60),
    ],
)
def test_load_config(config_name, exp_len) -> None:
    result = load_config(config_name)
    df = result.mapping
    # Data have the expected length
    assert exp_len == len(df.index), f"Wrong length of {config_name} mapping table"

    # Valid parameter data: no NaNs anywhere
    assert not df.isna().any(axis=None), f"NaN entries in {config_name} mapping table"

    # TODO Extend assertions


@pytest.mark.xfail(reason="Missing R12 scenario snapshot", raises=ValueError)
def test_run(scenario) -> None:
    run(scenario)
    # TODO Add assertions once test scenario is available
