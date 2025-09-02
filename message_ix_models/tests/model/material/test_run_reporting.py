import pandas as pd
import pyam
import pytest
from message_ix import Reporter, Scenario
from pytest import param

from message_ix_models import Context
from message_ix_models.model.material.report.run_reporting import (
    load_config,
    run,
    run_ch4_reporting,
    run_fe_methanol_nh3_reporting,
    run_fe_reporting,
    run_fs_reporting,
    run_prod_reporting,
    split_fe_other,
)
from message_ix_models.testing import bare_res

MARK = pytest.mark.xfail(reason="Fixture scenario does not have materials contents")


@pytest.fixture
def reporter(scenario) -> Reporter:
    return Reporter.from_scenario(scenario)


@pytest.fixture
def scenario(request: pytest.FixtureRequest, test_context: Context) -> Scenario:
    test_context.model.regions = "R12"
    return bare_res(request, test_context, solved=False)


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


@MARK
def test_run(scenario: Scenario) -> None:
    run(scenario)
    # TODO Add assertions once test scenario is available


@pytest.mark.parametrize(
    "func",
    [
        pytest.param(run_ch4_reporting, marks=MARK),
        pytest.param(run_fe_reporting, marks=MARK),
        run_fe_methanol_nh3_reporting,
        pytest.param(run_fs_reporting, marks=MARK),
        pytest.param(run_prod_reporting, marks=MARK),
    ],
)
def test_other(reporter: Reporter, func) -> None:
    """Placeholder tests for several functions with identical signatures."""
    result = func(reporter, "model name", "scenario name")

    # TODO Add assertions
    del result


@MARK
def test_split_fe_other(reporter: Reporter) -> None:
    split_fe_other(
        reporter,
        # NB IamDataFrame() can't be initialized empty or with an empty df; this is the
        #    shortest call that works
        pyam.IamDataFrame(
            pd.DataFrame(
                [["m", "s", "r", "v", "u", 0]],
                columns=["model", "scenario", "region", "variable", "unit", "year"],
            ).assign(value=1.0)
        ),
        "model name",
        "scenario name",
    )
