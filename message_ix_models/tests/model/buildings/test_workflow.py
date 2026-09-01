from typing import cast

import pandas as pd
import pytest
import yaml
from message_ix import Scenario

from message_ix_models import Context
from message_ix_models.model.buildings.workflow import ngfs


class MockScenario:
    """A mock Scenario that returns certain PRICE_COMMODITY variable data.

    .. todo:: Unify with :class:`.report.sim.MockScenario` and move upstream to
       :mod:`message_ix`.
    """

    def var(self, name: str, filters) -> pd.DataFrame:
        assert name == "PRICE_COMMODITY"
        return pd.DataFrame(
            {
                "node": ["R12_AFR", "R12_AFR", "R12_CHN"],
                "commodity": ["gas", "electr", "lightoil"],
                "lvl": [5.0, 15.0, 8.0],
            }
        ).assign(level="final", year=2020, time="year")


@pytest.mark.ci_not_macos_intel
def test_ngfs(capfd: pytest.CaptureFixture, mock_buildings_context: Context) -> None:
    """call_sturm merges scenario prices and invokes STURM R scripts."""
    ctx = mock_buildings_context

    # Path to STURM code in the mock installation
    sturm_dir = ctx.buildings.code_dir.joinpath("message_ix_buildings", "sturm")

    scenario = cast(Scenario, MockScenario())

    # Function runs without error
    result = ngfs(ctx, scenario)

    # Returns the same Scenario object passed
    assert result is scenario

    # R scripts from the mock_buildings_context fixture were executed and produced
    # output
    captured = capfd.readouterr()
    assert "Executed run_MIXB_aligner.R" in captured.out
    print(captured.err)

    # Price data as input to STURM was written to file
    price_input = sturm_dir / "data" / "input_prices_R12.csv"
    updated = pd.read_csv(price_input)

    # Configuration needed by STURM was written to file
    with open(sturm_dir / "scenario_config.yaml", encoding="utf-8") as f:
        assert {"scenarios": ["R"]} == yaml.safe_load(f)

    # A particular value was limited to the reference value (10.0, from
    # mock_buildings_context fixture) because the scenario value (5.0, from
    # MockScenario) was too low
    gas_row = updated.query("commodity == 'gas'").iloc[0]
    assert gas_row["lvl"] == 10.0

    # For commodity="electr", the scenario value was used despite being lower than the
    # reference value
    electr_row = updated.query("commodity == 'electr'").iloc[0]
    assert electr_row["lvl"] == 15.0


def test_ngfs_missing_reference_prices(mock_buildings_context: Context) -> None:
    """:func:`ngfs` (actually :func:`get_prices`) raises on missing reference file."""
    ctx = mock_buildings_context

    # Delete the file with reference price data
    sturm_dir = ctx.buildings.sturm_code_dir
    (sturm_dir / "data" / "input_prices_R12_default.csv").unlink()

    scenario = cast(Scenario, MockScenario())

    with pytest.raises(FileNotFoundError, match="input_prices_R12_default"):
        ngfs(ctx, scenario)
