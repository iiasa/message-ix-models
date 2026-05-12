"""Tests for BMT STURM helpers in :mod:`message_ix_models.model.buildings.sturm`.

Covers :func:`_message_buildings_install_dir`, :func:`call_sturm`, and
:func:`call_buildings_demand`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pandas as pd
import pytest
from message_ix import make_df

from message_ix_models.model.buildings import sturm
from message_ix_models.testing import bare_res

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context


STURM_YEAR = 2030
STURM_TIME = "year"
STURM_UNIT = "GWa"
PRICE_UNIT = "USD/kWa"
STURM_PRICE_NODES = ["R11_AFR", "R11_CHN"]
STURM_PRICE_COMMODITIES = ["gas", "electr"]
STURM_SUBPROCESS = "message_ix_models.model.buildings.sturm.subprocess.run"
STURM_INSTALL_DIR = (
    "message_ix_models.model.buildings.sturm._message_buildings_install_dir"
)


def _sturm_price_input(values: list[float] | None = None) -> pd.DataFrame:
    if values is None:
        values = [10.0, 20.0]
    return make_df(
        "price_commodity",
        node=STURM_PRICE_NODES,
        commodity=STURM_PRICE_COMMODITIES,
        level="final",
        year=STURM_YEAR,
        time=STURM_TIME,
        unit=PRICE_UNIT,
        value=values,
    ).rename(columns={"value": "lvl"})


def _scenario_price_par(
    node: str | list[str],
    commodity: str | list[str],
    value: float | list[float],
) -> pd.DataFrame:
    return make_df(
        "price_commodity",
        node=node,
        commodity=commodity,
        level="final",
        year=STURM_YEAR,
        time=STURM_TIME,
        unit=PRICE_UNIT,
        value=value,
    )


def _scenario_price_var(
    node: str | list[str],
    commodity: str | list[str],
    value: float | list[float],
) -> pd.DataFrame:
    return _scenario_price_par(node, commodity, value).rename(columns={"value": "lvl"})


def _sturm_demand(
    commodity: str | list[str],
    value: float | list[float],
    *,
    node: str | list[str] = "R12_AFR",
    level: str = "useful",
) -> pd.DataFrame:
    return make_df(
        "demand",
        node=node,
        commodity=commodity,
        level=level,
        year=STURM_YEAR,
        time=STURM_TIME,
        unit=STURM_UNIT,
        value=value,
    )


def _write_sturm_price_input(
    path: Path, values: list[float] | None = None
) -> pd.DataFrame:
    frame = _sturm_price_input(values)
    frame.to_csv(path, index=False)
    return frame


def _assert_sturm_price_file(path: Path, expected: pd.DataFrame) -> None:
    columns = list(expected.columns)
    actual = pd.read_csv(path)[columns].sort_values(columns).reset_index(drop=True)
    pd.testing.assert_frame_equal(
        actual,
        expected[columns].sort_values(columns).reset_index(drop=True),
        check_dtype=False,
    )


def _assert_demand_matches(
    actual: pd.DataFrame, expected: pd.DataFrame, *, columns: list[str] | None = None
) -> None:
    columns = columns or list(make_df("demand").columns)
    pd.testing.assert_frame_equal(
        actual.loc[:, columns].sort_values(columns).reset_index(drop=True),
        expected.loc[:, columns].sort_values(columns).reset_index(drop=True),
        check_dtype=False,
    )


def _sturm_layout(tmp_path: Path) -> tuple[Path, Path, Path]:
    buildings_root = tmp_path / "buildings"
    sturm_dir = buildings_root / "message_ix_buildings" / "sturm"
    price_dir = sturm_dir / "data"
    temp_dir = sturm_dir / "temp"
    price_dir.mkdir(parents=True)
    temp_dir.mkdir(parents=True)
    for name in ("run_STURM_bmt_resid.R", "run_STURM_bmt_comm.R"):
        (sturm_dir / name).write_text("# stub\n")
    return buildings_root, sturm_dir, price_dir


def test_message_buildings_install_dir_raises_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "message_ix_models.model.buildings.sturm.ixmp.config.get",
        MagicMock(side_effect=KeyError("missing")),
    )

    with pytest.raises(ValueError, match="message_buildings_dir"):
        sturm._message_buildings_install_dir()


def test_message_buildings_install_dir_reads_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    buildings_root = tmp_path / "message-buildings"
    buildings_root.mkdir()

    def _get(key: str) -> str:
        if key == "message_buildings_dir":
            return str(buildings_root)
        raise KeyError(key)

    monkeypatch.setattr("message_ix_models.model.buildings.sturm.ixmp.config.get", _get)

    assert sturm._message_buildings_install_dir() == buildings_root.resolve()


def test_call_sturm_raises_when_price_input_missing(
    test_context: Context, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    buildings_root, _, _ = _sturm_layout(tmp_path)
    monkeypatch.setattr(STURM_INSTALL_DIR, lambda: buildings_root)

    with pytest.raises(FileNotFoundError, match="input_prices_R12.csv"):
        sturm.call_sturm(test_context, MagicMock())


def test_call_sturm_merges_prices_and_runs_rscripts(
    request: pytest.FixtureRequest,
    test_context: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buildings_root, _, price_dir = _sturm_layout(tmp_path)
    price_input = price_dir / "input_prices_R12.csv"
    _write_sturm_price_input(price_input)

    monkeypatch.setattr(STURM_INSTALL_DIR, lambda: buildings_root)

    scenario = bare_res(request, test_context)
    # The scenario has to be solved to provide energy commodity prices.
    scenario_prices = _scenario_price_var(
        ["R12_AFR", "R12_CHN"],
        ["gas", "electr"],
        [15.0, 5.0],
    )
    original_var = scenario.var

    def fake_var(name, filters=None):
        if name != "PRICE_COMMODITY":
            return original_var(name, filters=filters)
        df = scenario_prices.copy()
        if filters:
            for key, values in filters.items():
                if key in df.columns:
                    df = df[df[key].isin(values)]
        return df

    monkeypatch.setattr(scenario, "var", fake_var)

    run_calls: list[list[str]] = []

    def _fake_run(command, cwd=None, check=None):  # noqa: ARG001
        run_calls.append(command)

    monkeypatch.setattr(STURM_SUBPROCESS, _fake_run)

    result = sturm.call_sturm(test_context, scenario)

    assert result is scenario
    assert (price_dir / "input_prices_R12_ori.csv").is_file()
    _assert_sturm_price_file(price_input, _sturm_price_input([15.0, 20.0]))
    assert run_calls == [
        ["Rscript", "run_STURM_bmt_resid.R"],
        ["Rscript", "run_STURM_bmt_comm.R"],
    ]


# TODO: Discuss with Alessio whether flooring initial prices is desirable.
# def test_call_sturm_keeps_original_when_scenario_price_is_lower(
#     test_context: Context, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
# ) -> None:
#     buildings_root, _, price_dir = _sturm_layout(tmp_path)
#     price_input = price_dir / "input_prices_R12.csv"
#     original = _write_sturm_price_input(price_input)
#
#     monkeypatch.setattr(STURM_INSTALL_DIR, lambda: buildings_root)
#     monkeypatch.setattr(STURM_SUBPROCESS, MagicMock())
#
#     scenario = MagicMock()
#     scenario.var.return_value = _scenario_price_var("R12_AFR", "gas", 4.0)
#
#     sturm.call_sturm(test_context, scenario)
#
#     _assert_sturm_price_file(price_input, original)


def test_call_buildings_demand_filters_and_adds_useful_demand(
    request: pytest.FixtureRequest,
    test_context: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buildings_root, _, _ = _sturm_layout(tmp_path)
    temp_dir = buildings_root / "message_ix_buildings" / "sturm" / "temp"

    monkeypatch.setattr(STURM_INSTALL_DIR, lambda: buildings_root)

    scenario: Scenario = bare_res(request, test_context)
    node = scenario.set("node")[0]
    _sturm_demand(
        ["resid_heat_electr", "resid_heat_mat_steel", "comm_heat_electr"],
        [1.0, 2.0, 0.5],
        node=node,
    ).to_csv(temp_dir / "resid_sturm.csv", index=False)
    _sturm_demand("comm_heat_gas", 0.25, node=node).to_csv(
        temp_dir / "comm_sturm.csv",
        index=False,
    )

    sturm.call_buildings_demand(test_context, scenario)

    expected = _sturm_demand(
        ["resid_heat_electr", "comm_heat_electr", "comm_heat_gas"],
        [1.0, 0.5, 0.25],
        node=node,
    )
    actual = scenario.par("demand")
    _assert_demand_matches(
        actual.loc[actual["commodity"].isin(expected["commodity"])],
        expected,
    )


def test_call_buildings_demand_raises_when_temp_dir_missing(
    test_context: Context, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    buildings_root = tmp_path / "buildings"
    buildings_root.mkdir()

    monkeypatch.setattr(STURM_INSTALL_DIR, lambda: buildings_root)

    with pytest.raises(FileNotFoundError, match="Buildings demand directory not found"):
        sturm.call_buildings_demand(test_context, MagicMock())
