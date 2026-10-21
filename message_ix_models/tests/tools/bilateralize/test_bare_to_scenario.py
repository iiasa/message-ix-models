import pandas as pd
import pytest

from message_ix_models.tools.bilateralize import (
    bare_to_scenario as bare_to_scenario_module,
)
from message_ix_models.tools.bilateralize.bare_to_scenario import (
    bare_to_scenario,
    build_parameter_sheets,
    calibrate_historical_shipping,
)
from message_ix_models.tools.bilateralize.utils import get_logger, load_config

MARK = pytest.mark.xfail(
    raises=FileNotFoundError,
    reason="Input data files not available for testing.",
)


@MARK  # P:/ene.model/MESSAGE_trade/IMO/GISIS/Crude Tankers.csv
def test_calibrate_historical_shipping(
    project_name: str | None = None, config_name: str | None = None
) -> None:
    config, config_path, tec_config = load_config(
        project_name=project_name, config_name=config_name, load_tec_config=True
    )

    covered_tec = config["covered_trade_technologies"]

    # Get logger
    log = get_logger(__name__)

    # Read and inflate sheets based on model horizon
    trade_dict = build_parameter_sheets(
        log=log, project_name=project_name, config_name=config_name
    )
    calibrate_historical_shipping(
        config=config,
        trade_dict=trade_dict,
        covered_tec=covered_tec,
        project_name=project_name,
        config_name=config_name,
    )
    assert True


def test_calibrate_historical_shipping_skips_uncovered_tec(monkeypatch) -> None:
    """Shipping technologies not in `covered_tec` are skipped, not KeyError'd.

    Regression test for the `covered_tec` guard added to
    :func:`calibrate_historical_shipping`: it used to unconditionally write into
    `trade_dict` for every key of `nc_dict` (all shipping technologies), which raised
    ``KeyError`` whenever a project's config did not cover one of them.
    """

    def fake_build_hist_new_capacity_flow(
        infile, ship_type, project_name=None, config_name=None
    ):
        return pd.DataFrame({"technology": [ship_type], "value": [10.0]})

    monkeypatch.setattr(
        bare_to_scenario_module,
        "build_hist_new_capacity_flow",
        fake_build_hist_new_capacity_flow,
    )

    config = {
        "shipping_fuels": {
            "LNG_tanker": {"LNG_tanker_loil": 1.0, "LNG_tanker_LNG": 1.0},
            "oil_tanker": {
                "oil_tanker_loil": 1.0,
                "oil_tanker_foil": 1.0,
                "oil_tanker_eth": 1.0,
            },
        }
    }

    # Only one of the six shipping technologies in nc_dict is covered
    covered_tec = ["crudeoil_shipped"]
    trade_dict = {
        "crudeoil_shipped": {
            "flow": {},
            "trade": {"input": pd.DataFrame({"technology": ["crudeoil_tanker_loil"]})},
        }
    }

    result = calibrate_historical_shipping(
        config=config, trade_dict=trade_dict, covered_tec=covered_tec
    )

    # Covered technology received its historical new capacity data
    hnc = result["crudeoil_shipped"]["flow"]["historical_new_capacity"]
    assert list(hnc["technology"]) == ["crudeoil_tanker_loil"]

    # Uncovered technologies (e.g. "lh2_shipped") were skipped, not added
    assert "lh2_shipped" not in result


def test_bare_to_scenario_uses_bare_trade_technology_name(
    monkeypatch, tmp_path
) -> None:
    """`hist_tec` uses the bare `trade_technology` name, without an `_exp` suffix.

    Regression test: `bare_to_scenario` used to append `"_exp"` to the configured
    `trade_technology` name before using it as a `str.contains` filter, which meant it
    never matched real historical-activity technology names (e.g. `..._imp`).
    """
    tec = "crudeoil_shipped"
    config = {
        "covered_trade_technologies": [tec],
        "scenario": {"regions": "R12"},
    }
    tec_config = {tec: {f"{tec}_trade": {"trade_technology": "crudeoil_tanker"}}}
    config_path = str(tmp_path / "config.yaml")

    histdf = pd.DataFrame(
        {"technology": ["crudeoil_tanker_imp"], "year_act": [2020], "value": [5.0]}
    )
    empty_histnc = pd.DataFrame({"technology": pd.Series(dtype="object"), "value": []})

    monkeypatch.setattr(
        bare_to_scenario_module,
        "load_config",
        lambda **kwargs: (config, config_path, tec_config),
    )
    monkeypatch.setattr(
        bare_to_scenario_module,
        "build_parameter_sheets",
        lambda **kwargs: {tec: {"flow": {}, "trade": {}}},
    )
    monkeypatch.setattr(
        bare_to_scenario_module,
        "build_historical_activity",
        lambda **kwargs: histdf.copy(),
    )
    monkeypatch.setattr(
        bare_to_scenario_module,
        "build_hist_new_capacity_trade",
        lambda **kwargs: empty_histnc.copy(),
    )
    monkeypatch.setattr(
        bare_to_scenario_module,
        "calibrate_historical_shipping",
        lambda **kwargs: kwargs["trade_dict"],
    )
    monkeypatch.chdir(tmp_path)

    trade_dict = bare_to_scenario(p_drive_access=True)

    hist_activity = trade_dict[tec]["trade"]["historical_activity"]
    assert not hist_activity.empty
    assert list(hist_activity["technology"]) == ["crudeoil_tanker_imp"]
