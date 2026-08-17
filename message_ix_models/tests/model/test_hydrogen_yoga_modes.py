import pandas as pd
import pytest

from message_ix_models.model.hydrogen.yoga_modes import (
    ORIGINAL_MODES,
    SPLIT_MODES,
    SPLIT_SUFFIXES,
    _history_totals,
    _restored_history_rows,
    _verify_history_restored,
)
from message_ix_models.util import package_data_path


def _broadcast_history(rows: pd.DataFrame) -> pd.DataFrame:
    original = rows[rows["mode"].isin(ORIGINAL_MODES)]
    if original.empty:
        m1 = rows[rows["mode"] == "M1"]
        split = []
        for mode in SPLIT_MODES:
            part = m1.copy()
            part["mode"] = mode
            split.append(part)
        return pd.concat([rows, *split], ignore_index=True)

    untouched = rows[~rows["mode"].isin(ORIGINAL_MODES)]
    split = []
    for mode in ORIGINAL_MODES:
        source = rows[rows["mode"] == mode]
        for suffix in SPLIT_SUFFIXES:
            part = source.copy()
            part["mode"] = f"{mode}_{suffix}"
            split.append(part)
    return pd.concat([untouched, *split], ignore_index=True)


def _apply_restored_rows(
    broadcast: pd.DataFrame, restored: pd.DataFrame
) -> pd.DataFrame:
    split = broadcast[broadcast["mode"].isin(SPLIT_MODES)]
    untouched = broadcast[~broadcast["mode"].isin(SPLIT_MODES)]
    if restored.empty:
        return broadcast

    keys = [column for column in broadcast if column != "value"]
    adjusted = split.set_index(keys)
    adjusted.loc[restored.set_index(keys).index, "value"] = restored.set_index(keys)[
        "value"
    ]
    return pd.concat([untouched, adjusted.reset_index()], ignore_index=True)


@pytest.mark.parametrize(
    "technology, expected_before, expected_broadcast, corrected",
    [
        ("h2_elec_alk", 22.4808762904, 47.4569524165, True),
        ("h2_elec_pem", 21.8641399185, 46.8402160446, True),
        ("h2_elec_soe", 0.04595100725, 0.32165705075, True),
    ],
)
def test_restore_real_electrolyser_history(
    technology: str,
    expected_before: float,
    expected_broadcast: float,
    corrected: bool,
) -> None:
    rows = pd.read_csv(
        package_data_path(
            "hydrogen", "parameters", technology, "historical_activity.csv"
        )
    )
    before = _history_totals(rows)
    broadcast = _broadcast_history(rows)

    assert before["value"].sum() == pytest.approx(expected_before)
    assert broadcast["value"].sum() == pytest.approx(expected_broadcast)

    restored = _restored_history_rows(broadcast, before)
    assert (not restored.empty) is corrected

    result = _apply_restored_rows(broadcast, restored)
    _verify_history_restored(before, result)
    assert result["value"].sum() == pytest.approx(expected_before)


def test_restoration_is_noop_on_clean_history() -> None:
    clean = pd.DataFrame(
        {
            "node_loc": ["R12_AFR"] * 3,
            "technology": ["h2_elec_alk"] * 3,
            "year_act": [2020] * 3,
            "mode": ["M1", "feedstock_bic", "fuel_fic"],
            "time": ["year"] * 3,
            "value": [2.0, 3.0, 5.0],
            "unit": ["GWa"] * 3,
        }
    )
    before = _history_totals(clean)

    restored = _restored_history_rows(clean, before)

    assert restored.empty
    _verify_history_restored(before, clean)


def test_verifier_rejects_inflated_history() -> None:
    source = pd.DataFrame(
        {
            "node_loc": ["R12_AFR"] * 3,
            "technology": ["h2_elec_alk"] * 3,
            "year_act": [2020] * 3,
            "mode": ["M1", "feedstock", "fuel"],
            "time": ["year"] * 3,
            "value": [2.0, 3.0, 5.0],
            "unit": ["GWa"] * 3,
        }
    )
    before = _history_totals(source)
    inflated = _broadcast_history(source)

    with pytest.raises(RuntimeError, match="worst delta 16"):
        _verify_history_restored(before, inflated)


def test_history_totals_reject_null_values() -> None:
    rows = pd.DataFrame(
        {
            "node_loc": ["R12_AFR"],
            "technology": ["h2_elec_alk"],
            "year_act": [2020],
            "mode": ["M1"],
            "time": ["year"],
            "value": [None],
        }
    )

    with pytest.raises(ValueError, match="contains null"):
        _history_totals(rows)
