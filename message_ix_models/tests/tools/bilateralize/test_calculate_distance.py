import math
import os

import pandas as pd
import pytest

from message_ix_models.tools.bilateralize import (
    calculate_distance as calculate_distance_module,
)
from message_ix_models.tools.bilateralize.calculate_distance import (
    calculate_distance,
    calculate_pipeline_distances,
    calculate_port_distances,
    haversine_distance,
)


def test_haversine_distance_same_point() -> None:
    assert haversine_distance(0, 0, 0, 0) == pytest.approx(0)


def test_haversine_distance_quarter_circumference() -> None:
    # From (0, 0) to (0, 90) is a quarter of the Earth's circumference
    expected = (math.pi / 2) * 6371
    assert haversine_distance(0, 0, 0, 90) == pytest.approx(expected, rel=1e-6)


def test_calculate_port_distances() -> None:
    df = pd.DataFrame(
        {
            "Port": ["Rotterdam", "New York"],
            "Latitude": [51.9581, 40.6892],
            "Longitude": [4.08, -74.0445],
        }
    )
    result = calculate_port_distances(df)

    assert set(result.columns) == {"Port1", "Port2", "Distance_km"}
    assert len(result) == 2  # both directions included

    d1 = result.loc[
        (result["Port1"] == "Rotterdam") & (result["Port2"] == "New York"),
        "Distance_km",
    ].iloc[0]
    d2 = result.loc[
        (result["Port1"] == "New York") & (result["Port2"] == "Rotterdam"),
        "Distance_km",
    ].iloc[0]
    assert d1 == d2
    assert d1 > 0


def test_calculate_port_distances_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        calculate_port_distances(pd.DataFrame({"Port": ["A"]}))


def test_calculate_pipeline_distances() -> None:
    df = calculate_pipeline_distances(regional_specification="R12")

    assert list(df.columns) == ["Node1", "Node2", "Distance_km"]
    assert not df.empty
    assert (df["Distance_km"] > 0).all()


def test_calculate_pipeline_distances_missing_columns(monkeypatch) -> None:
    monkeypatch.setattr(
        pd,
        "read_excel",
        lambda *args, **kwargs: pd.DataFrame(
            {"Regionalization": ["R12"], "Port": ["A"]}
        ),
    )
    with pytest.raises(ValueError, match="Missing required columns"):
        calculate_pipeline_distances(regional_specification="R12")


def test_calculate_pipeline_distances_no_valid_coordinates() -> None:
    # No pipeline nodes exist for this (bogus) regional specification
    with pytest.raises(ValueError, match="No valid coordinate data found"):
        calculate_pipeline_distances(regional_specification="NOPE")


def test_calculate_distance(monkeypatch) -> None:
    # Avoid the (slow) real shortest-path routing
    monkeypatch.setattr(
        calculate_distance_module,
        "calculate_port_distances",
        lambda df: pd.DataFrame(
            {"Port1": ["A"], "Port2": ["B"], "Distance_km": [123.4]}
        ),
    )
    # Avoid overwriting real files under data/bilateralize/distances/
    written: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(
        pd.DataFrame,
        "to_csv",
        lambda self, path, **kw: written.__setitem__(os.path.basename(path), self),
    )

    calculate_distance(regional_specification="R12", commodity_list=["base"])

    assert "R12_base_distances.csv" in written
    assert list(written["R12_base_distances.csv"].columns) == [
        "Node1",
        "Port1",
        "Node2",
        "Port2",
        "Distance_km",
    ]
