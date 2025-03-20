import os
from pathlib import Path
import pandas as pd
import pytest

from message_ix_models.model.water.data import demands_pt3 as demands_pt3_old
from message_ix_models.model.water.data import demands_pt3 as demands_pt3_new

# dummy context with minimal attributes
class DummyContext(dict):
    def __init__(self, regions, time, SDG, water_build_info):
        super().__init__()
        self.regions = regions
        self.time = time
        self.SDG = SDG
        self["water build info"] = water_build_info

class DummyWaterBuildInfo:
    def __init__(self, Y):
        self.Y = Y

@pytest.fixture
def dummy_context():
    # using the real csv file; ensure the region corresponds to a real data file
    build_info = DummyWaterBuildInfo(Y=[2010, 2015, 2020, 2030, 2040])
    ctx = DummyContext(regions="R12", time="year", SDG="sdg", water_build_info=build_info)
    return ctx

def sort_and_reset(df: pd.DataFrame) -> pd.DataFrame:
    # sort and reset for robust comparisons
    return df.sort_index(axis=1).reset_index(drop=True)

def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame):
    pd.testing.assert_frame_equal(sort_and_reset(df1), sort_and_reset(df2), check_dtype=False)

def test_parity(dummy_context):
    # run original implementation
    out_old = demands_pt3_old.add_sectoral_demands(dummy_context)
    # run new dsl-based implementation
    out_new = demands_pt3_new.add_sectoral_demands(dummy_context)
    # ensure both have same keys
    assert set(out_old.keys()) == set(out_new.keys())
    # compare corresponding dataframes
    for key in out_old:
        compare_dataframes(out_old[key], out_new[key])