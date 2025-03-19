import os
from pathlib import Path

import pandas as pd
import pytest

from message_ix_models.model.water.data import demands_pt3
from message_ix_models.model.water.data import demands_pt3_refactor_DSL

# dummy context class for minimal attributes
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
def temp_data_dir(tmp_path):
    # create a temporary folder to hold dummy csv files
    # expected folder structure is: water/demands/harmonized/dummy_region
    data_dir = tmp_path / "water" / "demands" / "harmonized" / "dummy_region"
    data_dir.mkdir(parents=True, exist_ok=True)
    # create a dummy csv file with the expected structure
    df = pd.DataFrame({
        "Unnamed: 0": [2020, 2030, 2040],
        "dummy": [100, 200, 300]
    })
    # file name follows the naming scheme used in the code
    csv_file = data_dir / "ssp2_regional_dummy.csv"
    df.to_csv(csv_file, index=False)
    # return the base directory for package_data_path to point to the harmonized folder
    return tmp_path / "water" / "demands" / "harmonized"

@pytest.fixture
def dummy_context(temp_data_dir):
    # create dummy water build info with expected years
    build_info = DummyWaterBuildInfo(Y=[2020, 2030, 2040])
    # create dummy context; note that 'time' is set to "year"
    ctx = DummyContext(regions="dummy_region", time="year", SDG="baseline", water_build_info=build_info)
    
    # define a dummy package_data_path that returns our temporary csv folder
    def dummy_package_data_path(*args, **kwargs):
        # expected signature: package_data_path("water", "demands", "harmonized", region, additional)
        # return the directory for the given region using our dummy structure
        return temp_data_dir / ctx.regions

    # patch package_data_path in both legacy and refactored modules
    demands_pt3.package_data_path = dummy_package_data_path
    demands_pt3_refactor_DSL.package_data_path = dummy_package_data_path
    return ctx

def sort_and_reset(df: pd.DataFrame) -> pd.DataFrame:
    # sort columns and reset index for robust comparison
    return df.sort_index(axis=1).reset_index(drop=True)

def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame):
    # compare two dataframes after sorting; use tolerance in case there are minor differences
    pd.testing.assert_frame_equal(sort_and_reset(df1), sort_and_reset(df2), check_dtype=False)

def test_parity(dummy_context):
    # run original implementation
    out_old = demands_pt3.add_sectoral_demands(dummy_context)
    # run the refactored implementation
    out_new = demands_pt3_refactor_DSL.add_sectoral_demands(dummy_context)

    # check that both outputs contain the same keys
    assert set(out_old.keys()) == set(out_new.keys())

    # compare each dataframe result
    for key in out_old:
        df_old = out_old[key]
        df_new = out_new[key]
        compare_dataframes(df_old, df_new)