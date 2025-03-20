"""prepare data for adding demands

this file sets targets for connection, sanitation and treatment based on basin info
"""

import os
import builtins
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Union

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df

from message_ix_models.util import broadcast, minimum_version, package_data_path

if TYPE_CHECKING:
    from message_ix_models import Context

builtins.Sequence = Sequence
builtins.Union = Union
builtins.Literal = Literal
builtins.TYPE_CHECKING = TYPE_CHECKING
builtins.np = np
builtins.pd = pd
builtins.xr = xr
builtins.make_df = make_df
builtins.broadcast = broadcast
builtins.minimum_version = minimum_version
builtins.package_data_path = package_data_path
builtins.os = os
if TYPE_CHECKING:
    builtins.Context = Context

from message_ix_models.model.water.data.demands_pt2 import *
from message_ix_models.model.water.data.demands_pt3 import *


def get_basin_sizes(basin: pd.DataFrame, node: str) -> tuple[int, int]:
    """return dev and ind sizes for a node"""
    temp = basin[basin["BCU_name"] == node]
    sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")
    sizes_dev = sizes.get("DEV", 0)
    sizes_ind = sizes.get("IND", 0)
    return sizes_dev, sizes_ind


def target_rate(df: pd.DataFrame, basin: pd.DataFrame, target: Union[float, str]) -> pd.DataFrame:
    """
    adjust rate values according to target.

    if target is 'treatment', then for developed basins (dev >= ind) a threshold of 2040 is used,
    otherwise 2030; values are increased by half the gap toward 1.
    
    if target is a float (for connection/sanitation), then in developed basins the rate is set at 2030,
    while in developing basins the 2035 rate is set as the average of the 2030 rate and target, and
    the 2040 value is set to target.
    """
    for node in df["node"].unique():
        sizes_dev, sizes_ind = get_basin_sizes(basin, node)
        is_developed = sizes_dev >= sizes_ind
        
        match target:
            case "treatment":
                # determine threshold based on basin type
                match is_developed:
                    case True:
                        threshold = 2040
                    case False:
                        threshold = 2030
                cond = (df["node"] == node) & (df["year"] >= threshold)
                df.loc[cond, "value"] = df.loc[cond, "value"] + (1 - df.loc[cond, "value"]) / 2

            case float() as t:
                # adjust connection/sanitation targets based on basin type
                match is_developed:
                    case True:
                        cond = (df["node"] == node) & (df["year"] == 2030)
                        df.loc[cond, "value"] = df.loc[cond, "value"].where(df.loc[cond, "value"] >= t, t)
                    case False:
                        cond_2030 = (df["node"] == node) & (df["year"] == 2030)
                        rate_2030 = df.loc[cond_2030, "value"].iloc[0] if not df.loc[cond_2030, "value"].empty else 0
                        cond_2035 = (df["node"] == node) & (df["year"] == 2035)
                        cond_2040 = (df["node"] == node) & (df["year"] == 2040)
                        target_2035 = (rate_2030 + t) / 2
                        df.loc[cond_2035, "value"] = df.loc[cond_2035, "value"].where(df.loc[cond_2035, "value"] >= target_2035, target_2035)
                        df.loc[cond_2040, "value"] = df.loc[cond_2040, "value"].where(df.loc[cond_2040, "value"] >= t, t)
            case _:
                raise ValueError(f"unrecognized target parameter: {target}")
    return df