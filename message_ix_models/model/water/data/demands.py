"""Prepare data for adding demands"""

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






def get_basin_sizes(
    basin: pd.DataFrame, node: str
) -> Sequence[Union[pd.Series, Literal[0]]]:
    """Returns the sizes of developing and developed basins for a given node"""
    temp = basin[basin["BCU_name"] == node]
    print(temp)
    sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")
    print(sizes)
    # sizes_### = sizes["###"] if "###" in sizes.index else 0
    sizes_dev = sizes["DEV"] if "DEV" in sizes.index else 0
    sizes_ind = sizes["IND"] if "IND" in sizes.index else 0
    return_tuple: tuple[Union[pd.Series, Literal[0]], Union[pd.Series, Literal[0]]] = (
        sizes_dev,
        sizes_ind,
    )  # type: ignore # Somehow, mypy is unable to recognize the proper type without forcing it
    return return_tuple


def set_target_rate(df: pd.DataFrame, node: str, year: int, target: float) -> None:
    """Sets the target value for a given node and year"""
    indices = df[df["node"] == node][df[df["node"] == node]["year"] == year].index
    for index in indices:
        if (
            df[df["node"] == node][df[df["node"] == node]["year"] == year].at[
                index, "value"
            ]
            < target
        ):
            df.at[index, "value"] = target


def set_target_rate_developed(df: pd.DataFrame, node: str, target: float) -> None:
    """Sets target rate for a developed basin"""
    set_target_rate(df, node, 2030, target)


def set_target_rate_developing(df: pd.DataFrame, node: str, target: float) -> None:
    """Sets target rate for a developing basin"""
    for i in df.index:
        if df.at[i, "node"] == node and df.at[i, "year"] == 2030:
            value_2030 = df.at[i, "value"]
            break

    set_target_rate(
        df,
        node,
        2035,
        (value_2030 + target) / 2,
    )
    set_target_rate(df, node, 2040, target)


def set_target_rates(df: pd.DataFrame, basin: pd.DataFrame, val: float) -> None:
    """Sets target rates for all nodes in a given basin"""
    for node in df.node.unique():
        dev_size, ind_size = get_basin_sizes(basin, node)
        if dev_size >= ind_size:
            set_target_rate_developed(df, node, val)
        else:
            set_target_rate_developing(df, node, val)


def target_rate(df: pd.DataFrame, basin: pd.DataFrame, val: float) -> pd.DataFrame:
    """
    Sets target connection and sanitation rates for SDG scenario.
    The function filters out the basins as developing and
    developed based on the countries overlapping basins.
    If the number of developing countries in the basins are
    more than basin is categorized as developing and vice versa.
    If the number of developing and developed countries are equal
    in a basin, then the basin is assumed developing.
    For developed basins, target is set at 2030.
    For developing basins, the access target is set at
    2040 and 2035 target is the average of
    2030 original rate and 2040 target.

    Returns
    -------
        df (pandas.DataFrame): Data frame with updated value column.
    """
    set_target_rates(df, basin, val)
    return df


def target_rate_trt(df: pd.DataFrame, basin: pd.DataFrame) -> pd.DataFrame:
    """
    Sets target treatment rates for SDG scenario. The target value for
    developed and developing region is making sure that the amount of untreated
    wastewater is halved beyond 2030 & 2040 respectively.

    Returns
    -------
    data : pandas.DataFrame
    """

    value = []
    for i in df.node.unique():
        temp = basin[basin["BCU_name"] == i]

        sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")

        if len(sizes) > 1:
            if sizes["DEV"] > sizes["IND"] or sizes["DEV"] == sizes["IND"]:
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])
            else:
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])
        else:
            if sizes.index[0] == "DEV":
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])
            else:
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])

    valuetest = pd.DataFrame(data=value, columns=["Index", "Value"])

    for i in range(len(valuetest["Index"])):
        df.at[valuetest["Index"][i], "Value"] = valuetest["Value"][i]

    real_value = df["Value"].combine_first(df["value"])

    df.drop(["value", "Value"], axis=1, inplace=True)

    df["value"] = real_value
    return df




