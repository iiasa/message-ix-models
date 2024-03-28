# -*- coding: utf-8 -*-
import glob
import inspect
import itertools
import os
import sys
from functools import cmp_to_key

import numpy as np
import pandas as pd
from message_ix_models.util import private_data_path
from pandas.api.types import is_numeric_dtype

from . import iamc_tree, utilities

all_years = None
years = None
firstmodelyear = None
all_tecs = None
regions = None
region_id = None
verbose = False
globalname = None
model_nm = None
scen_nm = None
unit_conversion = None

#  IAMC index
iamc_idx = ["Model", "Scenario", "Region", "Variable", "Unit"]
index_order = [
    "Model",
    "Region",
    "Technology",
    "Commodity",
    "Unit",
    "Mode",
    "Grade",
    "Vintage",
]


def combineDict(*args):
    result = {}
    for dic in args:
        for key in result.keys() | dic.keys():
            if key in dic:
                if type(dic[key]) is list:
                    result.setdefault(key, []).extend(dic[key])
                else:
                    result.setdefault(key, []).append(dic[key])
    return result


def fil(df, fil, factor, unit_out=None):
    """Uses predefined values from a fil file"""
    inf = os.path.join(
        private_data_path(), "report", "fil_files", "*-{}.fil".format(fil)
    )
    files = glob.glob(inf)
    dfs = []
    for f in files:
        reg = os.path.basename(f).split("-")[0]
        # Ensure that fil-files are only read for regions contained in the scenario.
        if reg in [regions[r] for r in regions.keys()]:
            dftmp = pd.read_csv(f)
            dftmp["Region"] = reg
            dfs.append(dftmp)
    df_fil = pd.concat(dfs, sort=True)
    df_fil.Region = df_fil.Region.map(
        {item[0].replace(f"{region_id}_", ""): item[1] for item in regions.items()}
    )
    df_fil = df_fil.set_index(["Region"])
    df_fil = df_fil[df_fil.Variable == factor].drop("Variable", axis=1)
    for cols in numcols(df_fil):
        df_fil = df_fil.rename(columns={cols: int(cols)})
    # Add missing years to fil file dataframe
    missyrs = [y for y in df.columns if y not in df_fil.columns]
    for y in missyrs:
        df_fil[y] = np.nan
    df_fil = df_fil[sorted(df_fil.columns)]

    # Interpolate missing years
    for reg in df_fil.index:
        df_fil.loc[reg] = df_fil.loc[reg].interpolate(method="index")

    df = df_fil.fillna(0) * df.fillna(0)
    df["Unit"] = "???"
    if unit_out:
        for col in numcols(df):
            df[col] = df.apply(
                lambda row: row[col] * unit_conversion[row.Unit][unit_out], axis=1
            )
    df = df.drop("Unit", axis=1)
    return df.sort_index()


def ppgroup(df, value=0, group=["Region"]):
    """Groupby function for dataframe

    Parameters
    ----------
    df : dataframe
    value : numeric value
    group : list
        List of column headers by which the dataframe should be re-grouped

    Returns
    -------
    df : dataframe
    """
    df = df.fillna(value).reset_index()
    if "Vintage" in df.columns:
        df["Vintage"] = df["Vintage"].astype("object")
    df = df.groupby(group).sum(numeric_only=True)
    return df.sort_index()


def aggr_glb(df):
    """Overwrites all regional values with the global value

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    df : dataframe
    """
    for reg in regions:
        df.loc[regions[reg]] = df.loc["World"].values
    return df.sort_index()


def rem_glb(df):
    """Removes global region

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    df : dataframe
    """
    df = df.loc[[regions[reg] for reg in regions if regions[reg] != "World"]]
    return df.sort_index()


def aggr_reg(df):
    """Overwrites all regional values with the sum over regions

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    df : dataframe
    """
    df_tmp = df.copy().reset_index()
    vals = df_tmp[df_tmp.Region != "World"].set_index(["Region"]).sum().values
    for reg in regions:
        df.loc[regions[reg]] = vals
    return df.sort_index()


def rem_reg(df):
    """Removes all regions except WORLD

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    df : dataframe
    """
    df = df.loc[[regions[reg] for reg in regions if regions[reg] == "World"]]
    return df.sort_index()


def sum_reg(df):
    """Overwrites glb values with the sum over regions

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    df : dataframe
    """
    df_tmp = df.copy().reset_index()
    vals = df_tmp[df_tmp.Region != "World"].set_index(["Region"]).sum().values
    df.loc["World"] = vals
    return df


def globiom_glb_priceindex(ds, price, quantity, quanity_variable, y0=2005):
    """Calculate the global price-index.

    The following formulat is used and is based on the approach used in GLOBIOM.

    Price-Index values are recalcualted using the following formula:
    Laspeyres index = sum(r, p1*q0) / sum(r, q0)
    Paasche index = sum(r, p1*q1) / sum(r, q1)
    Fisher index = (laaspreyes * paasche) ** 0.5
    where, p=price, q=quantity produced, r=region

    Parameters
    ----------
    ds : :class:`message_ix.Scenario`
    price : :class:`pandas.DataFrame`
        Regional price information.
    quantity : :class:`pandas.DataFrame`
        Regional quantity information.
    quantity_variable : string
        Name of the `land_output` commodity used to derive quantities.
    y0 : int (default=2005)
        initial indexing year ie. values are 1.
    """

    # Retrive values for reference index year.
    q0 = quantity[y0]

    # Ensure that there are values for the year "y0", otherwise retrive these from
    # the parameter land-output.
    if q0.sum() == 0:
        # Retrieve regional data from parameter
        q0 = ds.par(
            "land_output",
            filters={
                "commodity": quanity_variable,
                "land_scenario": ds.set("land_scenario")[0],
                "year": y0,
            },
        )
        # Re-Format
        q0 = q0.drop(["land_scenario", "commodity", "level", "time", "unit"], axis=1)
        q0 = q0.rename(columns={"node": "Region"})
        q0.Region = q0.Region.map(regions)
        q0 = q0.pivot(index="Region", columns="year", values="value")[y0]

    las = price.multiply(q0, axis=0).sum().divide(q0.sum())

    paas = price.multiply(quantity).sum().divide(quantity.sum()).fillna(0)

    fischer = (las * paas) ** 0.5

    quantity.loc["World"] = fischer

    return quantity


def numcols(df):
    """Retrieves numeric columns of a dataframe.

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    list : list
        column names of numeric columns
    """
    return [x for x in df.columns if is_numeric_dtype(df[x])]


def nonnumcols(df):
    """Retrieves nonnumeric columns of a dataframe.

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    list : list
        column names of numeric columns
    """

    values = [x for x in df.columns if not is_numeric_dtype(df[x])]
    values.sort(key=lambda x: index_order.index(x))
    return values


def diff(l1, l2):
    vals = [x for x in l1 if x not in l2]
    return vals


def _convert_units(df, unit_out):
    """Converts and renames units.

    Parameters
    ----------
    df : dataframe
    unit_out : string
        unit to which output should be converted. the unit in the column "Unit"
        is the conversion input unit. The input and output unit as well as the
        conversion factor needs to be defined in the
        python-variable: unit_conversion

    Returns
    -------
    df : dataframe
    """

    cols = [c for c in numcols(df) if c != "Vintage"]
    if cols:
        try:
            df[cols] = df[cols].multiply(
                df["Unit"].apply(lambda x: unit_conversion[x][unit_out]), axis=0
            )
        except Exception:
            print(
                f"No unit conversion factor found to convert {df.unit.unique()[0]} to {unit_out}"
            )
    df.Unit = unit_out

    return df.sort_index()


def cum_vals(df):
    """Cumulates values over a timeseries and converts annual to period-length
        values.

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    df : dataframe
    """

    col = numcols(df)
    for i in range(len(col)):
        if i == len(col) - 1:
            df.loc[:, col[i]] = df.apply(
                lambda row: (row[col[i - 1]] + (row[col[i]] * (10))), axis=1
            )
        elif i == 0:
            df.loc[:, col[i]] = df.apply(
                lambda row: (row[col[i]] * (col[i + 1] - col[i])), axis=1
            )
        else:
            df.loc[:, col[i]] = df.apply(
                lambda row: (row[col[i - 1]] + (row[col[i]] * (col[i + 1] - col[i]))),
                axis=1,
            )
    return df.sort_index()


def write_xlsx(df, path):
    """Writes final results dataframe to xlsx file.

    Parameters
    ----------
    df : dataframe
    path : string
        path to where the xlsx file should be written

    """

    out_path = path / f"{model_nm}_{scen_nm}.xlsx"
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="data", index=False)


def make_outputdf(vars, units, param="sum", glb=True, weighted_by=None):
    """Data is reformatted to the iamc-template output

    global data is derived bu summing over the 11 Regions + World

    Parameters
    ----------
    vars : dictionary
        output data to be converted into iamc-template format
    units : string
        units of the output
    param : string (optional, default = 'sum')
        methods with which the global value is derived from the regional
        numbers - alternatives include max, mean,
    glb : True/False (optional, default = True)
        if True then global values will be calculated


    Returns
    -------
    df : dataframe
        index : iamc_index
    """

    dfs = []
    # Adds variable names to dataframe and combines all data into a single
    # dataframe
    for var in vars:
        df = vars[var]
        df.loc[:, "Model"] = model_nm
        df.loc[:, "Scenario"] = scen_nm
        df.loc[:, "Variable"] = var
        # Currently, units are inserted manually because of problems with
        # multiplication of dataframes
        df.loc[:, "Unit"] = units
        if glb:
            df = gen_GLB(df, param, weighted_by)
        else:
            df = df.reset_index()
        df = df[iamc_idx + years]
        dfs.append(df)
    return pd.concat(dfs, sort=True)


def gen_GLB(df, param, weighted_by):
    """Aggregates results from individual regions onto global region.

    If values are present for the global regions these will be accounted
    for in the process.

    Parameters
    ----------
    df : dataframe
    param : string (can be either sum/max/mean/weighted_avg)
            -> if weighted_avg is passed then the values indexed over
               the region must be passed

    Returns
    -------
    df : dataframe
    """

    df = df.fillna(value=0).reset_index()
    idx = ["Model", "Scenario", "Variable", "Unit"]
    if param == "sum":
        df_tmp = df.groupby(idx).sum(numeric_only=True).reset_index()
    elif param == "max":
        df_tmp = df.groupby(idx).max().reset_index()
    elif param == "mean":
        # Global region needs to be dropped or else it is used for calcualting
        # the mean
        df_tmp = df[df.Region != "World"]
        df_tmp = df_tmp.groupby(idx).mean().reset_index()
    elif param == "weighted_avg":
        weighted_by = weighted_by.reset_index()
        weighted_by = weighted_by[weighted_by.Region != "World"].set_index("Region")
        df_tmp = df[df.Region != "World"]
        df_tmp = df_tmp.set_index(iamc_idx)
        df_tmp = df_tmp * weighted_by
        df_tmp = df_tmp.reset_index().groupby(idx).sum(numeric_only=True)
        df_tmp = (df_tmp / weighted_by.sum()).fillna(0).reset_index()

    df_tmp.loc[:, "Region"] = globalname
    df_tmp.Region = df_tmp.Region.map(regions)
    df_tmp = df_tmp.set_index(iamc_idx)
    df = df.set_index(iamc_idx)
    df = df_tmp.combine_first(df).reset_index()
    return df.sort_index()


def iamc_it(df, what, mapping, rm_totals=False):
    """Creates any missing parent variables if these are missing and
    adds a pre-fix to the variable name.

    Variable parent/child relationships are determined by separating
    the variable name by "|".

    Parameters
    ----------
    df : dataframe
    what : string
        variable pre-fix
    mapping : dictionary
        contains the variable tree aggregate settings
    rm_totals : True/False (optional, default = False)
        if True then all totals a removed and recalculated.


    Returns
    -------
    df : dataframe
    """

    root = what
    df = iamc_tree.sum_iamc_sectors(
        df, mode="Add", root=root, cleanname=False
    ).reset_index()
    if rm_totals:
        df = df[df.Variable != what]
    df = utilities.EmissionsAggregator(df, mapping=mapping).add_variables().df
    return df.sort_index()


def _make_emptydf(tec, vintage=None, grade=None, units="GWa"):
    """Creates an empty dataframe for cases where data no entries can be found
    in the database for a single or set of technolgies.

    This function should only be used to create placeholders for data that
    cannot be retrieved from the database and which will be procssed in
    subsequent steps.

    Parameters
    ----------
    tec : string or list
        technology name
    vintage : True/False (optional, default = False)
        switch for creating an empty dataframe either with or with-out the
        column "Vintage"
    grade : True/False (optional, default = False)
        switch for creating an empty dataframe either with or with-out the
        column "Grade"
    units : string (optional, default = 'GWa')
        value which will be enetered into the column "Unit"

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage (optional), Mode, Unit,
        Grade (optional)
    """

    dfs = []
    for t in tec:
        if vintage:
            df = pd.DataFrame(
                data={
                    "Region": list(regions.keys()),
                    "Technology": t,
                    "Vintage": firstmodelyear,
                    "Mode": "M1",
                    "Unit": units,
                }
            )
        elif grade:
            df = pd.DataFrame(
                data={
                    "Region": list(regions.keys()),
                    "Commodity": t,
                    "Unit": units,
                    "Grade": "a",
                }
            )
        else:
            df = pd.DataFrame(
                data={
                    "Region": list(regions.keys()),
                    "Technology": t,
                    "Mode": "M1",
                    "Unit": units,
                }
            )
        dfs.append(df)
    df = pd.concat(dfs, sort=True)
    if "Vintage" in df.columns:
        df.loc[:, "Vintage"] = df.loc[:, "Vintage"].astype("object")
    return df.sort_index()


def _make_zero():
    """Creates a dataframe in the iamc output format.

    This function should only be used as a placeholder for reporting variables.

    Returns
    -------
    df : dataframe
       index: iamc_index(values are "0")
    """

    df = pd.DataFrame(data={"Region": list(regions.keys())})
    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _clean_up_regions(df, units=None):
    """Converts region names from the database format into desired
    reporting region names.

    Should a region be missing, it is appended to the dataframe. values = 0.

    Define desired reporting region names in python - variable: "regions".

    Parameters
    ----------
    df : dataframe
    units : string(optional, default=None)
        inserts units into unit column which may be required for unit
        conversion

    Returns
    -------
    df : dataframe
        index: Region, Technology(optional) or Commodity(optional), Unit,
        Vintage(optional), Mode, Grade(optional)
    """
    if not units:
        if "Unit" in df.columns:
            df_unit = df.Unit.unique()
            if len(df_unit) > 1:
                if verbose:
                    print(
                        (
                            inspect.stack()[0][3],
                            ": there are more than 1",
                            "unit in the dataframe:",
                            df_unit,
                        )
                    )
                units = "GWa"
                df.Unit = "GWa"
                idx = [i for i in index_order if i in df.columns.tolist()]
                df = df.groupby(idx).sum(numeric_only=True).reset_index()
            else:
                units = df.Unit.unique()[0]
        else:
            units = "GWa"

    # Makes exception if only a world value is available for the carbon price
    # In MESSAGE_ix_legacy, the carbon price was returned for all regions when
    # a budget is applied. In the January, 2018 version of MESSAGEix, only a
    # global value is returned.
    # Therefore the value needs to be replicated for all other regions. This is
    # only done when there is only a single carbon price entry for the region
    # 'World'.
    # If there are multiple `emission`s for which a price is returned, then the
    # order of the list below defines the order of presidence applied ot the
    # dataframe `emission`.
    if "Technology" in df.columns:
        # List of different `emission`s for which a carbon price is retruned.
        cprice_list = [
            "TCE",
            "TCO2",
            "TCE_CO2",
            "TCE_non-CO2",
            "TCE_FFI",
            "TCE_LU",
            "TCE_CO2_trade",
        ]
        # List of "technologies" (which are in fact the `emissions`)
        teclist = df.Technology.unique().tolist()

        # Check if there is a match of the two lists
        matchlist = [c for c in teclist if c in cprice_list]
        if matchlist:
            # Filter out priority from cprice_list
            df = df.loc[df.Technology == matchlist[0]]

            if df.Region.unique().tolist() == ["World"]:
                # Filter out a single carbon_price
                dfs = []
                dfs.append(df)
                for reg in regions.keys():
                    tmp = df.copy()
                    tmp.Region = reg
                    dfs.append(tmp)
                df = pd.concat(dfs, sort=True)

    # Removes aggregate region 'WORLD'
    df = df[df["Region"] != "World"]

    # for reg in list(regions.keys()):
    for reg in regions.keys():
        if reg not in df.Region.unique():
            if "Technology" in df.columns:
                for tec in df.Technology.unique():
                    if "Mode" in df.columns:
                        if "Vintage" in df.columns:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array(
                                            [[reg, tec, units, firstmodelyear, "M1"]]
                                        ),
                                        columns=[
                                            "Region",
                                            "Technology",
                                            "Unit",
                                            "Vintage",
                                            "Mode",
                                        ],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                        else:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units, "M1"]]),
                                        columns=[
                                            "Region",
                                            "Technology",
                                            "Unit",
                                            "Mode",
                                        ],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                    elif "Unit" in df.columns:
                        if "Vintage" in df.columns:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units, firstmodelyear]]),
                                        columns=[
                                            "Region",
                                            "Technology",
                                            "Unit",
                                            "Vintage",
                                        ],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                        elif "Grade" in df.columns:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units, "a"]]),
                                        columns=[
                                            "Region",
                                            "Technology",
                                            "Unit",
                                            "Grade",
                                        ],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                        else:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units]]),
                                        columns=["Region", "Technology", "Unit"],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                    else:
                        df = pd.concat(
                            [
                                pd.DataFrame(
                                    np.array([[reg, tec]]),
                                    columns=["Region", "Technology"],
                                ),
                                df,
                            ],
                            sort=True,
                        )

            elif "Commodity" in df.columns:
                for tec in df.Commodity.unique():
                    if "Mode" in df.columns:
                        if "Vintage" in df.columns:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array(
                                            [[reg, tec, units, firstmodelyear, "M1"]]
                                        ),
                                        columns=[
                                            "Region",
                                            "Commodity",
                                            "Unit",
                                            "Vintage",
                                            "Mode",
                                        ],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                        else:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units, "M1"]]),
                                        columns=["Region", "Commodity", "Unit", "Mode"],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                    else:
                        if "Vintage" in df.columns:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units, firstmodelyear]]),
                                        columns=[
                                            "Region",
                                            "Commodity",
                                            "Unit",
                                            "Vintage",
                                        ],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                        elif "Grade" in df.columns:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units, "a"]]),
                                        columns=[
                                            "Region",
                                            "Commodity",
                                            "Unit",
                                            "Grade",
                                        ],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
                        else:
                            df = pd.concat(
                                [
                                    pd.DataFrame(
                                        np.array([[reg, tec, units]]),
                                        columns=["Region", "Commodity", "Unit"],
                                    ),
                                    df,
                                ],
                                sort=True,
                            )
            else:
                if "Mode" in df.columns:
                    if "Vintage" in df.columns:
                        df = pd.concat(
                            [
                                pd.DataFrame(
                                    np.array([[reg, units, firstmodelyear, "M1"]]),
                                    columns=["Region", "Unit", "Vintage", "Mode"],
                                ),
                                df,
                            ],
                            sort=True,
                        )
                    else:
                        df = pd.concat(
                            [
                                pd.DataFrame(
                                    np.array([[reg, units, "M1"]]),
                                    columns=["Region", "Unit", "Mode"],
                                ),
                                df,
                            ],
                            sort=True,
                        )
                else:
                    if "Vintage" in df.columns:
                        df = pd.concat(
                            [
                                pd.DataFrame(
                                    np.array([[reg, units, firstmodelyear]]),
                                    columns=["Region", "Unit", "Vintage"],
                                ),
                                df,
                            ],
                            sort=True,
                        )
                    elif "Grade" in df.columns:
                        df = pd.concat(
                            [
                                pd.DataFrame(
                                    np.array([[reg, units, "a"]]),
                                    columns=["Region", "Unit", "Grade"],
                                ),
                                df,
                            ],
                            sort=True,
                        )
                    else:
                        df = pd.concat(
                            [
                                pd.DataFrame(
                                    np.array([[reg, units]]), columns=["Region", "Unit"]
                                ),
                                df,
                            ],
                            sort=True,
                        )

    df.Region = df.Region.map(regions)
    if "Vintage" in df.columns:
        df["Vintage"] = df["Vintage"].apply(np.int64)
        df["Vintage"] = df["Vintage"].astype("object")
    return df.sort_index()


def _clean_up_vintage(ds, df, units=None):
    """Checks if a vintage year is found for each region.

    If a vintage year is missing it is appended to the dataframe. values = 0.

    Parameters
    ----------
    ds : ix-datastructure
    df : dataframe
    units: string(optional, default=None)
        inserts units into unit column which may be required for
        unit conversion

    Returns
    -------
    df : dataframe
        index: Region, Technology, Unit, Vintage(optional), Mode(optional)
    """
    if not units:
        if "Unit" in df.columns:
            df_unit = df.Unit.unique()
            if len(df_unit) > 1:
                if verbose:
                    print(
                        (
                            inspect.stack()[0][3],
                            ": there are more than 1 unit in the dataframe:",
                            df_unit,
                        )
                    )
                units = "GWa"
            else:
                units = df.Unit.unique()[0]
        else:
            units = "GWa"

    def set_standard(column, df):
        if column == "Mode":
            values = df.Mode.unique().tolist()
        elif column == "Region":
            values = [regions[reg] for reg in list(regions.keys())]
        elif column == "Technology":
            values = df.Technology.unique().tolist()
        elif column == "Unit":
            values = [units]
        elif column == "Vintage":
            values = all_years.tolist()
        return values

    index = nonnumcols(df)

    reg = [regions[reg] for reg in list(regions.keys())]
    tec = df.Technology.unique().tolist()
    unit = [units]
    if "Mode" in index:
        mode = df.Mode.unique().tolist()
        idx = pd.MultiIndex.from_tuples(
            list(itertools.product(reg, tec, unit, mode, all_years)), names=index
        )
    else:
        idx = pd.MultiIndex.from_tuples(
            list(itertools.product(reg, tec, unit, all_years)), names=index
        )
    df_fill = pd.DataFrame(0, index=idx, columns=numcols(df)).reset_index()
    df_fill["Vintage"] = df_fill["Vintage"].apply(np.int64)
    df_fill["Vintage"] = df_fill["Vintage"].astype("object")
    df = df.set_index(index).combine_first(df_fill.set_index(index)).reset_index()
    return df.sort_index()


def compare(x, y):
    """Comparison function that is Python 2/3 robust"""
    if x == y:
        return 0
    try:
        if x < y:
            return -1
        else:
            return 1
    except TypeError as e:
        # The case where both are None is taken care of by the equality test
        if x is None:
            return -1
        elif y is None:
            return 1
        # Compare by type name
        if type(x) != type(y):
            return compare(type(x).__name__, type(y).__name__)
        elif isinstance(x, type):
            return compare(x.__name__, y.__name__)
        # Types are the same but a native compare didn't work, recursively
        # compare elements
        try:
            for a, b in zip(x, y):
                c = compare(a, b)
                if c != 0:
                    return c
        except TypeError:
            raise e

        return compare(len(x), len(y))


def _clean_up_years(df, method="zero"):
    """Checks if columns are missing an years.

    If a column is missing it is added. values = 0

    Years are defined in the model and are retrieved via the python-
    variable: years

    Parameters
    ----------
    df : dataframe
    method : string (optional, default = 'forwardfill')
        possiblities for filling data for missing years.
        forwardfill -> fills future users with last available year
        zero -> fills with 0

    Returns
    -------
    df : dataframe
        index: the index of the input dataframe will be preserved
    """

    add_years = [y for y in years if y not in numcols(df)]
    if add_years:
        if method == "zero":
            new_df = pd.DataFrame(dict.fromkeys(add_years, 0), index=df.index)
            df = pd.concat([df, new_df], axis=1, sort=True)
            df = df.fillna(0)
            oth_idx = sorted(nonnumcols(df), key=cmp_to_key(compare))
            yr_idx = sorted(numcols(df), key=cmp_to_key(compare))
            idx = oth_idx + yr_idx
            df = df.reindex(idx, axis=1)
        elif method == "ffill":
            new_df = pd.DataFrame(dict.fromkeys(add_years, pd.np.nan), index=df.index)
            df = pd.concat([df, new_df], axis=1, sort=True)
            oth_idx = sorted(nonnumcols(df), key=cmp_to_key(compare))
            yr_idx = sorted(numcols(df), key=cmp_to_key(compare))
            idx = oth_idx + yr_idx
            df = df.reindex(idx, axis=1)
            df = df.set_index(oth_idx)
            # use previous values to fill forward and anythin else with 0
            df = df.fillna(method="ffill", axis=1).fillna(0).reset_index()
    return df.sort_index()


def _clean_up_formatting(df):
    """Indexing is set depending on input column names.

    Function checks for columns: Vintage, Grade, Mode

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    df : dataframe
        index: Region, Technology (optional), Commodity (optional),
        Vintage (optional), Mode (optional), Grade (optional)
        the index will be set to all non-numerical columns with the
        exception of 'Vintage' which will also be part of the index
        structure. the column units is dropped
    """
    if "Vintage" in df.columns:
        df["Vintage"] = df["Vintage"].apply(np.int64)
        df["Vintage"] = df["Vintage"].astype("object")

    yrs = [int(x) for x in numcols(df)]

    values = diff(df.columns.tolist(), yrs)
    values.sort(key=lambda x: index_order.index(x))
    values = [val for val in values if val != "Unit"]
    cols = values + yrs
    df = df[cols]
    df = df.set_index(values)

    return df.sort_index()


def _drap(df, pivot_col, drop=None, add=None, group=None):
    """Drop, Renames, Adds and Pivots variables dataframes

    Parameters
    ----------

    df :  dataframe
    pivot_col : list
        a list containging two column names, of which the first tells the
        pivot_table function which column contains the values and
        the second indicates the index column which contains the year
        entries which are used to pivot the results.
    drop : list
        a list of columns to be dropped from the dataframe
    add : dictionary or list
        must contain two entries: the first is the new column name; the
        second is the value to be entered in that column
    group : list
        a list of columns over which the dataframe should be grouped

    Returns
    -------
    df : dataframe
        index: in IAMC style format - can contain some extra index entries
        (Mode, Grade, etc) over which an sum is formed later on in the script

    """

    col_newnames = {
        "node": "Region",
        "node_loc": "Region",
        "emission": "Technology",
        "type_emission": "Technology",
        "land_scenario": "Technology",
        "technology": "Technology",
        "unit": "Unit",
        "mode": "Mode",
        "year_vtg": "Vintage",
        "grade": "Grade",
        "commodity": "Commodity",
    }

    # Drop columns if defined
    if drop:
        drop = [drop] if type(drop) == str else drop
        df = df.drop(drop, axis=1)

    # Groups dataframe
    if group:
        df = df.groupby(group).sum(numeric_only=True).reset_index()

    # Renames columns
    df = df.rename(columns=col_newnames)

    # Adds column with a spcified value
    if add:
        if type(add) == dict:
            for var in add.keys():
                df[var] = add[var]
        elif type(add) == list:
            df[add[0]] = df[add[1]]

    # An exception has to be made for the column "Vintage" as this is included
    # in the numcols; in some case 'Vintage' will be used for the years and
    # can therefore be in pivot_col
    df = df.pivot_table(
        pivot_col[0],
        [
            x
            for x in df.columns
            if (x not in pivot_col and x not in numcols(df))
            or (x == "Vintage" and x not in pivot_col)
        ],
        pivot_col[1],
    )

    return df.reset_index()


def _retr_act_data(ds, ix, param, filter, units, convert=1):
    """Output for a single or set of technolgies is retrieved.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'reference_activity' or 'ACT' (IX only)
    filter : dictionary
        filters specific to 'reference_activity' or 'ACT' tables
    units : string
        see unit doc
    convert : integer(optional, default = 1)
        0 turns off unit conversion
        1 turns on unit conversion

    Returns
    -------
    df : dataframe
        index: Region, Technology, Mode, Vintage(IX only)
    """

    tec = (
        [filter["technology"][0]]
        if type(filter["technology"][0]) == str
        else filter["technology"][0]
    )
    df = ds.var(param, filter) if ix else ds.par(param, filter)

    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        df = _make_emptydf(tec, vintage=1) if ix else _make_emptydf(tec)
    else:
        if ix:
            drop = ["time", "mrg"]
            group = ["node_loc", "technology", "year_act", "year_vtg", "mode"]
            add = {"Unit": "GWa"}
            df = _drap(df, ["lvl", "year_act"], add=add, drop=drop, group=group)
        else:
            drop = ["time"]
            group = ["node_loc", "technology", "unit", "year_act", "mode"]
            df = _drap(df, ["value", "year_act"], drop=drop, group=group)

    df = _clean_up_regions(df)
    if ix:
        df = _clean_up_vintage(ds, df)
    df = _clean_up_years(df)
    if convert and units is not None:
        df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_io_data(ds, ix, param, filter, formatting="standard"):
    """Retrieves commodity - input or commodity - output coefficients for a
    single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'output' or 'input'
    filter : dictionary
        filters specific to technology, input, output tables
    method : string (optional, default is defined by the callinf function)
        possiblities for filling data for missing years.
        forwardfill -> fills future users with last available year
        zero -> fills with 0
    formatting : string (optional, default = 'standard')
        the formatting can be set to "default" in which case the "Vintage" will
        be preserved alternatively, the formatting can be set to "reporting" in
        which case values will be returned only for those cases
        where "year_act" == "year_vtg"

    Returns
    -------
    df : dataframe
        index: Region, Technology, Mode, Vintage
        ( for IX and formatting == 'standard' only)
    """

    tec = (
        [filter["technology"][0]]
        if type(filter["technology"][0]) == str
        else filter["technology"][0]
    )
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        if ix and formatting != "reporting":
            df = _make_emptydf(tec, vintage=1)
        else:
            df = _make_emptydf(tec)
    else:
        if ix and formatting != "reporting":
            if param == "input":
                drop = ["time_origin", "commodity", "level", "node_origin", "time"]
            if param == "output":
                drop = ["time_dest", "commodity", "level", "node_dest", "time"]
            group = ["node_loc", "technology", "unit", "year_act", "year_vtg", "mode"]

        else:
            # Filter out all entries where year_act == year_vtg
            tmp = df[df.year_act == df.year_vtg]
            # Fill years where year_act is greater than the latest
            # year_vtg. This must be done for each region and technology
            # as more than one technology may be in a datframe and these have
            # differing lifetimes.
            for reg in df.node_loc.unique():
                for t in df[df.node_loc == reg].technology.unique():
                    tmp_df = df[(df.node_loc == reg) & (df.technology == t)]
                    yr_miss = [
                        y
                        for y in tmp_df.year_act.unique()
                        if y
                        not in tmp[
                            (tmp.node_loc == reg) & (tmp.technology == t)
                        ].year_vtg.unique()
                    ]
                    tmp_df = tmp_df[tmp_df.year_act.isin(yr_miss)]
                    for y in yr_miss:
                        tmp_df2 = tmp_df[tmp_df.year_act == y]
                        tmp_df2 = tmp_df2[tmp_df2.year_vtg == tmp_df2.year_vtg.max()]
                        tmp_df2.year_vtg = tmp_df2.year_act
                        tmp = pd.concat([tmp, tmp_df2], sort=True)
            tmp = tmp.reset_index().drop("index", axis=1)

            df = tmp
            if param == "input":
                drop = [
                    "time_origin",
                    "commodity",
                    "level",
                    "node_origin",
                    "year_vtg",
                    "time",
                ]
            if param == "output":
                drop = [
                    "time_dest",
                    "commodity",
                    "level",
                    "node_dest",
                    "year_vtg",
                    "time",
                ]
            group = ["node_loc", "technology", "unit", "year_act", "mode"]
        df = _drap(df, ["value", "year_act"], drop=drop, group=group)

    df = _clean_up_regions(df)
    if ix and formatting != "reporting":
        df = _clean_up_vintage(ds, df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_emi_data(ds, ix, param, emifilter, tec, units):
    """Retrieves coefficient for emissions

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param: string
        'relation_activity'
    emifilter: dictionary
        filters specific to relation tables
    tec: string or list
        name of relation
    units: string
        see unit doc

    Returns
    -------
    df: dataframe
        index: Region, Technology, Mode
    """

    filter = (
        {"technology": tec}
        if emifilter is None
        else dict(list(emifilter.items()) + list({"technology": tec}.items()))
    )
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        df = _make_emptydf(tec)
    else:
        df = df if ix else df[df.year_act == df.year_rel]
        drop = ["node_rel", "relation", "year_rel"]
        df = _drap(df, ["value", "year_act"], drop=drop)

    # Clean up operations
    df = _convert_units(df, units)
    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_cpf_data(ds, ix, param, tec, formatting="standard"):
    """Retrieves capacity factor for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'capacity_factor'
    tec : string or list
        technology name
    formatting : string (optional, default = 'standard')
        the formatting can be set to "default" in which case the "Vintage" will
        be preserved alternatively, the formatting can be set to "reporting" in
        which case values will be returned only for those cases
        where "year_act" == "year_vtg"

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage(IX only)
    """

    filter = {"technology": tec}
    # checks if technology exists, if not then a proxy df is made.
    if tec not in all_tecs.values:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "not included in model"))
        if ix and formatting != "reporting":
            df = _make_emptydf(tec, vintage=1)
        else:
            df = _make_emptydf(tec)
    else:
        df = ds.par(param, filter)
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
            if ix and formatting != "reporting":
                df = _make_emptydf(tec, vintage=1)
            else:
                df = _make_emptydf(tec)
        else:
            if ix and formatting != "reporting":
                drop = ["time"]
            else:
                df = df[df.year_act == df.year_vtg]
                drop = ["time", "year_vtg"]
            df = _drap(df, ["value", "year_act"], drop=drop)

    # Clean up operations
    df = _clean_up_regions(df)
    if ix and formatting != "reporting":
        df = _clean_up_vintage(ds, df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_tic_data(ds, ix, param, tec, units):
    """Retrieves total installed capacity for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'historical_new_capacity' or 'CAP' (IX only)
    tec : string or list
        technology name
    units : string
        see unit doc

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage
    """

    filter = {"technology": tec}
    if ix:
        df = ds.var(param, filter)
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
            df = _make_emptydf(tec, vintage=1) if ix else _make_emptydf(tec)
        else:
            drop = ["mrg"]
            add = {"Unit": "GW"}
            df = _drap(df, ["lvl", "year_act"], drop=drop, add=add)
    else:
        # The reason why this cant be calcualted is because the only capacity related
        # paramter is historical_new_capacity. This does not though tell you what
        # historical capacity may have been retired earlier than the end of its
        # lifetime. Assuming that all ppls are used until the end of their lifetime
        # is a false assumption.
        if verbose:
            print((inspect.stack()[0][3], ": is not yet working for non-IX results"))
        df = _make_emptydf(tec)

    # Clean up operations
    df = _clean_up_regions(df, units=units)
    if ix:
        df = _clean_up_vintage(ds, df, units=units)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_extr_data(ds, ix, param, tec, units):
    """Retrieves extracted resource quanitites

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param: string
        'historical_extraction' or 'EXT' (IX only)
    tec: string or list
        extraction technology name
    units: string
        see unit doc
    method : string (optional, default = 'zero')
        possiblities for filling data for missing years.
        forwardfill -> fills future users with last available year
        zero -> fills with 0

    Returns
    -------
    df : dataframe
        index: Region, Commodity, Grade
    """

    filter = {"commodity": tec}
    df = ds.var(param, filter) if ix else ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        df = _make_emptydf(tec, grade=1)
    else:
        add = {"Unit": "GWa"}
        if ix:
            drop = ["mrg"]
            df = _drap(df, ["lvl", "year"], drop=drop, add=add)
        else:
            df = _drap(df, ["value", "year"], add=add)
    # Clean up operations
    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_histextr_data(ds, ix, param, tec, units):
    """Retrieves pre - 2020 extracted resource quanitites (IX only)

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'historical_extraction'
    tec : string or list
        extraction technology name
    units : string
        see unit doc
    method : string (optional, default = 'zero')
        possiblities for filling data for missing years.
        forwardfill -> fills future users with last available year
        zero -> fills with 0

    Returns
    -------
    df: dataframe
        index: Region, Commodity, Grade
    """

    filter = {"commodity": tec}
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        df = _make_emptydf(tec, grade=1)
    else:
        add = {"Unit": "GWa"}
        df = _drap(df, ["value", "year"], add=add)
    # Clean up operations
    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_rel_data(ds, ix, param, relfilter, tec):
    """Retrieves coefficient with which a technology writes into a given
    relation for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'relation_activity'
    relfilter : dictionary
        filters specific to relation tables
    tec : string or list
        technology name

    Returns
    -------
    df : dataframe
        index: Region, Technology, Mode
    """

    # Checks for additonal keyword arguments
    filter = (
        {"technology": tec}
        if relfilter is None
        else dict(list(relfilter.items()) + list({"technology": tec}.items()))
    )
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        df = _make_emptydf(tec)
    else:
        df = df if ix else df[df.year_act == df.year_rel]
        drop = ["node_rel", "relation", "year_rel"]
        df = _drap(df, ["value", "year_act"], drop=drop)

    # Clean up operations
    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_nic_data(ds, ix, param, tec, units):
    """Retrieves new installed capacity for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'historical_new_capacity' or 'CAP_NEW' (IX only)
    tec : string or list
        technology name
    units : string
        see unit doc

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage
    """

    filter = {"technology": tec}
    if ix:
        df = ds.var(param, filter)
        # add a column "year_act"
        df["year_act"] = df["year_vtg"]
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
            df = _make_emptydf(tec, vintage=1)
        else:
            drop = ["mrg"]
            add = {"Unit": "GW"}
            df = _drap(df, ["lvl", "year_act"], drop=drop, add=add)
    else:
        df = ds.par(param, filter)
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
            df = _make_emptydf(tec)
        else:
            add = {"Unit": "GW"}
            df = _drap(df, ["value", "Vintage"], add=add)

    # Clean up operations
    df = _clean_up_regions(df, units=units)
    if ix:
        df = _clean_up_vintage(ds, df, units=units)
    # Method is 'zero' so that the new installed capacity is only retained for
    # the single year in which it is built
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_capcost_data(ds, param, tec, units):
    """Retrieves capital cost for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    param : string
        'inv_cost'
    tec : string or list
        technology name
    units : string
        see unit doc

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage
    """

    filter = {"technology": tec}
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        df = _make_emptydf(tec, units="USD/GWa")
    else:
        df = _drap(df, ["value", "Vintage"])

    # Clean up operations
    df = _clean_up_regions(df, units="USD/GWa")
    df = _clean_up_years(df)
    df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_fom_data(ds, ix, param, tec, units, formatting):
    """Retrieves fix O&M cost for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'fix_cost'
    tec : string or list
        technology name
    units : string
        see unit doc
    formatting : string (optional, default = 'standard')
        the formatting can be set to "default" in which case the "Vintage"
        will be preserved alternatively, the formatting can be set to
        "reporting" in which case values will be returned only for
        those cases where "year_act" == "year_vtg"

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage
    """

    filter = {"technology": tec}
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        if formatting == "standard" and ix:
            df = _make_emptydf(tec, vintage=1, units="USD/GWa")
        if formatting == "reporting" or not ix:
            df = _make_emptydf(tec, units="USD/GWa")
    else:
        if formatting == "standard" and ix:
            df = _drap(df, ["value", "year_act"])
        if formatting == "reporting" or not ix:
            df = df[df.year_act == df.year_vtg]
            drop = ["year_act"]
            df = _drap(df, ["value", "Vintage"], drop=drop)

    # Clean up operations
    df = _clean_up_regions(df, units="USD/GWa")
    if formatting == "standard" and ix:
        df = _clean_up_vintage(ds, df, units="USD/GWa")
    df = _clean_up_years(df)
    df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_vom_data(ds, ix, param, tec, units, formatting):
    """Retrieves variable O&M cost for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'var_cost'
    tec : string or list
        technology name
    units : string
        see unit doc
    formatting : string (optional, default = 'standard')
        the formatting can be set to "default" in which case the "Vintage"
        will be preserved alternatively, the formatting can be set to
        "reporting" in which case values will be returned only for those
        cases where "year_act" == "year_vtg"

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage
    """

    filter = {"technology": tec}
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        if formatting == "standard" and ix:
            df = _make_emptydf(tec, vintage=1, units="USD/GWa")
        if formatting == "reporting" or not ix:
            df = _make_emptydf(tec, units="USD/GWa")
    else:
        if formatting == "standard" and ix:
            drop = ["time"]
            df = _drap(df, ["value", "year_act"], drop=drop)
        if formatting == "reporting" or not ix:
            df = df[df.year_act == df.year_vtg]
            drop = ["time", "year_act"]
            df = _drap(df, ["value", "Vintage"], drop=drop)

    # Clean up operations
    df = _clean_up_regions(df, units="USD/GWa")
    if formatting == "standard" and ix:
        df = _clean_up_vintage(ds, df, units="USD/GWa")
    df = _clean_up_years(df)
    df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_pll_data(ds, param, tec, units):
    """Retrieves technical lifetime for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    param : string
        'var_cost'
    tec : string or list
        technology name
    units : string
        see unit doc

    Returns
    -------
    df : dataframe
        index: Region, Technology, Vintage
    """

    filter = {"technology": tec}
    df = ds.par(param, filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        df = _make_emptydf(tec, units="y")
    else:
        df = _drap(df, ["value", "Vintage"])

    # Clean up operations
    df = _clean_up_regions(df, units="y")
    df = _clean_up_years(df)
    df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_crb_prc(ds, units):
    """Retrieves carbon price.

    Parameters
    ----------
    ds : ix-datastructure
    units : string
        see unit doc

    Returns
    -------
    df : dataframe
        index: Region, Technology
    """

    # Retrieve VAR - PRICE_EMISSION
    var = "PRICE_EMISSION_NEW"
    df = ds.var(var, {"type_tec": ["all"]})
    if df.empty:
        if var == "PRICE_EMISSION":
            df = ds.par("tax_emission", {"type_tec": ["all"]})
            type_emi = df.type_emission.unique().tolist()
            if len(type_emi) > 1:
                df = df.loc[df.type_emission == type_emi[0]]
                print("Reporting Carbon Price for", type_emi[0])
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], f": {var} dataframe empty"))
            df = _make_emptydf("TCE", units="US$2005/tC")

        else:
            drop = ["type_tec", "unit"]
            add = {"Unit": "US$2005/tC"}
            df = _drap(df, ["value", "type_year"], drop=drop, add=add)
            for yr in numcols(df):
                if type(yr) != int:
                    df = df.rename(columns={yr: int(yr)})
    else:
        drop = ["type_tec", "mrg"]
        add = {"Unit": "US$2005/tC"}
        df = _drap(df, ["lvl", "year"], drop=drop, add=add)

    # Clean up operations
    df = _clean_up_regions(df, units="US$2005/tC")
    df = _clean_up_years(df)
    if units:
        df = _convert_units(df, units)
    df = _clean_up_formatting(df)

    #    # Retrieve parameter tax_emission
    #    df2 = ds.par('tax_emission', {'type_tec': ['all']})
    #    type_emi = df2.type_emission.unique().tolist()
    #    if len(type_emi) > 1:
    #        df2 = df2.loc[df2.type_emission == type_emi[0]]
    #        print('Reporting Carbon Price for', type_emi[0])
    #    if df2.empty:
    #        df2 = _make_emptydf('TCE', units='US$2005/tC')
    #    else:
    #        drop = ['type_tec', 'unit']
    #        add = {'Unit': 'US$2005/tC'}
    #        df2 = _drap(df2, ['value', 'type_year'], drop=drop, add=add)
    #        for yr in numcols(df2):
    #            if type(yr) != int:
    #                df2 = df2.rename(columns={yr: int(yr)})
    #    df2 = _clean_up_regions(df2, units='US$2005/tC')
    #    df2 = _clean_up_years(df2)
    #    if units:
    #        df2 = _convert_units(df2, units)
    #    df2 = _clean_up_formatting(df2)
    #
    #    # Take the maximum of both dataframes
    #    df = pd.concat([df, df2]).groupby(level=[0, 1]).max()
    #    df = df[sorted(df.columns)]

    return df.sort_index()


def _retr_ene_prc(ds, units, enefilter):
    """Retrieves energy price.

    Parameters
    ----------
    ds : ix-datastructure
    units : string
        see unit doc
    enefilter : dictionary
        filter specific to "PRICE_COMMODITY' table

    Returns
    -------
    df : dataframe
        index: Region, Technology (where Technology == Commodity)
    """
    filter = enefilter
    df = ds.var("PRICE_COMMODITY", filter)
    if df.empty:
        if verbose:
            print(
                (
                    inspect.stack()[0][3],
                    ": technology",
                    filter["commodity"],
                    "dataframe empty",
                )
            )
        df = _make_emptydf(filter["commodity"], units="USD/GWa")
    else:
        drop = ["level", "mrg", "time"]
        add = {"Unit": "USD/GWa"}
        df = _drap(df, ["lvl", "year"], drop=drop, add=add)

    # Clean up operations
    df = _clean_up_regions(df, units="USD/GWa")
    df = _clean_up_years(df)
    df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_demands(ds, ix, commodity, level, units):
    """Retrieves demands.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    commodity : string
        the commodity on which the results should be filtered
    level : string
        the level on which the results should be filtered
    units : string
        see unit doc

    Returns
    -------
    df : dataframe
        index: Region, Technology
    """
    filter = {"commodity": commodity, "level": [level]}

    # Retrieves par('demand_fixed') if var('DEMAND') doesnt exist
    if "DEMAND" in ds.var_list():
        df = ds.var("DEMAND", filter)
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], ": DEMAND dataframe empty"))
            df = _make_emptydf(commodity)
        else:
            drop = ["time", "mrg", "level", "commodity"]
            add = {"Unit": "GWa"}
            df = _drap(df, ["lvl", "year"], drop=drop, add=add)
        df_macro = df
    if "DEMAND" not in ds.var_list() or not ix:
        df = ds.par("demand", filter)
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], ": DEMAND dataframe empty"))
            df = _make_emptydf(commodity)
        else:
            drop = ["time", "level", "commodity"]
            df = _drap(df, ["value", "year"], drop=drop)
        df_message = df
    if not ix and "DEMAND" in ds.var_list():
        df_macro = df_macro.set_index(["Region", "Unit"]).fillna(0)
        df_message = df_message.set_index(["Region", "Unit"]).fillna(0)
        df_message = df_message.drop(df_macro.columns, axis=1)
        df = df_message.add(df_macro, fill_value=0).fillna(0).reset_index()

    # Clean up operations
    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    if units:
        df = _convert_units(df, units)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_emif_data(ds, ix, param, emiflt, tec, units=None):
    """Retrieves coefficient with which a technology writes into a given
    relation for a single or set of technolgies.

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'
    param : string
        'emission_factor'
    emiflt : dictionary
        filters specific to emission_factor tables
    tec: string or list
        technology name
    unit : string
        see unit doc

    Returns
    -------
    df: dataframe
        index: Region, Technology, Mode
    """

    # Checks for additonal keyword arguments
    df = ds.par(param, emiflt)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": technology", tec, "dataframe empty"))
        if ix:
            df = _make_emptydf(tec, vintage=1, units="-")
        else:
            df = _make_emptydf(tec, units="-")
    else:
        if ix:
            drop = ["emission"]
            df = _drap(df, ["value", "year_act"], drop=drop)
        else:
            df = df[df.year_act == df.year_vtg]
            drop = ["emission", "year_vtg"]
            df = _drap(df, ["value", "year_act"], drop=drop)

    # Clean up operations
    if units:
        df = _convert_units(df, units)
    df = _clean_up_regions(df)
    if ix:
        df = _clean_up_vintage(ds, df, units="-")
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)
    return df.sort_index()


def _retr_par_MERtoPPP(ds):
    """Retrieves parameter conversion factor from GDP(MER) to GDP (PPP)

    Parameters
    ----------
    ds : ix-datastructure

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    df = ds.par("MERtoPPP")
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": parameter MERtoPPP dataframe empty"))
        df = _make_emptydf("MERtoPPP")
    else:
        drop = ["unit"]
        add = {"Technology": "MERtoPPP"}
        df = _drap(df, ["value", "year"], drop=drop, add=add)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_var_consumption(ds):
    """Retrieve variable "Consumption"

    Parameters
    ----------
    ds : ix-datastructure

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    df = ds.var("C")
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": variable C (Consumption) dataframe empty"))
        df = _make_emptydf("C")
    else:
        drop = ["mrg"]
        add = {"Technology": "C"}
        df = _drap(df, ["lvl", "year"], drop=drop, add=add)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_var_gdp(ds, ix):
    """Retrieve variable "GDP"

    Parameters
    ----------
    ds : ix-datastructure

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    if ix:
        df = ds.var("GDP")
        if df.empty:
            if verbose:
                print(
                    (
                        inspect.stack()[0][3],
                        ": variable GDP dataframe empty,",
                        " using parameter: gdp_calibrate",
                    )
                )
            df = ds.par("gdp_calibrate")
            if df.empty:
                if verbose:
                    print(
                        (
                            inspect.stack()[0][3],
                            ": parameter gdp_calibrate dataframe empty",
                        )
                    )
                df = _make_emptydf("GDP")
            else:
                add = {"Technology": "GDP"}
                df = _drap(df, ["value", "year"], add=add)
        else:
            drop = ["mrg"]
            add = {"Technology": "GDP"}
            df = _drap(df, ["lvl", "year"], drop=drop, add=add)
    else:
        df = ds.par("historical_activity", filters={"technology": "GDP"})
        if df.empty:
            if verbose:
                print((inspect.stack()[0][3], ": technology", "GDP", "dataframe empty"))
            df = _make_emptydf("GDP")
        else:
            drop = ["time"]
            group = ["node_loc", "technology", "unit", "year_act", "mode"]
            df = _drap(df, ["value", "year_act"], drop=drop, group=group)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_var_cost(ds):
    """Retrieve variable "COST_NODAL_NET"

    Parameters
    ----------
    ds : ix-datastructure

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    df = ds.var("COST_NODAL_NET")
    if df.empty:
        if verbose:
            print(
                (
                    inspect.stack()[0][3],
                    ": variable COST_NODAL_NET dataframe empty,",
                    " check the GAMS postproccesing in MESSAGE_run!",
                )
            )
    else:
        drop = ["mrg"]
        add = {"Technology": "COST_NODAL_NET"}
        df = _drap(df, ["lvl", "year"], drop=drop, add=add)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_land_act(ds, ix):
    """Retrieve activity of land-use technologies

    Parameters
    ----------
    ds : ix-datastructure
    ix : string
        'True' or 'False'

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    df = ds.var("LAND") if ix else ds.par("historical_land")
    if df.empty:
        if verbose:
            print(
                (
                    inspect.stack()[0][3],
                    ": variable LAND dataframe empty. Please check that",
                    " correct version of the land emulator is being used",
                )
            )
        sys.exit(1)
    elif ix:
        drop = ["mrg"]
        df = _drap(df, ["lvl", "year"], drop=drop)
    else:
        drop = ["unit"]
        df = _drap(df, ["value", "year"], drop=drop)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_land_emission(ds, tec, units, convert=1):
    """Retrieve emissions of land-use technologies

    Parameters
    ----------
    ds : ix-datastructure
    tec : string
        emission type
    units : string
        see unit doc
    convert : integer
        switch to convert units (optional, 0 = False, 1 = True (default))

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    filter = {"emission": [tec]}
    df = ds.par("land_emission", filter)
    if df.empty:
        if verbose:
            print(
                (
                    inspect.stack()[0][3],
                    ": parameter land_emission",
                    tec,
                    " dataframe empty. Please check that correct version of",
                    " the land emulator is being used",
                )
            )
        df = _make_emptydf(tec)
    else:
        drop = ["emission"]
        df = _drap(df, ["value", "year"], drop=drop)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    if convert and units is not None:
        df = _convert_units(df, units)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_land_output(ds, filter, units, convert=1):
    """Retrieve emissions of land-use technologies

    Parameters
    ----------
    ds : ix-datastructure
    filter : dictionary
        specific to 'land_output' table
    units : string
        see unit doc
    convert : integer
        switch to convert units (optional, 0 = False, 1 = True (default))

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    df = ds.par("land_output", filter)
    if df.empty:
        if verbose:
            print(
                (
                    inspect.stack()[0][3],
                    ": parameter land_output",
                    filter,
                    " dataframe empty. Please check that correct version of",
                    " the land emulator is being used",
                )
            )
        df = _make_emptydf(filter["commodity"])
    else:
        drop = ["commodity", "level", "time"]
        df = _drap(df, ["value", "year"], drop=drop)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    if convert and units is not None:
        df = _convert_units(df, units)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_land_use(ds, filter, units, convert=1):
    """Retrieve land use by type of land-use technologies

    Parameters
    ----------
    ds : ix-datastructure
    filter : dictionary
        specific to 'land_output' table
    units : string
        see unit doc
    convert : integer
        switch to convert units (optional, 0 = False, 1 = True (default))

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    df = ds.par("land_use", filter)
    if df.empty:
        print(
            (
                inspect.stack()[0][3],
                ": parameter land_use",
                filter,
                " dataframe empty. Please check that correct version of",
                " the land emulator is being used",
            )
        )
        sys.exit(1)
    else:
        drop = ["land_type"]
        df = _drap(df, ["value", "year"], drop=drop)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    if convert and units is not None:
        df = _convert_units(df, units)
    df = _clean_up_formatting(df)

    return df.sort_index()


def _retr_emiss(ds, emission, type_tec):
    """Retrieve emissions

    Parameters
    ----------
    ds : ix-datastructure
    emission : string
        emission type
    type_tec : string
        'all' or 'cumulative'

    Returns
    -------
    df: dataframe
        index: Region, Technology
    """

    filter = {"emission": [emission], "type_tec": [type_tec]}
    df = ds.var("EMISS", filter)
    if df.empty:
        if verbose:
            print((inspect.stack()[0][3], ": variable EMISS dataframe empty"))
        df = _make_emptydf(emission)
    else:
        drop = ["mrg", "type_tec"]
        df = _drap(df, ["lvl", "year"], drop=drop)

    df = _clean_up_regions(df)
    df = _clean_up_years(df)
    df = _clean_up_formatting(df)

    return df.sort_index()
