import numpy as np
import pandas as pd

from .get_nodes import get_nodes
from .utilities import intpol


def add_missing_years(df, model_years, missing_years, idx_yr):
    """Adds missing years and formats dataframe.

    Missing model years are added via interpolation.

    The dataframe is then formatted so it can be used to overwrite existing model data.

    Parameters
    ----------
    df : pandas.DataFrame
        Data frame containing cost data, in wide format (i.e. having years as columns
        plus "technology", "node_loc" and "unit").
    model_years : .list
        List of years in the model, starting from 1990 up to 2100.
    missing_years : .list
        List of years for which data needs to be added, e.g. [2015, 2025, 2035, 2045,
        2055].
    idx_yr : .str
        Being ``year_vtg`` when filling investment costs, or ``year_act`` for fixed O&M
        costs (**only** if vintaging is turned off).

    Returns
    -------
    df : DataFrame
        Data frame containing cost data for all model years
    df_tec : list
        List of technologies for which cost data should be updated
    """

    for y in missing_years:
        df[y] = df.apply(
            lambda row: intpol(
                row[model_years[model_years.index(y) - 1]],
                row[model_years[model_years.index(y) + 1]],
                model_years[model_years.index(y) - 1],
                model_years[model_years.index(y) + 1],
                y,
            )
            if y not in [model_years[0], model_years[-1]]
            else float(row[y]),
            axis=1,
        )

    # The last value for 2110 is equal to 2100
    df[2110] = df[2100]

    # Formats the dataframe so it can be combined with model data
    df = df.melt(
        id_vars=["node_loc", "technology", "unit"], var_name=idx_yr, value_name="value1"
    ).set_index(["node_loc", "technology", "unit", idx_yr])

    # Creates technology list
    df_tec = df.reset_index().technology.unique()

    return df, df_tec


def main(scenario, data_path, ssp, data_filename=None):
    """Update fixed O&M and investment costs.

    The investment as well as the fixed O&M costs are read from an xlsx file for the
    corresponding SSP scenario. The costs are currently provided for 10 year
    timesteps. Hence, missing model year values are interpolated through
    :func:`~update_fix_and_inv_cost.add_missing_years`.

    Parameters
    ----------
    scenario : :class:`message_ix.Scenario`
        Scenario to which changes should be applied.
    data_path : :class:`pathlib.Path`
        Path to model-data directory.
    ssp : .str
        The SSP version for which costs should be updated.
        (SSP1, SSP2 or SSP3)
    data_filename : .str
        Name of file including extension (".xlsx").
    """
    if not data_filename:
        data_filename = "{}_techinput.xlsx".format(ssp)
    data_file = data_path / "investment_cost" / data_filename

    # Configures the rows/columns which need to be read
    # from the xlsx
    usecols = "B,F:S"
    inv_cost_nrows = 61
    fix_cost_skiprows = 63
    fix_cost_nrows = 61

    # Configure regions and remove prefix
    regions = [
        r.replace("R11_", "")
        for r in get_nodes(scenario)
        if r not in ["R11_GLB", "World"]
    ]

    for r in regions:
        # Retrieve investment costs xlsx
        df_inv = (
            pd.read_excel(
                data_file,
                sheet_name="{}_{}".format(r, ssp),
                index_col=0,
                usecols=usecols,
                nrows=inv_cost_nrows,
            )
            .reset_index()
            .rename(columns={"inv": "technology"})
            .assign(node_loc="R11_{}".format(r), unit="USD/kWa")
        )

        # Identifies which modelyears are missing in the data and interpolates
        # missing values.
        model_years = [y for y in scenario.set("year") if 2100 >= y >= 1990]
        missing_years = [y for y in model_years if y not in df_inv.columns]
        df_inv, df_inv_tec = add_missing_years(
            df_inv, model_years, missing_years, "year_vtg"
        )

        # Retrieves dataframe of current data in the model
        tmp = (
            scenario.par(
                "inv_cost",
                filters={"node_loc": ["R11_{}".format(r)], "technology": df_inv_tec},
            )
            .assign(unit="USD/kWa")
            .set_index(["node_loc", "technology", "unit", "year_vtg"])
        )

        # Merges new and current model data; all current model
        # data is replaced by available new data
        tmp["value1"] = df_inv["value1"]
        tmp.value = tmp.apply(
            lambda row: row["value1"] if not np.isnan(row["value1"]) else row["value"],
            axis=1,
        )
        tmp = tmp.reset_index().drop("value1", axis=1)
        tmp.value = round(tmp.value, 2)

        scenario.check_out()
        scenario.add_par("inv_cost", tmp)
        scenario.commit("Updated inv_cost for region R11_{}".format(r))

        # Retrieve fix_cost from xlsx
        # NOTE currently the index is set to year_act. This ensures that the
        #      apporach is consistent with vintaging turned off in the model.
        #      Other wise see notes below.
        df_fom = (
            pd.read_excel(
                data_file,
                sheet_name="{}_{}".format(r, ssp),
                skiprows=fix_cost_skiprows,
                index_col=0,
                usecols=usecols,
                nrows=fix_cost_nrows,
            )
            .reset_index()
            .rename(columns={"fom": "technology"})
            .assign(node_loc="R11_{}".format(r), unit="USD/kWa")
        )

        df_fom, df_fom_tec = add_missing_years(
            df_fom, model_years, missing_years, "year_act"
        )
        # NOTE if vintaging "on": switch to year_vtg in prev. line

        tmp = (
            scenario.par(
                "fix_cost",
                filters={"node_loc": ["R11_{}".format(r)], "technology": df_fom_tec},
            )
            .assign(unit="USD/kWa")
            .set_index(["node_loc", "technology", "unit", "year_act"])
        )
        # NOTE if vintaging "on": switch to year_vtg in prev. line
        tmp["value1"] = df_fom["value1"]

        tmp.value = tmp.apply(
            lambda row: row["value1"] if not np.isnan(row["value1"]) else row["value"],
            axis=1,
        )
        tmp = tmp.reset_index().drop("value1", axis=1)
        tmp.value = round(tmp.value, 2)

        scenario.check_out()
        scenario.add_par("fix_cost", tmp)
        scenario.commit("Updated fix_cost for region R11_{}".format(r))
