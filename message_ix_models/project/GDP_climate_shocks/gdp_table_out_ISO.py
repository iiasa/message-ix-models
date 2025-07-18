# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 08:41:07 2023

@author: vinca & byers


test waidelich GDP data trajectories
"""

import gc
import time

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyam
import xarray as xr
from dask.diagnostics import ProgressBar
from rime.core import RegionArray  # -*- coding: utf-8 -*-
from rime.core import GMTPathway
from rime.process_config import *
from rime.rime_functions import *
from rime.utils import *

from message_ix_models.util import private_data_path


def run_rime(
    sc_string,  # just scenario without damage model and iter
    dam_mod,
    it,
    wdir,
    pp=50,
):
    """
    Run RIME for a given scenario

    Parameters
    ----------
    sc_string : str
        The name of the scenario.
    dam_mod : str
        The name of the damage model to use.
    it : int
        The iteration number.
    wdir : str
        The working directory.

    Returns
    -------
    agg_gdp_pc_df : pd.DataFrame
        The aggregated regional GDP impacts of climate change.
    """

    # wdir = f"C:\\Users\\vinca\\IIASA\\ECE.prog - GDP_damages\\"
    # for local debuggin TEMP
    # wdir = f"/mnt/c/Users/byers/IIASA/ECE.prog - Documents/Research Theme - NEXUS/GDP_damages/"
    # prefixes = ["Waidelich", "Burke"]
    # prefix = 'Burke'
    if isinstance(dam_mod, str):
        dam_mod = [dam_mod]

    # do we need it? or does ti get it from process_config?
    temp_variable = f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{pp}.0th Percentile"
    years = range(2015, 2101, 5)
    lvaris = 200
    num_workers = 24  # Number of workers. More workers creates more overhead
    region = "COUNTRIES"

    for prefix in dam_mod:
        print(prefix)
        # %% Import scenarios data
        # Exchange for MESSAGE-MAGICC runs
        input_path = private_data_path().parent / "reporting_output" / "magicc_output"
        if it != 0:
            fname_input = f"{sc_string}_{pp}_{prefix}_{it}_magicc.xlsx"
        else:
            fname_input = f"{sc_string}_{it}_magicc.xlsx"

        fname_input_scenarios = input_path / fname_input

        out_file_path = private_data_path().parent / "reporting_output" / "rime_output"

        # if folder "magicc_output" does not exist, create it
        if not out_file_path.exists():
            out_file_path.mkdir()

        df_scens_in = pyam.IamDataFrame(fname_input_scenarios)
        dft = df_scens_in.filter(variable=temp_variable)
        dft = dft.filter(year=years)

        # dft = dft.filter(IMP_marker='non-IMP', keep=False)

        # Replace & fill missing SSP scenario allocation
        # dfp = ssp_helper(dft, ssp_meta_col="Ssp_family", default_ssp="SSP2")
        # dfp = dfp.filter(Ssp_family="SSP2")

        dfp = GMTPathway(dft)

        dft = dfp.df.timeseries()  # .reset_index()
        dft = dft.join(dfp.meta["Ssp_family"]).reset_index()

        for c in dft.columns:
            if isinstance(c, int):
                dft[c] = np.round(dft[c], 2)

        # %% Import GDP data
        start = time.time()
        years = range(2025, 2101, 5)

        # this was in loop before
        ds = xr.open_dataset(f"{wdir}GDP_{prefix}_data/GDP_pre_processed_{prefix}.nc")
        ds = ds.sel(year=years)
        if "gmt" in ds.coords:
            ds = ds.rename({"gmt": "gwl"})

        varis = list(ds.data_vars.keys())[:lvaris]
        dsi = ds[varis]
        print(f"# of variables = {len(varis)}")
        dsi = RegionArray(dsi)
        # % Start processing

        year_res = 5
        parallel = False
        if parallel:
            """
            For parallel processing, convert dft as a wide IAMC pd.Dataframe
            into a dask.DataFrame.
            """
            ddf = dd.from_pandas(dft, npartitions=1000)

            # dfx = dft.iloc[0].squeeze()  # FOR DEBUIGGING THE FUNCTION
            outd = ddf.apply(
                table_impacts_gwl,
                dsi=dsi,
                ssp_meta_col="Ssp_family",
                axis=1,
                meta=("result", None),
            )

            with ProgressBar():
                # try:
                df_new = outd.compute(num_workers=num_workers)
        else:
            df_new = dft.apply(table_impacts_gwl, dsi=dsi, axis=1)

        expandeddGWL = pd.concat([df_new[x] for x in df_new.index])
        print(f" Done:  {time.time() - start}")

        filename = f"{out_file_path}/RIME_out_{region}_{year_res}yr_{sc_string}_{pp}_{prefix}_{it}.csv"

        expandeddGWL.to_csv(filename, encoding="utf-8", index=False)
        print(f" Saved: {region} yrs={year_res}\n  {time.time() - start}")
        print(f"{len(dsi.dataset.data_vars)} variables, {len(dfp.meta)} scenarios")
        del ds, dsi, df_new, expandeddGWL
        gc.collect()

    time.time() - start
