# -*- coding: utf-8 -*-
import gc
import time

import numpy as np
import pandas as pd
import pyam
import xarray as xr
from rime.core import RegionArray  # -*- coding: utf-8 -*-
from rime.core import GMTPathway

# from rime.process_config import *
from rime.rime_functions import table_impacts_gwl

from message_ix_models.util import private_data_path


# from rime.utils import *
def run_rime_pre(sc_string, dam_mod, it, wdir, pp=50):
    if isinstance(dam_mod, str):
        dam_mod = [dam_mod]
    print(f"damage model(s) {dam_mod}")
    if "ensemble" in dam_mod:
        safe_models_ensemble = ["Waidelich", "Burke", "Nath", "Nath-pers"]
        run_rime(sc_string, safe_models_ensemble, it, wdir, pp, ensemble=True)

        out_file_path = private_data_path().parent / "reporting_output" / "rime_output"
        region = "COUNTRIES"
        year_res = 5

        model_dfs = []
        for model in safe_models_ensemble:
            filename = (
                out_file_path
                / f"RIME_out_{region}_{year_res}yr_{sc_string}_{pp}_{model}_{it}.csv"
            )
            if filename.exists():
                model_dfs.append((model, pd.read_csv(filename, dtype=str)))
            else:
                print(f"Warning: missing ensemble output file {filename}")

        if not model_dfs:
            raise FileNotFoundError("No RIME ensemble output files were found.")

        _, avg_df = model_dfs[0]
        avg_df = avg_df.copy()

        # Determine year columns from the first model file
        year_cols = [col for col in avg_df.columns if col.isdigit()]
        if not year_cols:
            year_cols = [
                col
                for col in avg_df.columns
                if col not in ["model", "scenario", "region", "variable", "unit"]
            ]

        merge_keys = [col for col in avg_df.columns if col not in year_cols]

        for model, model_df in model_dfs[1:]:
            avg_df = avg_df.merge(
                model_df, on=merge_keys, how="left", suffixes=("", f"_{model}")
            )

        for year in year_cols:
            merge_cols = [year] + [f"{year}_{model}" for model, _ in model_dfs[1:]]
            existing_cols = [col for col in merge_cols if col in avg_df.columns]
            avg_df[year] = avg_df[existing_cols].astype(float).mean(axis=1, skipna=True)

        # Drop the merged per-model year columns, keeping Waidelich-based keys and averaged year values
        drop_cols = [
            col
            for col in avg_df.columns
            if any(col.endswith(f"_{model}") for model, _ in model_dfs[1:])
        ]
        avg_df = avg_df.drop(columns=drop_cols)

        avg_df["model"] = "ensemble"
        ensemble_filename = (
            out_file_path
            / f"RIME_out_{region}_{year_res}yr_{sc_string}_{pp}_ensemble_{it}.csv"
        )
        avg_df.to_csv(ensemble_filename, encoding="utf-8", index=False)
        print(f"Saved ensemble average output: {ensemble_filename}")
    else:
        run_rime(sc_string, dam_mod, it, wdir, pp, ensemble=False)


def run_rime(
    sc_string,  # just scenario without damage model and iter
    dam_mod,
    it,
    wdir,
    pp=50,
    ensemble=False,
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

    # for local debuggin TEMP
    # wdir = f"C:\\Users\\vinca\\IIASA\\ECE.prog - GDP_damages\\"
    # prefixes = ["Waidelich", "Burke"]

    # do we need it? or does ti get it from process_config?
    temp_variable = (
        f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{pp}.0th"
        " Percentile"
    )
    years = range(2015, 2101, 5)
    lvaris = 200
    region = "COUNTRIES"

    out_file_path = private_data_path().parent / "reporting_output" / "rime_output"

    # if folder "rime_output" does not exist, create it
    if not out_file_path.exists():
        out_file_path.mkdir(parents=True, exist_ok=True)

    for prefix in dam_mod:
        print(prefix)
        if ensemble:
            dam_mod_magicc = "ensemble"
        else:
            dam_mod_magicc = prefix
        # %% Import scenarios data
        # Exchange for MESSAGE-MAGICC runs
        input_path = private_data_path().parent / "reporting_output" / "magicc_output"
        if it != 0:
            fname_input = f"{sc_string}_{pp}_{dam_mod_magicc}_{it}_magicc.xlsx"
        else:
            fname_input = f"{sc_string}_{it}_magicc.xlsx"

        fname_input_scenarios = input_path / fname_input

        df_scens_in = pyam.IamDataFrame(fname_input_scenarios)
        dft = df_scens_in.filter(variable=temp_variable)
        dft = dft.filter(year=years)

        # dft = dft.filter(IMP_marker='non-IMP', keep=False)

        # Replace & fill missing SSP scenario allocation
        # dfp = ssp_helper(dft, ssp_meta_col="Ssp_family", default_ssp="SSP2")
        # dfp = dfp.filter(Ssp_family="SSP2")

        dfp = GMTPathway(dft)  # automatically set SSP2 scenario

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
        df_new = dft.apply(table_impacts_gwl, dsi=dsi, axis=1)

        expandeddGWL = pd.concat([df_new[x] for x in df_new.index])
        print(f" Done:  {time.time() - start}")

        filename = (
            f"{out_file_path}/RIME_out_{region}_{year_res}yr_"
            f"{sc_string}_{pp}_{prefix}_{it}.csv"
        )

        expandeddGWL.to_csv(filename, encoding="utf-8", index=False)
        print(f" Saved: {region} yrs={year_res}\n  {time.time() - start}")
        print(f"{len(dsi.dataset.data_vars)} variables, {len(dfp.meta)} scenarios")
        del ds, dsi, df_new, expandeddGWL
        gc.collect()
