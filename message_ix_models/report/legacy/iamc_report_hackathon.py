import logging
from pathlib import Path

import pandas as pd
import yaml
from message_ix_models import Context
from message_ix_models.util import MESSAGE_DATA_PATH, private_data_path
from yaml.loader import SafeLoader

import message_data.tools.post_processing.postprocess as postprocess
import message_data.tools.post_processing.pp_utils as pp_utils
from message_data.tools.utilities import (
    get_historical_years,
    get_nodes,
    get_optimization_years,
)

log = logging.getLogger(__name__)


def report(
    mp,
    scen,
    ref_sol=False,
    model_out=None,
    scenario_out=None,
    out_dir=None,
    merge_hist=False,
    merge_ts=False,
    aggr_def=None,
    var_def=None,
    unit_yaml=None,
    run_config=None,
    urban_perc=None,
    kyoto_hist=None,
    lu_hist=None,
    verbose=False,
    *,
    context: Context = None,
):
    """Main reporting function.

    This function will run reporting for specific "tables" as specified in the YAML
    format configuration file given by `run_config`.

    Outputs will be stored as an xlsx file in IAMC format for upload to a scenario
    database/explorer instance.

    Within the `run_config` file, the key ``report_config: / var_def:`` refers to a CSV-
    format file containing a “variable template”. Only variables defined in the variable
    template are reported. All other variables are excluded.

    .. warning:: If extending the variable template, **do not** overwrite the existing
       file as this is used for some global model intercomparison projects.

    Parameters
    ----------

    mp : :class:`ixmp.Platform`
        Database connection where scenario object is located.
    scen : :class:`message_ix.Scenario`
        Scenario object for which reporting should be run.
    ref_sol : boolean (default: False)
        Option whether to process historical results or optimization results.
    model_out : str (default: None)
        Model name of the scenario in the output file.
    scenario_out : str (default: None)
        Scenario name of the scenario in the output file.
    out_dir : str or Path
        Directory in which to write the Excel output file. Default: a directory named
        :file:`…/reporting_output/` within the :mod:`.message_data` source tree, i.e.
        directly below :data:`message_ix_models.util.MESSAGE_DATA_PATH`. :func:`report`
        does not respond to the ``message local data`` configuration key for
        :ref:`local data <message_ix_models:local-data>`.
    merge_hist : boolean (default: False)
        Switch to determine whether the reporting results should be merged with
        already processed historical results, which are then, additionally, stored
        as time series data with the scenario object.
    merge_ts : boolean (default: False)
        Switch to use data stored as TS to overwrite results from reporting.
    var_def : str (default: None)
        Name of file to be used to define allowed variables.
    aggr_def : str (default=None)
        Name of file to be used to define aggregate mapping.
    unit_yaml : str (default: None)
        Directory incl file name of unit conversion factors from model units
        to output units.
    run_config : str (default: None)
        Directory incl file name of which reporting tables are to be run.
    urban_perc : str (default: None)
        Regional urban shares in %.
    kyoto_hist : str (default: None)
        Historic Kyoto Gas emissions excl. land-use emissions for regions.
    lu_hist : str (default: None)
        Historic land-use GHG emissions for regions.
    verbose : str (default: False)
        Option whether to print onscreen messages.
    context : .Context
        Only the ``dry_run`` setting is respected. If :data:`True`, configuration is
        read, but nothing is done.
    """
    nds0 = get_nodes(scen)
    nds = [n for n in nds0 if "|" not in n]
    region_id = list(set([x.split("_")[0] for x in get_nodes(scen)]))[0]

    # --------------------
    # Set global variables
    # --------------------

    model_nm = model_out if model_out else scen.model
    scen_nm = scenario_out if scenario_out else scen.scenario
    run_history = ref_sol
    path = private_data_path() / "report"

    # ----------------------------
    # Read reporting configuration
    # ----------------------------

    # Default config
    with open(path / "default_run_config.yaml") as f:
        config = yaml.load(f, Loader=SafeLoader)

    # Alternative config
    # If an alternative config has been defined, then ONLY
    # those items specified will replace or be added to the
    # default condig
    if run_config:
        with open(path / run_config) as f:
            tmp_config = yaml.load(f, Loader=SafeLoader)
        for i in tmp_config:
            for j in tmp_config[i]:
                if j in config[i]:
                    config[i].pop(j)
                config[i][j] = tmp_config[i][j]

    # Config: Reset path
    if "path" in config["report_config"]:
        path = config["report_config"]["path"]

    # Config: urban/rural share
    # The standard file corresponds to data for R11-SSP2
    # taken from the SSP2-database. Values in %.
    urban_perc_data = (
        path / config["report_config"]["urban_perc"] if not urban_perc else urban_perc
    )

    # Config: historic emissions
    # The standard file corresponds to data for R11-SSP1/2/3
    # taken from the activity of `TCE` less the historic
    # land-use emissions.
    # Historic land-use emissions (CO2) correspond to
    # R11-SSP1/2/3
    if run_history == "True":
        kyoto_hist_data = (
            path / config["report_config"]["kyoto_hist"]
            if not kyoto_hist
            else kyoto_hist
        )

        lu_hist_data = (
            path / config["report_config"]["lu_hist"] if not lu_hist else lu_hist
        )

    # Config: Define unit conversion factors
    unit_yaml = (
        path / config["report_config"]["unit_yaml"] if not unit_yaml else unit_yaml
    )

    # Config: Define which tables to be run
    run_tables = config["run_tables"]

    # Config: Define which variable aggregation should be used
    aggr_def = path / config["report_config"]["aggr_def"] if not aggr_def else aggr_def

    # Config: Define which variable definition should be used
    var_def = path / config["report_config"]["var_def"] if not var_def else var_def

    # Directory for output
    out_dir = Path(out_dir) if out_dir else MESSAGE_DATA_PATH / "reporting_output"

    if context and context.dry_run:
        log.info(f"(DRY RUN) Would write to {out_dir}")
        return

    # --------------------------------
    # Set global variables in pp_utils
    # --------------------------------

    if run_history != "True":
        # Configures reporting tools to retrieve results from optimization (var)
        pp = postprocess.PostProcess(scen)
        pp_utils.firstmodelyear = scen.firstmodelyear

        pp_utils.years = get_optimization_years(scen)
    else:
        # Configures reporting tools to retrieve results from "reference_solution" (par)
        pp = postprocess.PostProcess(scen, ix=False)
        pp_utils.years = get_historical_years(scen) + get_optimization_years(scen)

    # Passes all model years to reporting tools
    pp_utils.all_years = scen.set("year").tolist()
    pp_utils.globalname = "{}_GLB".format(region_id)

    # Provides option to rename model years for output
    regions = {n: (n.split("_")[1] if "GLB" not in n else "World") for n in nds}
    pp_utils.regions = regions
    pp_utils.region_id = region_id
    pp_utils.all_tecs = scen.set("technology")
    pp_utils.model_nm = model_nm
    pp_utils.scen_nm = scen_nm
    pp_utils.verbose = verbose

    # ----------------------------
    # Read in unit conversion file
    # ----------------------------

    with open(unit_yaml) as f:
        data = yaml.load(f, Loader=SafeLoader)
    global mu
    mu = data["model_units"]
    for i in mu:
        try:
            mu[i] = eval(mu[i])
        except Exception:
            continue

    data_cf = data["conversion_factors"]
    for u in data_cf:
        for i in list(data_cf[u].keys()):
            trgt = i
            fnd = 0
            try:
                trgt = eval(trgt)
                fnd = 1
            except Exception:
                trgt = trgt
            if type(data_cf[u][i]) == str:
                data_cf[u][trgt] = eval(data_cf[u][i])
            if fnd == 1:
                data_cf[u].pop(i)

    pp_utils.unit_conversion = data_cf

    # ------------------------
    # Compile reporting tables
    # ------------------------

    # Based on the default config, populate func_dict, which
    # has all the function required for running the reporting.
    DEFAULT_table_def = "message_data.tools.post_processing.default_tables"
    dflt_tbl = __import__(DEFAULT_table_def, fromlist=[None])
    dflt_tbl.pp = pp
    dflt_tbl.mu = mu
    dflt_tbl.run_history = run_history
    dflt_tbl.urban_perc_data = urban_perc_data
    if run_history == "True":
        dflt_tbl.kyoto_hist_data = kyoto_hist_data
        dflt_tbl.lu_hist_data = lu_hist_data

    func_dict = dflt_tbl.return_func_dict()

    if config["report_config"]["table_def"] != DEFAULT_table_def:
        tmp_tbl = __import__(config["report_config"]["table_def"], fromlist=[None])
        log.info(f"Replacement tables from {tmp_tbl!r}:")
        tmp_tbl.pp = pp
        tmp_tbl.mu = mu
        tmp_tbl.run_history = run_history
        tmp_tbl.urban_perc_data = urban_perc_data
        if run_history == "True":
            tmp_tbl.kyoto_hist_data = kyoto_hist_data
            tmp_tbl.lu_hist_data = lu_hist_data

        tmp_func_dict = tmp_tbl.return_func_dict()

        # Replace default functions with alternatives
        for f in tmp_func_dict:
            if f in func_dict:
                func_dict.pop(f)
            log.info(f"{f} → {tmp_func_dict[f]}")
            func_dict[f] = tmp_func_dict[f]

    # --------------------
    # Run reporting tables
    # --------------------

    dfs = {}
    for i in run_tables:
        if run_tables[i]["active"] is True:
            print("processing Table:", run_tables[i]["root"])
            if (
                "condition" in run_tables[i]
                and eval(run_tables[i]["condition"]) is True
            ):
                continue
            dfs[i] = (
                func_dict[run_tables[i]["function"]]()
                if "args" not in run_tables[i]
                else func_dict[run_tables[i]["function"]](**run_tables[i]["args"])
            )

    # ---------------------------------
    # Convert dataframes to IAMC-format
    # ---------------------------------

    if merge_hist or merge_ts:
        # Create mapping for regions
        # {database name: output name}
        # e.g. {"Subsaharan Africa (R11)": "AFR"}
        reg_ts = mp.regions()
        reg_ts = reg_ts[
            reg_ts.region.isin([r for r in scen.set("node") if r != "World"])
        ]
        reg_ts["region"] = reg_ts["region"].str.replace(f"{region_id}_", "")
        reg_ts["region"] = reg_ts["region"].str.replace("GLB", "World")
        reg_ts = (
            reg_ts[["region", "mapped_to"]].set_index("mapped_to").to_dict()["region"]
        )

    mapping = pd.read_csv(aggr_def)
    allowed_var = pd.read_csv(var_def)["Variable"].unique().tolist()
    df = []

    if merge_ts:
        # Retrieve ts
        ts = scen.timeseries()
        if merge_hist:
            ts = ts[ts["year"].isin(get_optimization_years(scen))]
        # Rename for compatibility
        ts = ts.rename(
            columns={
                "model": "Model",
                "scenario": "Scenario",
                "region": "Region",
                "variable": "Variable",
                "unit": "Unit",
            }
        )

        # Convert synonym region names
        ts.Region = ts.Region.map(reg_ts)

        iamc_index = ["Model", "Scenario", "Region", "Variable", "Unit"]
        # Flip from short to long format
        ts = ts.pivot_table(
            index=iamc_index, columns="year", values="value"
        ).reset_index()

    for i in dfs:
        if merge_ts:
            # Filter out timeseries entries which exist for a certain variable
            var = config["run_tables"][i]["root"]
            tmp = ts[ts.Variable.str.contains(var, regex=False)].assign(
                Variable=lambda df: df.Variable.str.replace(f"{var}|", "", regex=False)
            )
            if not tmp.empty:
                dfs[i] = (
                    tmp.set_index(iamc_index)
                    .combine_first(dfs[i].set_index(iamc_index))
                    .reset_index()
                )

            # Remove newly added timeseries from ts dataframe, to avoid double counting
            ts = ts[ts.Variable.str.find(var) < 0]

        if run_tables[i]["root"] == "Emissions|HFC":
            df.append(
                pp_utils.iamc_it(dfs[i], run_tables[i]["root"], mapping, rm_totals=True)
            )
        else:
            df.append(pp_utils.iamc_it(dfs[i], run_tables[i]["root"], mapping))

    df = pd.concat(df, sort=True)

    # --------------
    # Process output
    # --------------

    # Ensure that only variables included in the template are included in the final
    # output
    df = df.loc[df.Variable.isin(allowed_var)]

    # -------------------------------
    # Merge with historical TS values
    # -------------------------------

    if merge_hist:
        ix_upload = df.reset_index()
        ix_upload = ix_upload.drop(["index", "Model", "Scenario"], axis=1)
        ix_upload = ix_upload.rename(
            columns={
                "Region": "region",
                "Variable": "variable",
                "Unit": "unit",
            }
        )
        col_yr = pp_utils.numcols(df)
        model_year = int(
            scen.set("cat_year", {"type_year": ["firstmodelyear"]})["year"]
        )
        ix_regions = {regions[n]: n for n in regions}
        ix_upload.region = ix_upload.region.replace(ix_regions)
        if run_history == "True":
            cols = ["region", "variable", "unit"] + [int(yr) for yr in col_yr]
        else:
            cols = ["region", "variable", "unit"] + [
                int(yr) for yr in col_yr if yr >= model_year
            ]
        ix_upload = ix_upload[cols]

        # ix_mp._jobj.unlockRunid(11473)

        # NB could use scen.transact() here, if it accepted timeseries_only
        scen.check_out(timeseries_only=True)
        print("Starting to upload timeseries")
        print(ix_upload.head())

        ix_upload.to_csv("debug.csv")  # DEBUG

        try:
            scen.add_timeseries(ix_upload)
        except Exception as e:
            print(f"Failed: {repr(e)}")
            scen.discard_changes()  # Don't leave scenario in a locked state
            raise
        else:
            print("Finished uploading timeseries")
            scen.commit("Reporting uploaded as timeseries")

        df = scen.timeseries(iamc=True)
        df = df.rename(
            columns={
                "model": "Model",
                "scenario": "Scenario",
                "region": "Region",
                "variable": "Variable",
                "unit": "Unit",
            }
        )
        df["Model"] = model_nm
        df["Scenario"] = scen_nm

        df.Region = df.Region.map(reg_ts)
        df = df.set_index(
            ["Model", "Scenario", "Region", "Variable", "Unit"]
        ).reset_index()
        if "subannual" in df.columns:
            df = df.drop("subannual", axis=1)

    # Write to an Excel file in the configured output directory
    out_dir.mkdir(parents=True, exist_ok=True)

    pp_utils.write_xlsx(df, out_dir)
