"""MESSAGEix-Buildings model."""
import gc
import subprocess
import sys
from itertools import product
from pathlib import Path
from time import time
from typing import Optional, Tuple

import click
import message_ix
import message_ix_models.util as mutil
import numpy as np
import pandas as pd
from message_ix_models import Context, ScenarioInfo

# from message_data.projects.ngfs.util import add_macro_COVID  # Unused

#: Commodities for the buildings sector.
build_commodities = [
    "resid_floor_construction",  # floorspace to be constructed
    "resid_floor_demolition",  # floorspace to be demolished
    "comm_floor_construction",  # floorspace to be constructed
    "comm_floor_demolition",  # floorspace to be demolished
    # TODO: Need to harmonize on the commodity names (remove the material name)
]

#: Technologies for the buildings sector.
build_techs = [
    # technology providing residential floorspace activity
    "construction_resid_build",
    "demolition_resid_build",
    # technology providing commercial floorspace activity
    "construction_comm_build",
    "demolition_comm_build",
]

#: Commodity names to be converted for use in MESSAGEix-Materials.
build_comm_convert = [
    "resid_mat_int_scrap_steel",
    "resid_mat_int_scrap_aluminum",
    "resid_mat_int_scrap_cement",
    "resid_mat_int_demand_steel",
    "resid_mat_int_demand_aluminum",
    "resid_mat_int_demand_cement",
    "comm_mat_int_scrap_steel",
    "comm_mat_int_scrap_aluminum",
    "comm_mat_int_scrap_cement",
    "comm_mat_int_demand_steel",
    "comm_mat_int_demand_aluminum",
    "comm_mat_int_demand_cement",
]

#: Types of materials.
materials = ["steel", "cement", "aluminum"]

# Columns for indexing demand parameter
nclytu = ["node", "commodity", "level", "year", "time", "unit"]


def get_prices(s: message_ix.Scenario) -> pd.DataFrame:
    """Retrieve PRICE_COMMODITY for certain quantities; excluding _GLB node."""
    result = s.var(
        "PRICE_COMMODITY",
        filters={
            "level": "final",
            "commodity": ["biomass", "coal", "lightoil", "gas", "electr", "d_heat"],
        },
    )
    return result[~result["node"].str.endswith("_GLB")]


def subset_demands(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[
        df["commodity"].isin(
            [
                c
                for c in df["commodity"].unique()
                if ("hotwater" in c) | ("cool" in c) | ("heat" in c)
            ]
        )
    ]


def run_sturm(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Invoke STURM, either using rpy2 or via Rscript.

    Returns
    -------
    pd.DataFrame
        The `sturm_scenarios` data frame.
    pd.DataFrame or None
        The `comm_sturm_scenarios` data frame if `first_iteration` is :obj:`True`;
        otherwise :obj:`None`.
    """
    try:
        import rpy2.situation

        if first_iteration:
            print(*rpy2.situation.iter_info(), sep="\n")

        return _sturm_rpy2(context)
    except ImportError:
        if first_iteration:
            print("rpy2 NOT found")

        return _sturm_rscript(context)


def _sturm_rpy2(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`rpy2`."""
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter

    # Retrieve info from the Context object
    config = context["buildings"]

    # Path to R code
    rcode_path = config["code_dir"].joinpath("STURM_model")

    # Source R code
    r = ro.r
    r.source(str(rcode_path.joinpath("F10_scenario_runs_MESSAGE_2100.R")))

    # Common arguments for invoking STURM
    args = dict(
        run=config["ssp"],
        scenario_name=f"{config['ssp']}_{config['clim_scen']}",
        prices=prices,
        path_rcode=str(rcode_path),
        path_in=str(config["code_dir"].joinpath("STURM_data")),
        path_out=str(config["code_dir"].joinpath("STURM_output")),
        geo_level_report=context.regions,  # Should be R12
    )

    with localconverter(ro.default_converter + pandas2ri.converter):
        # Residential
        sturm_scenarios = r.run_scenario(**args, sector="resid")
        # Commercial
        # NOTE: run only on the first iteration!
        comm_sturm_scenarios = (
            r.run_scenario(**args, sector="comm") if first_iteration else None
        )

    del r
    gc.collect()

    return sturm_scenarios, comm_sturm_scenarios


def _sturm_rscript(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`subprocess` and :program:`Rscript`."""
    # Retrieve info from the Context object
    config = context["buildings"]

    # Prepare input files
    # Temporary directory in the MESSAGE_Buildings directory
    temp_dir = config["code_dir"].joinpath("temp")
    temp_dir.mkdir(exist_ok=True)

    # Write prices to file
    input_path = temp_dir.joinpath("prices.csv")
    prices.to_csv(input_path)

    def run_edited(sector: str) -> pd.DataFrame:
        """Edit the run_STURM.R script, then run it."""
        # Read the script and split lines
        script_path = config["code_dir"].joinpath("run_STURM.R")
        lines = script_path.read_text().split("\n")

        # Replace some lines
        # FIXME(PNK) This is extremely fragile. Instead use a template or regex
        # replacements
        lines[8] = f"ssp_scen <- \"{config['ssp']}\""
        lines[9] = f"clim_scen <- \"{config['clim_scen']}\""
        lines[10] = f'sect <- "{sector}"'

        script_path.write_text("\n".join(lines))

        # Need to supply cwd= because the script uses R's getwd() to find others
        subprocess.check_call(["Rscript", "run_STURM.R"], cwd=config["code_dir"])

        # Read output, then remove the file
        output_path = temp_dir.joinpath(f"{sector}_sturm.csv")
        result = pd.read_csv(output_path)
        output_path.unlink()

        return result

    # Residential
    sturm_scenarios = run_edited(sector="resid")

    # Commercial
    comm_sturm_scenarios = run_edited(sector="comm") if first_iteration else None

    input_path.unlink()
    temp_dir.rmdir()

    return sturm_scenarios, comm_sturm_scenarios


def setup_scenario(
    scenario: message_ix.Scenario,
    info: ScenarioInfo,
    demand: pd.DataFrame,
    prices: pd.DataFrame,
    sturm_scenarios: pd.DataFrame,
    comm_sturm_scenarios: pd.DataFrame,
    first_iteration: bool,
):
    """Set up the structure and data for MESSAGE_Buildings on `scenario`."""
    if build_commodities[0] in info.set["commodity"]:
        # Scenario already set up; do notihing
        return

    from utils import rc_afofi

    nodes = info.N
    years_model = info.Y

    # Add new commodities and technologies
    scenario.add_set("commodity", build_commodities)
    scenario.add_set("technology", build_techs)

    # Find emissions in relation activity
    emiss_rel = list(
        filter(
            lambda rel: "Emission" in rel,
            scenario.par("relation_activity").relation.unique(),
        )
    )

    # Create new demands and techs for AFOFI
    # based on percentages between 2010 and 2015
    # (see rc_afofi.py in utils)
    dd_replace = scenario.par(
        "demand",
        filters={"commodity": ["rc_spec", "rc_therm"], "year": years_model},
    )
    [perc_afofi_therm, perc_afofi_spec] = rc_afofi.return_PERC_AFOFI()
    afofi_dd = dd_replace.copy(True)
    for reg in perc_afofi_therm.index:
        # Boolean mask for rows matching this `reg`
        mask = afofi_dd["node"].str.endswith(reg)

        # NB(PNK) This could probably be simplified using groupby()
        afofi_dd.loc[mask & (afofi_dd.commodity == "rc_therm"), "value"] = (
            afofi_dd.loc[mask & (afofi_dd.commodity == "rc_therm"), "value"]
            * perc_afofi_therm.loc[reg][0]
        )
        afofi_dd.loc[mask & (afofi_dd.commodity == "rc_spec"), "value"] = (
            afofi_dd.loc[mask & (afofi_dd.commodity == "rc_spec"), "value"]
            * perc_afofi_spec.loc[reg][0]
        )

    afofi_dd["commodity"] = afofi_dd.commodity.str.replace("rc", "afofi")
    scenario.add_set("commodity", afofi_dd.commodity.unique())
    scenario.add_par("demand", afofi_dd)

    rc_techs = scenario.par("output", filters={"commodity": ["rc_therm", "rc_spec"]})[
        "technology"
    ].unique()

    for tech_orig in rc_techs:
        tech_new = tech_orig.replace("rc", "afofi")

        if "RC" in tech_orig:
            tech_new = tech_orig.replace("RC", "AFOFI")

        filters = dict(filters={"technology": tech_orig})
        for name in ("input", "capacity_factor", "emission_factor"):
            scenario.add_par(
                name, scenario.par(name, **filter).assign(technology=tech_new)
            )

        afofi_out = scenario.par("output", **filter).assign(
            technology=tech_new,
            commodity=lambda df: df["commodity"].str.replace("rc", "afofi"),
        )

        afofi_rel = scenario.par(
            "relation_activity",
            filters={"technology": tech_orig, "relation": emiss_rel},
        ).assign(technology=tech_new)

        scenario.add_set("technology", tech_new)
        scenario.add_par("output", afofi_out)
        scenario.add_par("relation_activity", afofi_rel)

    # Set model demands for rc_therm and rc_spec to zero
    dd_replace["value"] = 0
    scenario.add_par("demand", dd_replace)

    # Create new input/output for building material intensities
    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
        mode="M1",
        year_vtg=years_model,
        year_act=years_model,
    )

    # Iterate over `build_comm_convert` and  nodes (excluding World and *_GLB)
    for c, n in product(
        build_comm_convert, filter(lambda n: "World" not in n and "GLB" not in n, nodes)
    ):
        comm = c.split("_")[-1]
        typ = c.split("_")[-2]
        rc = c.split("_")[-5]  # "resid" or "comm"

        common.update(node_loc=n, node_origin=n, node_dest=n)

        if rc == "resid":
            df_mat = sturm_scenarios.loc[
                (sturm_scenarios["commodity"] == c) & (sturm_scenarios["node"] == n)
            ]
        elif rc == "comm" and first_iteration:
            df_mat = comm_sturm_scenarios.loc[
                (comm_sturm_scenarios["commodity"] == c)
                & (comm_sturm_scenarios["node"] == n)
            ]

        if typ == "demand":
            tec = f"construction_{rc}_build"
            # Need to take care of 2110 by appending the last value
            df_demand = mutil.make_io(
                (comm, "demand", "t"),
                (f"{rc}_floor_construction", "demand", "t"),
                efficiency=pd.concat([df_mat.value, df_mat.value.tail(1)]),
                technology=tec,
                **common,
            )
            scenario.add_par("input", df_demand["input"])
            scenario.add_par("output", df_demand["output"])
        elif typ == "scrap":
            tec = f"demolition_{rc}_build"
            # Need to take care of 2110 by appending the last value
            df_scrap = mutil.make_io(
                (comm, "end_of_life", "t"),  # will be flipped to output
                (f"{rc}_floor_demolition", "demand", "t"),
                efficiency=pd.concat([df_mat.value, df_mat.value.tail(1)]),
                technology=tec,
                **common,
            )
            # Flip input to output (no input for demolition)
            df_temp = df_scrap["input"].rename(
                columns={"node_origin": "node_dest", "time_origin": "time_dest"}
            )
            scenario.add_par("output", df_temp)
            scenario.add_par("output", df_scrap["output"])

    # Subtract building material demand from existing demands in scenario
    for rc in ["resid", "comm"]:
        # Don't do this for commercial demands in the first iteration
        if rc == "comm" and first_iteration:
            continue

        df_out = (
            sturm_scenarios.copy(True)
            if rc == "resid"
            else comm_sturm_scenarios.copy(True)
        )
        df = df_out[
            df_out.commodity.str.fullmatch(f"{rc}_mat_demand_(cement|steel|aluminum)")
        ]  # .copy(True)
        df["commodity"] = df.apply(lambda x: x.commodity.split("_")[-1], axis=1)
        df = df.rename(columns={"value": f"demand_{rc}_const"}).drop(
            columns=["level", "time", "unit"]
        )
        # df = df.stack()
        mat_demand = (
            scenario.par("demand", {"level": "demand"})
            .join(
                df.set_index(["node", "year", "commodity"]),
                on=["node", "year", "commodity"],
                how="left",
            )
            .dropna()
        )
        mat_demand["value"] = np.maximum(
            mat_demand["value"] - mat_demand[f"demand_{rc}_const"], 0
        )
        scenario.add_par("demand", mat_demand.drop(columns=f"demand_{rc}_const"))

    # Create new technologies for building energy
    rc_tech_fuel = pd.DataFrame(
        {
            "fuel": ["biomass", "coal", "lightoil", "gas", "electr", "d_heat"],
            "technology": [
                "biomass_rc",
                "coal_rc",
                "loil_rc",
                "gas_rc",
                "elec_rc",
                "heat_rc",
            ],
        }
    )

    # Add for fuels above
    for fuel in prices["commodity"].unique():
        # Find the original rc technology for the fuel
        tech_orig = rc_tech_fuel.loc[rc_tech_fuel["fuel"] == fuel, "technology"].values[
            0
        ]

        # Remove lower bound in activity for older, now unused
        # rc techs to allow them to reach zero
        filters = dict(filters={"technology": tech_orig, "year_act": years_model})
        for constraint, value in (
            ("bound_activity", 0.0),
            ("growth_activity", -1.0),
            ("soft_activity", 0.0),
        ):
            name = f"{constraint}_lo"
            scenario.add_par(name, scenario.par(name, **filters).assign(value=value))

        # Create the technologies for the new commodities
        for commodity in filter(
            lambda com: f"_{fuel}" in com or f"-{fuel}" in com,
            demand["commodity"].unique(),
        ):

            # Fix for lightoil gas included
            if "lightoil-gas" in commodity:
                tech_new = fuel + "_lg_" + commodity.replace("_lightoil-gas", "")
            else:
                tech_new = fuel + "_" + commodity.replace("_" + fuel, "")

            filters = dict(filters={"technology": tech_orig})
            build_in = scenario.par("input", **filters).assign(
                technology=tech_new, value=1.0
            )

            build_out = scenario.par("output", **filters).assign(
                technology=tech_new, commodity=commodity, value=1.0
            )

            build_cf = scenario.par("capacity_factor", **filters).assign(
                technology=tech_new
            )

            build_ef = scenario.par("emission_factor", **filters).assign(
                technology=tech_new
            )

            build_rel = scenario.par(
                "relation_activity",
                filters={"technology": tech_orig, "relation": emiss_rel},
            ).assign(technology=tech_new)

            scenario.add_set("commodity", commodity)
            scenario.add_set("technology", tech_new)
            scenario.add_par("input", build_in)
            scenario.add_par("output", build_out)
            scenario.add_par("capacity_factor", build_cf)
            scenario.add_par("emission_factor", build_ef)
            scenario.add_par("relation_activity", build_rel)


@click.command("buildings")
@click.argument("code_dir", type=Path)
@click.pass_obj
def cli(context, code_dir):
    """MESSAGEix-Buildings model.

    The (required) argument CODE_DIR is the path to the MESSAGE_Buildings repo/code.
    """
    config = {
        "code_dir": code_dir.resolve(),
        "run ACCESS": True,
        # Specify SSP and climate scenarios
        "ssp": "SSP2",
        "clim_scen": "BL",
        # "clim_scen": "2C", # Alternate option?
    }
    context["buildings"] = config

    # The MESSAGE_Buildings repo is not an installable Python package. Prepend its
    # location to sys.path so code/modules within it can be imported
    sys.path.append(str(config["code_dir"]))

    # Import from MESSAGE_Buildings
    from utils import add_globiom

    # Load database
    mp = context.get_platform()

    # Add floorspace unit
    mp.add_unit("Mm2/y", "mil. square meters by year")

    # Specify whether to make a new copy from a baseline
    # scenario or load an existing scenario
    clone = 1

    # Specify whether to solve MESSSGE (0) or MESSAGE-MACRO (1)
    solve_macro = 0

    # Specify the scenario to be cloned
    # NOTE: this scenario has the updated GLOBIOM matrix

    # # M -> BM baseline
    # mod_orig = "MESSAGEix-GLOBIOM 1.1-M-R12-NGFS"
    # scen_orig = "baseline"

    # mod_new = "MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS"
    # scen_new = "baseline"

    # BM NPi (after "run_cdlinks_setup" for NPi)
    # This has MACRO but here run MESSAGE only.
    mod_orig = "MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS"
    scen_orig = "NPi2020-con-prim-dir-ncr"

    mod_new = "MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS"
    scen_new = "NPi2020-con-prim-dir-ncr-building"

    scen_to_clone = message_ix.Scenario(mp, mod_orig, scen_orig)

    if clone:
        scenario = scen_to_clone.clone(model=mod_new, scenario=scen_new)
    else:
        scenario = message_ix.Scenario(mp, mod_new, scen_new)

    # Open reference climate scenario if needed
    if context["buildings"]["clim_scen"] == "2C":
        mod_mitig = "ENGAGE_SSP2_v4.1.8"
        scen_mitig = "EN_NPi2020_1000f"
        scen_mitig_prices = message_ix.Scenario(mp, mod_mitig, scen_mitig)

    done = 0
    start_time = time()
    iterations = 0

    old_diff = -1
    # FIXME(PNK) This is not used anywhere. Document its purpose, or comment/remove.
    oscilation = 0

    # Placeholders; replaced on the first iteration
    demand = pd.DataFrame()
    comm_sturm_scenarios = pd.DataFrame()

    # Non-model and model periods
    info = ScenarioInfo(scenario)
    years_not_mod = list(filter(lambda y: y < info.y0, info.set["year"]))

    while done < 1:
        # Get prices from MESSAGE
        # On the first iteration, from the parent scenario; onwards, from the current
        # scenario
        prices = get_prices(scen_to_clone if iterations == 0 else scenario)

        # Save demand from previous iteration for comparison
        if iterations == 0:
            demand_old = pd.DataFrame()
            diff_dd = 1e6
        else:
            demand_old = demand.copy(True)

        # Run ACCESS-E-USE
        if config["run ACCESS"]:
            from E_USE_Model import Simulation_ACCESS_E_USE

            e_use_scenarios = Simulation_ACCESS_E_USE.run_E_USE(
                scenario=config["ssp"], prices=prices
            )
        else:
            # NB(PNK) This will cause the code below to fail. Define and satisfy the
            # minimum conditions for the remaining code.
            e_use_scenarios = pd.DataFrame()

        # Scale results to match historical activity
        # NOTE: ignore biomass, data was always imputed here
        # so we are dealing with guesses over guesses
        e_use_2010 = (
            e_use_scenarios[
                (e_use_scenarios.year == 2010)
                & ~e_use_scenarios.commodity.str.contains("bio")
                & ~e_use_scenarios.commodity.str.contains("non-comm")
            ]
            .groupby("node", as_index=False)
            .sum()
        )
        rc_act_2010 = scen_to_clone.par(
            "historical_activity",
            filters={
                "year_act": 2010,
                "technology": list(
                    filter(
                        lambda t: "rc" in t and "bio" not in t, info.set["technology"]
                    )
                ),
            },
        )
        rc_act_2010 = rc_act_2010.rename(columns={"node_loc": "node"})
        rc_act_2010 = (
            rc_act_2010[["node", "value"]].groupby("node", as_index=False).sum()
        )

        adj_fact = rc_act_2010.copy(True)
        adj_fact["value"] = adj_fact["value"] / e_use_2010["value"]
        adj_fact = adj_fact.rename(columns={"value": "adj_fact"})

        e_use_scenarios = e_use_scenarios.merge(adj_fact, on=["node"])
        e_use_scenarios["value"] = (
            e_use_scenarios["value"] * e_use_scenarios["adj_fact"]
        )
        e_use_scenarios = e_use_scenarios.drop("adj_fact", axis=1)
        e_use_scenarios = e_use_scenarios.loc[e_use_scenarios["year"] > 2010]

        # Run STURM
        sturm_scenarios, css = run_sturm(context, prices, iterations == 0)
        comm_sturm_scenarios = css or comm_sturm_scenarios

        # TEMP: remove commodity "comm_heat_v_no_heat"
        if iterations == 0:
            comm_sturm_scenarios = comm_sturm_scenarios[
                ~comm_sturm_scenarios.commodity.isin(
                    ["comm_heat_v_no_heat", "comm_hotwater_v_no_heat"]
                )
            ]

        # TEMP: remove commodity "resid_heat_v_no_heat"
        sturm_scenarios = sturm_scenarios[
            ~sturm_scenarios.commodity.isin(
                ["resid_heat_v_no_heat", "resid_hotwater_v_no_heat"]
            )
        ]

        # Subset desired energy demands
        demands = [
            e_use_scenarios[~e_use_scenarios.commodity.str.contains("therm")],
            subset_demands(sturm_scenarios),
        ]
        # Add commercial demand in first iteration
        if iterations == 0:
            demands.append(subset_demands(comm_sturm_scenarios))

        # Concatenate 2 or 3 data frames together
        demand = pd.concat(demands)

        # Set energy demand level to useful (although it is final)
        # to be in line with 1 to 1 technologies btw final and useful
        demand["level"] = "useful"

        # Append floorspace demand from sturm_scenarios
        # TODO: Need to harmonize on the commodity names
        # (remove the material names)
        demands = [
            demand,
            sturm_scenarios[
                sturm_scenarios["commodity"].isin(
                    ["resid_floor_construction", "resid_floor_demolition"]
                )
            ],
        ]
        # Add commercial demand in first iteration
        if iterations == 0:
            demands.append(
                comm_sturm_scenarios[
                    comm_sturm_scenarios["commodity"].isin(
                        ["comm_floor_construction", "comm_floor_demolition"]
                    )
                ]
            )
        # Fill with zeros if NaN
        demand = pd.concat(demands).fillna(0)

        # Update demands in the scenario
        if scenario.has_solution():
            scenario.remove_solution()

        scenario.check_out()

        setup_scenario(
            scenario,
            info,
            demand,
            prices,
            sturm_scenarios,
            comm_sturm_scenarios,
            iterations == 0,
        )

        # Rename non-comm
        demand.loc[
            demand["commodity"] == "resid_cook_non-comm", "commodity"
        ] = "non-comm"

        # Fix years (they appear as float)
        demand["year"] = demand["year"].astype(int)

        # Fill missing years...
        # ...with zeroes before model starts
        fill_dd = demand.loc[demand["year"] == scenario.firstmodelyear].copy(True)
        fill_dd["value"] = 0
        for year in years_not_mod:
            fill_dd["year"] = year
            demand = pd.concat([demand, fill_dd])
        # and with the same growth as between 2090 and 2100 for 2110
        dd_2110 = demand.loc[demand["year"] >= 2090].copy(True)
        dd_2110 = dd_2110.pivot(
            index=["node", "commodity", "level", "time", "unit"],
            columns="year",
            values="value",
        ).reset_index()
        dd_2110["value"] = dd_2110[2100] * dd_2110[2100] / dd_2110[2090]
        # unless the demand from 2090 is zero, which creates div by zero
        # in which case take the average (i.e. value for 2100 div by 2)
        # NOTE: no particular reason, just my choice!
        dd_2110.loc[dd_2110[2090] == 0, "value"] = (
            dd_2110.loc[dd_2110[2090] == 0, 2100] / 2
        )
        # or if the demand grows too much indicating a
        # relatively too low value for 2090
        dd_2110.loc[dd_2110["value"] > 3 * dd_2110[2100], "value"] = (
            dd_2110.loc[dd_2110["value"] > 3 * dd_2110[2100], 2100] / 2
        )
        # or simply if there is an NA
        dd_2110.loc[dd_2110["value"].isna(), "value"] = (
            dd_2110.loc[dd_2110["value"].isna(), 2100] / 2
        )

        dd_2110["year"] = 2110
        demand = pd.concat([demand, dd_2110[nclytu + ["value"]]], ignore_index=True)

        # Update demand in scenario
        demand = demand.sort_values(by=["node", "commodity", "year"])
        scenario.add_par("demand", demand)

        # Add tax emissions from mitigation scenario if running a
        # climate scenario and if they are not already there
        if (scenario.par("tax_emission").size == 0) and (config["clim_scen"] != "BL"):
            tax_emission_new = scen_mitig_prices.var("PRICE_EMISSION")
            tax_emission_new.columns = scenario.par("tax_emission").columns
            tax_emission_new["unit"] = "USD/tCO2"
            scenario.add_par("tax_emission", tax_emission_new)

        if "time_relative" not in scenario.set_list():
            scenario.init_set("time_relative")

        # Run MESSAGE
        scenario.commit("buildings test")

        # Add bio backstop
        add_globiom.add_bio_backstop(scenario)

        mod = "MESSAGE-MACRO" if solve_macro else "MESSAGE"

        try:
            # Solve LP with barrier method, faster
            scenario.solve(model=mod, solve_options=dict(lpmethod=4))
        except Exception:
            # Didn't work; try again with dual simplex (the default)
            scenario.solve(model=mod, solve_options=dict(lpmethod=2))

        # Compare prices and see if they converge
        prices_new = get_prices(scenario)

        # Create the DataFrames to keep track of demands and prices
        if iterations == 0:
            price_sav = prices_new.copy(True).drop("lvl", axis=1)
            demand_sav = demand.copy(True).drop("value", axis=1)

        # Compare differences in mean percentage deviation
        diff = prices_new.merge(prices, on=["node", "commodity", "year"])
        diff = diff.loc[diff["year"] != 2110]
        diff["diff"] = (diff["lvl_x"] - diff["lvl_y"]) / (
            0.5 * ((diff["lvl_x"] + diff["lvl_y"]))
        )
        diff = np.mean(abs(diff["diff"]))

        print("Iteration:", iterations)
        print("Mean Percentage Deviation in Prices:", diff)

        # Compare differences in demand after the first iteration
        if iterations > 0:
            diff_dd = demand.merge(demand_old, on=["node", "commodity", "year"])
            diff_dd = diff_dd.loc[diff_dd["year"] != 2110]
            diff_dd["diff_dd"] = (diff_dd["value_x"] - diff_dd["value_y"]) / (
                0.5 * ((diff_dd["value_x"] + diff_dd["value_y"]))
            )
            diff_dd = np.mean(abs(diff_dd["diff_dd"]))
            print("Mean Percentage Deviation in Demand:", diff_dd)

        # Uncomment this on for testing
        # diff = 0.0

        if (diff < 5e-3) or ((iterations > 0) & (diff_dd < 5e-3)):
            done = 1
            print("Converged in ", iterations, " iterations")
            print("Total time:", (time() - start_time) / 3600)
            # scenario.set_as_default()

        if iterations > 10:
            done = 2
            print("Not Converged after 10 iterations!")
            print("Averaging last two demands and running MESSAGE one more time")
            price_sav[f"lvl{iterations}"] = prices_new["lvl"]

            demand_sav = demand_sav.merge(demand, on=nclytu, how="left")
            demand_sav = demand_sav.rename(columns={"value": f"value{iterations}"})
            demand_sav.columns.isin(demand.columns)  # FIXME(PNK) this does nothing
            dd_avg = demand_sav[
                nclytu + [f"value{iterations - 1}", f"value{iterations}"]
            ].copy(True)
            dd_avg["value_avg"] = (
                dd_avg[f"value{iterations - 1}"] + dd_avg[f"value{iterations}"]
            ) / 2
            dd_avg = dd_avg.loc[~dd_avg["value_avg"].isna()]

            demand = demand.merge(dd_avg[nclytu + ["value_avg"]], on=nclytu, how="left")
            demand.loc[~demand["value_avg"].isna(), "value"] = demand.loc[
                ~demand["value_avg"].isna(), "value_avg"
            ]
            demand = demand.drop(columns="value_avg")

            scenario.remove_solution()
            scenario.check_out()
            scenario.add_par("demand", demand)
            scenario.commit("buildings test")
            scenario.solve(model=mod)
            iterations = iterations + 0.5
            prices_new = get_prices(scenario)
            print("Final solution after Averaging last two demands")
            print("Total time:", (time() - start_time) / 3600)

        if abs(old_diff - diff) < 1e-5:
            oscilation = 1  # noqa: F841

        # Keep track of results
        demand_sav = demand_sav.merge(demand, on=nclytu, how="left").rename(
            columns={"value": f"value{iterations}"}
        )
        price_sav[f"lvl{iterations}"] = prices_new["lvl"]
        price_sav.to_csv("price_track.csv")
        demand_sav.to_csv("demand_track.csv")

        iterations = iterations + 1
        old_diff = diff

    # Calibrate MACRO with the outcome of MESSAGE baseline iterations
    # if done and solve_macro==0 and clim_scen=="BL":
    #     sc_macro = add_macro_COVID(scenario, reg="R12", check_converge=False)
    #     sc_macro = sc_macro.clone(scenario = "baseline_DEFAULT")
    #     sc_macro.set_as_default()

    mp.close_db()
