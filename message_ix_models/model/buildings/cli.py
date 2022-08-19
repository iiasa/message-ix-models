"""MESSAGEix-Buildings model."""
import logging
import sys
from itertools import count
from pathlib import Path

import click
import message_ix
import numpy as np
import pandas as pd
from message_ix_models import Context, ScenarioInfo
from message_ix_models.util import local_data_path
from message_ix_models.util._logging import mark_time
from message_ix_models.util.click import common_params

from . import add_bio_backstop, get_prices, setup_scenario
from .sturm import run_sturm

log = logging.getLogger(__name__)

#: Default values for Context["buildings"] keys that configure the code. See
#: :doc:`model/buildings` for a full explanation.
DEFAULTS = {
    "clim_scen": "BL",  # or "2C"?
    "clone": True,
    "max_iterations": 10,
    "run ACCESS": False,
    "solve_macro": False,
    "ssp": "SSP2",
}

# Columns for indexing demand parameter
nclytu = ["node", "commodity", "level", "year", "time", "unit"]


@click.group("buildings")
@click.argument("code_dir", type=Path)
@click.pass_obj
def cli(context, code_dir):
    """MESSAGEix-Buildings model.

    The (required) argument CODE_DIR is the path to the MESSAGE_Buildings repo/code.
    """
    # Handle configuration
    config = DEFAULTS.copy()
    config.update(code_dir=code_dir.resolve())
    context["buildings"] = config


# FIXME(PNK) Too complex; McCabe complexity of 25 > 14 for the rest of message_data
@cli.command("build-solve")
@common_params("dest")
@click.option(
    "--climate-scen", help="Model/scenario name of reference climate scenario"
)
@click.option("--run-access", is_flag=True, help="Run the ACCESS model.")
@click.option(
    "--sturm",
    "sturm_method",
    type=click.Choice(["rpy2", "Rscript"]),
    default="Rscript",
    help="Method to invoke STURM.",
)
@click.pass_obj
def build_and_solve(  # noqa: C901
    context: Context,
    climate_scen: str,
    run_access: bool,
    sturm_method: str,
    dest: str,
) -> None:
    """Build and solve the model."""
    mark_time()

    # Update configuration
    config = context["buildings"]
    config["run ACCESS"] = run_access
    config["sturm_method"] = sturm_method

    # The MESSAGE_Buildings repo is not an installable Python package. Add its location
    # to sys.path so code/modules within it can be imported. This must go first, as the
    # directory (=module) name "utils" is commonly used and can clash with those from
    # other installed packages.
    # TODO properly package MESSAGE_Buildings so this is not necessary
    sys.path.insert(0, str(config["code_dir"]))

    # Base path for output during iterations
    output_path = context.get_local_path("buildings")

    # Either clone the base scenario to dest_scenario, or load an existing scenario
    if config["clone"]:
        scenario = context.clone_to_dest(create=False)
        # Also retrieve the base scenario
        scen_to_clone = context.get_scenario()
    else:
        # NB(PNK) Can this ever work? scen_to_clone seems to be used below on the first
        # iteration
        scenario = context.get_scenario()

    # Store a reference to the platform
    mp = scenario.platform

    # Update the configuration if --climate-scen was given on the command line
    if climate_scen is not None:
        config["clim_scen"] = "2C"

    # Open reference climate scenario if needed
    if config["clim_scen"] == "2C":
        mod_mitig, scen_mitig = climate_scen.split("/")
        scen_mitig_prices = message_ix.Scenario(mp, mod_mitig, scen_mitig)

    # Loop variables
    done = False
    oscilation = False
    old_diff = -1
    diff_dd = 1e6

    mark_time()

    # Placeholders; replaced on the first iteration
    demand = pd.DataFrame()
    comm_sturm_scenarios = pd.DataFrame()
    # NB(PNK) Unless ACCESS is run, this will cause the code below to fail. Define and
    # satisfy the minimum conditions for the remaining code.
    e_use_scenarios = pd.DataFrame()

    # Non-model and model periods
    info = ScenarioInfo(scenario)
    years_not_mod = list(filter(lambda y: y < info.y0, info.set["year"]))

    for iterations in count():
        # Get prices from MESSAGE
        # On the first iteration, from the parent scenario; onwards, from the current
        # scenario
        price_cache_path = local_data_path("cache", "buildings-prices.csv")
        if iterations == 0:
            if config["run ACCESS"]:
                prices = get_prices(scen_to_clone)

                # Update the cache
                prices.to_csv(price_cache_path)
            else:
                # Read prices from cache
                prices = pd.read_csv(price_cache_path)
        else:
            prices = get_prices(scenario)

        # Save demand from previous iteration for comparison
        demand_old = demand.copy(True)

        # Path to cache ACCESS_E_USE outputs
        access_cache_path = local_data_path("cache", "buildings-access.csv")

        # Run ACCESS-E-USE
        if config["run ACCESS"]:
            from E_USE_Model import Simulation_ACCESS_E_USE  # type: ignore

            e_use_scenarios = Simulation_ACCESS_E_USE.run_E_USE(
                scenario=config["ssp"], prices=prices, base_path=config["code_dir"]
            )

            mark_time()

            # Update cached output
            e_use_scenarios.to_csv(access_cache_path)
        else:
            # Read the cache
            e_use_scenarios = pd.read_csv(access_cache_path)

        # Scale results to match historical activity
        # NB ignore biomass, data was always imputed here so we are dealing with
        #    guesses over guesses
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
        if css is not None:
            comm_sturm_scenarios = css

        mark_time()

        # TEMP: remove commodity "comm_heat_v_no_heat"
        if iterations == 0:
            comm_sturm_scenarios = comm_sturm_scenarios[
                ~comm_sturm_scenarios.commodity.str.fullmatch(
                    "comm_(heat|hotwater)_v_no_heat"
                )
            ]

        # TEMP: remove commodity "resid_heat_v_no_heat"
        sturm_scenarios = sturm_scenarios[
            ~sturm_scenarios.commodity.str.fullmatch("resid_(heat|hotwater)_v_no_heat")
        ]

        # Subset desired energy demands
        expr = "(cool|heat|hotwater)"
        demands = [
            e_use_scenarios[~e_use_scenarios.commodity.str.contains("therm")],
            sturm_scenarios[sturm_scenarios.commodity.str.match(expr)],
        ]
        # Add commercial demand in first iteration
        if iterations == 0:
            demands.append(
                comm_sturm_scenarios[comm_sturm_scenarios.commodity.str.match(expr)]
            )

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
                sturm_scenarios.commodity.str.fullmatch(
                    "resid_floor_(construc|demoli)tion"
                )
            ],
        ]
        # Add commercial demand in first iteration
        if iterations == 0:
            demands.append(
                comm_sturm_scenarios[
                    comm_sturm_scenarios.commodity.str.fullmatch(
                        "comm_floor_(construc|demoli)tion"
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

        mark_time()

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
        add_bio_backstop(scenario)

        mod = "MESSAGE-MACRO" if config["solve_macro"] else "MESSAGE"

        try:
            # Solve LP with barrier method, faster
            scenario.solve(model=mod, solve_options=dict(lpmethod=4))
        except Exception:
            # Didn't work; try again with dual simplex (the default)
            scenario.solve(model=mod, solve_options=dict(lpmethod=2))

        mark_time()

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

        print(f"Iteration: {iterations}\nMean Percentage Deviation in Prices: {diff}")

        if config["max_iterations"] == 1 == iterations + 1:
            # Once-through mode, e.g. for EF China / "MESSAGE-BUILDINGS-STURM config"
            done = True

        # Compare differences in demand after the first iteration
        if iterations > 0:
            _dd = demand.merge(demand_old, on=["node", "commodity", "year"])
            _dd = _dd.loc[_dd["year"] != 2110]
            _dd["diff_dd"] = (_dd["value_x"] - _dd["value_y"]) / (
                0.5 * (_dd["value_x"] + _dd["value_y"])
            )
            diff_dd = np.mean(abs(_dd["diff_dd"]))
            print(f"Mean Percentage Deviation in Demand: {diff_dd}")

        # Uncomment this on for testing
        # diff = 0.0

        if (diff < 5e-3) or ((iterations > 0) & (diff_dd < 5e-3)):
            done = True
            print("Converged in ", iterations, " iterations")
            # scenario.set_as_default()

        if iterations > config["max_iterations"]:
            done = True
            print(f"Not converged after {config['max_iterations']} iterations!")
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

            prices_new = get_prices(scenario)
            print("Final solution after averaging last two demands")

        if abs(old_diff - diff) < 1e-5:
            # FIXME(PNK) This is not used anywhere. What is it for?
            oscilation = True  # noqa: F841

        # Keep track of results
        demand_sav = demand_sav.merge(demand, on=nclytu, how="left").rename(
            columns={"value": f"value{iterations}"}
        )
        price_sav[f"lvl{iterations}"] = prices_new["lvl"]

        # Ensure the parent directory exists
        output_path.mkdir(exist_ok=True)
        # Write to file
        price_sav.to_csv(output_path / "price-track.csv")
        demand_sav.to_csv(output_path / "demand_track.csv")

        # After all post-solve steps
        mark_time()

        if done:
            break

        old_diff = diff

    # Calibrate MACRO with the outcome of MESSAGE baseline iterations
    # if done and solve_macro==0 and clim_scen=="BL":
    #     sc_macro = add_macro_COVID(scenario, reg="R12", check_converge=False)
    #     sc_macro = sc_macro.clone(scenario = "baseline_DEFAULT")
    #     sc_macro.set_as_default()

    mp.close_db()
