"""MESSAGEix-Buildings and related models.

This module currently includes the main algorithm for iterating between the models
ACCESS and STURM and MESSAGEix itself.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, cast

import ixmp
import message_ix
import numpy as np
import pandas as pd
from message_ix import Scenario, make_df

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.workflow import Config as SolveConfig
from message_ix_models.util import identify_nodes, local_data_path
from message_ix_models.util._logging import mark_time

from . import build, sturm
from .build import get_prices

log = logging.getLogger(__name__)


def _code_dir_factory() -> Path:
    """Return the default value for :attr:`.Config.code_dir`.

    In order of precedence:

    1. The directory where :mod:`message_ix_buildings` is installed.
    2. The :mod:`ixmp` configuration key ``message buildings dir``, if set. The older,
       private MESSAGE_Buildings repository is not an installable Python package, so it
       cannot be imported without information on its location.

       This key can be set in the local :ref:`ixmp configuration file
       <ixmp:configuration>`.
    3. A directory named :file:`./buildings` in the parent of the directory containing
       :mod:`message_ix_models`.
    """
    from importlib.util import find_spec

    from message_ix_models.util import MESSAGE_MODELS_PATH

    if spec := find_spec("message_ix_buildings"):
        assert spec.origin is not None
        return Path(spec.origin).parent

    try:
        return Path(ixmp.config.get("message buildings dir")).expanduser().resolve()
    except AttributeError:
        pass  # Not set

    return MESSAGE_MODELS_PATH.parents[1].joinpath("buildings")


@dataclass
class Config:
    """Configuration options for :mod:`.buildings` code.

    The code responds to values set on an instance of this class.

    Raises
    ------
    FileNotFoundError
        if :attr:`code_dir` points to a non-existent directory.
    """

    #: Name or ID of STURM scenario to run.
    sturm_scenario: str

    #: Climate scenario. Either `BL` or `2C`.
    climate_scenario: str = "BL"

    #: :obj:`True` if the base scenario should be cloned.
    clone: bool = False

    #: Path to the MESSAGEix-Buildings code and data.
    #:
    #: If not set explicitly, this is populated using :func:`_code_dir_factory`.
    code_dir: Path = field(default_factory=_code_dir_factory)

    #: Maximum number of iterations of the ACCESS–STURM–MESSAGE loop. Set to 1 for
    #: once-through mode.
    max_iterations: int = 0

    #: :obj:`True` if the MESSAGEix-Materials + MESSAGEix-Buildings combination is
    #: active
    with_materials: bool = True

    #: Path for STURM output.
    _output_path: Optional[Path] = None

    #: Run the ACCESS model on every iteration.
    run_access: bool = False

    #: Keyword arguments for :meth:`.message_ix.Scenario.solve`. Set
    #: `model="MESSAGE_MACRO"` to solve scenarios using MESSAGE_MACRO.
    solve: dict[str, Any] = field(default_factory=lambda: dict(model="MESSAGE"))

    #: Similar to `solve`, but using another config class
    solve_config: SolveConfig = field(
        default_factory=lambda: SolveConfig(
            solve=dict(model="MESSAGE"), reserve_margin=False
        )
    )

    #: .. todo:: Document the meaning of this setting.
    ssp: str = "SSP2"

    #: Method for running STURM. See :func:`.sturm.run`.
    sturm_method: str = "Rscript"

    def __post_init__(self):
        if not self.code_dir.exists():
            raise FileNotFoundError(f"MESSAGEix-Buildings not found at {self.code_dir}")

    def set_output_path(self, context: Context):
        # Base path for output during iterations
        self._output_path = context.get_local_path("buildings")
        self._output_path.mkdir(parents=True, exist_ok=True)


# Columns for indexing demand parameter
nclytu = ["node", "commodity", "level", "year", "time", "unit"]


def build_and_solve(context: Context) -> Scenario:
    """Build MESSAGEix-Buildings and solve."""
    config = context.buildings

    config.set_output_path(context)

    # Data storage across iterations
    data = dict(
        converged=False,  # True if convergence reached at/before max_iterations
        oscilation=False,  # TODO clarify what this is intended for
        diff_log=list(),  # Log of price mean percent deviation, latest first
        demand=pd.DataFrame(),  # Replaced by pre_solve()
    )

    # Either clone the base scenario to dest_scenario, or load an existing scenario
    if config.clone:
        scenario = context.clone_to_dest(create=False)
        s_ref = context.get_scenario()
    else:
        scenario = context.get_scenario()
        s_ref = scenario

    # Structure information about `scenario`
    info = ScenarioInfo(scenario)
    data.update(
        info=info,
        # Non-model periods
        years_not_mod=list(filter(lambda y: y < info.y0, info.set["year"])),
    )

    # Retrieve data from the base scenario
    data.update(
        # Reference price data from the base scenario
        price_ref=get_prices(s_ref),
        # Reference historical_activity data from the base scenario
        rc_act_2010=s_ref.par(
            "historical_activity",
            filters=dict(
                year_act=2010,
                technology=list(
                    filter(
                        lambda t: "rc" in t and "bio" not in t,
                        info.set["technology"],
                    )
                ),
            ),
        )
        .rename(columns={"node_loc": "node"})[["node", "value"]]
        .groupby("node", as_index=False)
        .sum(),
    )

    # Store a reference to the platform
    mp = scenario.platform

    context.model.regions = identify_nodes(scenario)

    # Retrieve data from climate scenario if needed
    if config.climate_scenario == "2C":
        # FIXME(PNK) this statement doesn't appear to make sense given the advertised
        # possible values for this option
        mod_mitig, scen_mitig = config.climate_scenario.split("/")
        s_climate = message_ix.Scenario(mp, mod_mitig, scen_mitig)

        data.update(PRICE_EMISSION_ref=s_climate.var("PRICE_EMISSION"))

        del s_climate

    # Scenario.solve(…, callback=…) does not run the callback for the first time until
    # *after* MESSAGE(-MACRO) has solved, while the MESSAGE_Buildings linkage requires
    # that ACCESS (maybe) and STURM are run *before* MESSAGE. Thus invoke pre_solve()
    # explicitly with iterations=0.
    mark_time()
    data.update(iterations=0)
    pre_solve(scenario, context, data)

    mark_time()

    def _callback(s):
        """Post-MESSAGE callback for :meth:`.Scenario.solve`."""
        # First run the convergence checks and other post-solve steps
        data.update(iterations=s.iteration)
        done = post_solve(s, context, data)

        if done:  # Convergence achieved or iteration limit reached
            return True

        # Prepare for next iteration: run the pre-solve steps, including ACCESS (maybe)
        # and STURM, before the next solve of MESSAGE
        pre_solve(s, context, data)

    # Start (possibly iterated) solution
    scenario.solve(callback=_callback, **config.solve)

    # Handle non-convergence
    if not data["converged"] and data["iterations"] == config.max_iterations > 1:
        # NB(PNK) use of cast() here may indicate that data should be a typed dataclass
        i = cast(int, data["iterations"])
        log.info(f"Not converged after {i} iterations!")
        log.info("Averaging last two demands and running MESSAGE one more time")

        dd_avg = (
            cast(pd.DataFrame, data["demand_log"])
            .assign(value_avg=lambda df: (df[f"value{i - 1}"] + df[f"value{i}"]) / 2)
            .dropna(subset="value_avg")
        )
        demand = cast(pd.DataFrame, data["demand"]).merge(
            dd_avg[nclytu + ["value_avg"]], on=nclytu, how="left"
        )
        # FIXME can likely use .where() or other pandas built-ins here
        demand.loc[~demand["value_avg"].isna(), "value"] = demand.loc[
            ~demand["value_avg"].isna(), "value_avg"
        ]
        demand = demand.drop(columns="value_avg")

        scenario.remove_solution()
        scenario.check_out()
        scenario.add_par("demand", demand)
        scenario.commit("buildings test")
        scenario.solve(**config.solve)

        log.info("Final solution after averaging last two demands")
        log_data(config, data, demand, get_prices(scenario), i + 1)

    # Calibrate MACRO with the outcome of MESSAGE baseline iterations
    # if done and solve_macro==0 and climate_scenario=="BL":
    #     sc_macro = add_macro_COVID(scenario, reg="R12", check_converge=False)
    #     sc_macro = sc_macro.clone(scenario = "baseline_DEFAULT")
    #     sc_macro.set_as_default()

    return scenario


def pre_solve(scenario: Scenario, context, data):
    """Pre-solve portion of the ACCESS-STURM-MESSAGE loop.

    - (optionally) Run ACCESS.
    - Run STURM.
    - Call :func:`.buildings.build.main`.
    - Update the ``demand`` parameter of `scenario`.
    """
    config = context.buildings
    first_iteration = data["iterations"] == 0

    # Get prices from MESSAGE
    # On the first iteration, from the parent scenario; onwards, from the current
    # scenario
    price_cache_path = local_data_path("cache", "buildings-prices.csv")
    if first_iteration:
        try:
            # Read prices from cache
            prices = pd.read_csv(price_cache_path)
        except FileNotFoundError:
            # Cache doesn't exist; use reference values from the base scenario
            prices = data["price_ref"]

        if config.run_access:
            # Force use of price_ref in this case
            prices = data["price_ref"]

        # Update the cache
        prices.to_csv(price_cache_path)
    else:
        # Get updated prices directly from `scenario`
        prices = get_prices(scenario)

    # Save demand from previous iteration for comparison
    data["demand_old"] = data["demand"].copy(True)

    # Path to cache ACCESS_E_USE outputs
    access_cache_path = local_data_path("cache", "buildings-access.csv")

    # Run ACCESS-E-USE
    if config.run_access:
        from E_USE_Model import Simulation_ACCESS_E_USE  # type: ignore

        e_use = Simulation_ACCESS_E_USE.run_E_USE(
            scenario=config.ssp,
            prices=prices,
            base_path=config.code_dir,
            full_access=False,
            reporting=False,
        )

        mark_time()

        # Scale ACCESS results to match historical activity
        # NB ignore biomass, data was always imputed here so we are dealing with guesses
        #    over guesses
        e_use_2010 = (
            e_use[(e_use.year == 2010) & ~e_use.commodity.str.contains("bio|non-comm")]
            .groupby("node", as_index=False)
            .sum()
        )
        adj_fact = data["rc_act_2010"].copy(True)
        adj_fact["adj_fact"] = adj_fact["value"] / e_use_2010["value"]
        adj_fact = adj_fact.rename(columns={"value": "adj_fact"})
        e_use = (
            e_use.merge(adj_fact.drop("value"), on=["node"])
            .eval("value = value * adj_fact")
            .drop("adj_fact", axis=1)
            .query("year > 2010")
        )

        # Update cached output
        e_use.to_csv(access_cache_path)
    else:
        # Read the cache
        e_use = pd.read_csv(access_cache_path)

    # Run STURM. If first_iteration is False, sturm_c will be empty.
    sturm_r, sturm_c = sturm.run(context, prices, first_iteration)

    mark_time()

    # TODO describe why this is necessary, and why it should be temporary
    # TEMP: remove commodity "(comm|resid)_heat_v_no_heat"
    expr = "(comm|resid)_(heat|hotwater)_v_no_heat"
    sturm_r = sturm_r[~sturm_r.commodity.str.fullmatch(expr)]
    sturm_c = sturm_c[~sturm_c.commodity.str.fullmatch(expr)]

    # - Subset desired energy demands. sturm_c is empty after the first iteration, so
    #   will contribute no rows. This expression identifies only the end-uses modeled by
    #   STURM.
    # - Concatenate.
    # - Set energy demand level to useful (although it is final) to be in line with 1 to
    #   1 technologies btw final and useful.
    expr = "cool|heat|hotwater|other_uses"
    demand = pd.concat(
        [
            e_use[~e_use.commodity.str.contains("therm")],
            sturm_r[sturm_r.commodity.str.contains(expr)],
            sturm_c[sturm_c.commodity.str.contains(expr)],
        ]
    ).assign(level="useful")

    # - Append floorspace demand from STURM.
    #   TODO Need to harmonize on the commodity names (remove the material names)
    # - Fill NaNs with zeros.
    expr = "(comm|resid)_floor_(construc|demoli)tion"
    demand = pd.concat(
        [
            demand,
            sturm_r[sturm_r.commodity.str.fullmatch(expr)],
            sturm_c[sturm_c.commodity.str.fullmatch(expr)],
        ]
    ).fillna(0)

    # Dump data for debugging
    demand.to_csv(config._output_path.joinpath("debug-demand.csv"))

    # Set up structure and apply one-time data modifications to the scenario
    if scenario.has_solution():
        scenario.remove_solution()

    build.main(context, scenario, demand, prices, sturm_r, sturm_c)

    mark_time()

    # Update demands in the scenario
    # NB here we would prefer to also use transfer_demands() in the case of tight policy
    #    constraints, to copy the DEMAND variable from a previous MACRO iteration to
    #    the demand parameter of `scenario`. However, that function and the code below
    #    would likely interfere and produce unintended outputs.
    # TODO create and test code to merge the two

    # - Rename non-comm.
    # - Ensure years are integers.
    demand = demand.replace("resid_cook_non-comm", "non-comm").astype({"year": int})

    # Fill missing years…
    # …with zeroes before model starts
    fill_dd = demand.loc[demand["year"] == scenario.firstmodelyear].assign(value=0)
    for year in data["years_not_mod"]:
        fill_dd["year"] = year
        demand = pd.concat([demand, fill_dd])
    # and with the same growth as between 2090 and 2100 for 2110
    dd_2110 = (
        demand.query("year >= 2090")
        .pivot(
            index=["node", "commodity", "level", "time", "unit"],
            columns="year",
            values="value",
        )
        .reset_index()
        .assign(value=lambda df: df[2100] * df[2100] / df[2090])
    )
    # …unless the demand from 2090 is zero, which creates div by zero in which case take
    # the average (i.e. value for 2100 div by 2)
    # NOTE: no particular reason, just my choice!
    dd_2110.loc[dd_2110[2090] == 0, "value"] = dd_2110.loc[dd_2110[2090] == 0, 2100] / 2
    # …or if the demand grows too much indicating a relatively too low value for 2090
    dd_2110.loc[dd_2110["value"] > 3 * dd_2110[2100], "value"] = (
        dd_2110.loc[dd_2110["value"] > 3 * dd_2110[2100], 2100] / 2
    )
    # …or simply if there is an NA
    dd_2110.loc[dd_2110["value"].isna(), "value"] = (
        dd_2110.loc[dd_2110["value"].isna(), 2100] / 2
    )

    dd_2110["year"] = 2110
    demand = pd.concat(
        [demand, dd_2110[nclytu + ["value"]]], ignore_index=True
    ).sort_values(by=["node", "commodity", "year"])

    # Add tax emissions from mitigation scenario if running a climate scenario and if
    # they are not already there
    #
    # TODO this seems to mostly duplicate the behaviour of
    #      .engage.workflow.Config.tax_emission_scenario → deduplicate
    name = "tax_emission"
    te = scenario.par(name)
    if config.climate_scenario != "BL" and not len(te):
        base = data["PRICE_EMISSION_ref"]
        tax_emission = make_df(
            name,
            node=base["node"],
            type_emission=base["emission"],
            type_tec=base["technology"],
            type_year=base["year"],
            unit="USD/tCO2",
            value=base["value"],
        )
    else:
        tax_emission = make_df(name).dropna()

    # NB This is a temporary hack for NAVIGATE to improve performance when re-solving
    #    the buildings models after ENGAGE workflow steps. It is not compatible with
    #    iterated ACCESS-STURM-MESSAGE using Scenario.solve(callback=…). There may be
    #    conflicts with the values added in the transact() block below. See comment in
    #    .navigate.workflow.generate().
    # FIXME Decouple build_and_solve() above; re-use message_data.model.workflow.solve
    #       (from which this block is copied) for the latter.
    sc = context.buildings.solve_config
    if sc.demand_scenario:
        from message_data.tools.utilities import transfer_demands

        # Retrieve DEMAND variable data from a different scenario and set as values
        # for the demand parameter
        source = Scenario(scenario.platform, **sc.demand_scenario)
        transfer_demands(source, scenario)

    # Add data
    with scenario.transact(f"{__name__}.pre_solve()"):
        scenario.add_par("demand", demand)
        scenario.add_par("tax_emission", tax_emission)

    # Store data for post_solve()
    data.update(demand=demand, prices=prices)


def _mpd(x: pd.DataFrame, y: pd.DataFrame, col: str) -> float:
    """Mean percentage deviation between columns `col` in `x` and `y`."""
    try:
        df = x.merge(y, on=["node", "commodity", "year"]).query("year != 2110")
    except KeyError:
        # Either x or y is missing one of the columns; likely empty
        return np.nan
    return (
        ((df[f"{col}_x"] - df[f"{col}_y"]) / (0.5 * (df[f"{col}_x"] + df[f"{col}_y"])))
        .abs()
        .mean()
    )


def log_data(config, data, demand, price, i: int):
    """Update `data` and files on disk with iteration logs of `demand` and `price`."""
    # Create data frames in `data` if they don't already exist; only keys, no values
    data.setdefault("demand_log", demand.drop("value", axis=1))
    data.setdefault("price_log", price.drop("lvl", axis=1))

    # Store values
    data["demand_log"] = data["demand_log"].merge(
        demand.rename(columns={"value": f"value{i}"}), on=nclytu, how="left"
    )
    data["price_log"][f"lvl{i}"] = price["lvl"]

    # Write to file
    data["demand_log"].to_csv(config._output_path.joinpath("demand-track.csv"))
    data["price_log"].to_csv(config._output_path.joinpath("price-track.csv"))


def post_solve(scenario: Scenario, context, data):
    """Post-solve portion of the ACCESS-STURM-MESSAGE loop."""
    # Unpack data
    config = context.buildings
    iterations = data["iterations"]
    demand = data["demand"]  # Stored by pre_solve()
    prices = data["prices"]  # Stored by pre_solve()

    log.info(f"Iteration: {iterations}")

    # Retrieve prices from the MESSAGE solution
    prices_new = get_prices(scenario)

    # Keep track of results
    log_data(config, data, demand, prices_new, iterations)

    # Compute mean percentage deviation in prices and demand
    diff = _mpd(prices_new, prices, "lvl")
    data["diff_log"].append(diff)
    diff_dd = _mpd(demand, data["demand_old"], "value")
    log.info(f"Mean Percentage Deviation in Prices: {diff}")
    log.info(f"Mean Percentage Deviation in Demand: {diff_dd}")

    # Uncomment this for testing
    # diff = 0.0

    if (diff < 5e-3) or (iterations > 0 and diff_dd < 5e-3):
        done = True
        data["converged"] = True
        log.info("Converged")
    elif iterations >= config.max_iterations:
        done = True
        data["converged"] = False
    elif abs(data["diff_log"][-2] - diff) < 1e-5:
        # FIXME(PNK) This is not used anywhere. What is it for?
        data["oscilation"] = True
        done = False

    # After all post-solve steps
    mark_time()

    return done
