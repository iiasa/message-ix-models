import logging
import pickle
import re
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Optional, cast

from genno import KeyExistsError
from genno.caching import hash_args
from message_ix import Scenario, make_df

from message_ix_models import Context
from message_ix_models.model import buildings
from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport.build import main as build_transport
from message_ix_models.model.workflow import Config as WfConfig
from message_ix_models.model.workflow import solve
from message_ix_models.project.engage import workflow as engage
from message_ix_models.project.navigate import get_policy_config
from message_ix_models.util import (
    add_par_data,
    identify_nodes,
    private_data_path,
    replace_par_data,
)
from message_ix_models.workflow import Workflow

from .report import gen_config

if TYPE_CHECKING:
    import pandas

log = logging.getLogger(__name__)

# Functions for individual workflow steps


def build_materials(context: Context, scenario: Scenario) -> Scenario:
    """Workflow step 2."""
    from message_data.model.material import build

    p = private_data_path("data")
    raise NotImplementedError(
        f"""Requires code on the material-R12-rebase branch.

Switch to that branch and run:

$ mix-models --url="ixmp://{scenario.platform.name}/{scenario.url}" --local-data "{p}" material build --tag=NAVIGATE
"""  # noqa: E501
    )

    return build(scenario)


def strip_policy_data(context: Context, scenario: Scenario) -> Scenario:
    """Remove policy data, i.e. ``bound_emission`` and ``tax_emission``.

    Currently unused.
    """
    with scenario.transact(message=__doc__):
        for name in ("bound_emission", "tax_emission"):
            scenario.remove_par(name, scenario.par(name))

    scenario.set_as_default()

    return scenario


def log_scenario_info(where: str, s: Scenario) -> None:
    """Dump information about `s` to the log.

    This can be useful to confirm scenarios are passed through the workflow correctly.
    """
    log.info(
        f"""in {where}:
   {repr(s)} = {s.url}
   has_solution = {s.has_solution()}; is_default = {s.is_default()}"""
    )


def add_globiom(context: Context, scenario: Scenario, *, clean=False):
    """Add GLOBIOM structure and data.

    Parameters
    ----------
    clean : bool
        If :data:`True`, strip existing GLOBIOM structure and data first. Default
        :data:`False` because the step can be extremely slow; at least 16 hours on
        hpg914.iiasa.ac.at.
    """
    # NB the awkward imports here are needed because /tools/utilities/__init__.py
    #    makes "add_globiom" a reference to the class "add_globiom", which obstructs
    #    access to the module of the same name or its contents. We'd prefer:
    # from message_data.tools.utilities import add_globiom as ag
    # ag.clean(...)
    # ag.add_globiom(...)
    from message_data.tools.utilities.add_globiom import add_globiom as ag_main
    from message_data.tools.utilities.add_globiom import clean as ag_clean

    # Disabled by default: this step is extremely slow; at least 16 hours on
    # hpg914.iiasa.ac.at
    # Strip out existing configuration and data
    if clean:
        ag_clean(scenario)

    context.model.regions = identify_nodes(scenario)

    # Add GLOBIOM emulator
    ag_main(
        mp=scenario.platform,
        scen=scenario,
        ssp="SSP2",
        data_path=private_data_path(),
        calibration_year=2015,
        # NB this presumes that data/globiom/config.yaml has a top-level key with this
        #    name, e.g. "R12"
        config_setup=context.model.regions,
        verbose=context.verbose,
    )


def adjust_materials(
    context: Context, scenario: Scenario, *, WP6_production: str
) -> Scenario:
    """Apply adjustments from WP2 for WP6 scenarios."""
    from message_data.projects.navigate.wp2.util import (
        add_CCS_constraint,
        add_electrification_share,
        add_LED_setup,
        limit_h2,
    )
    from message_data.tools.utilities import update_h2_blending

    if WP6_production == "advanced":
        add_LED_setup(scenario)
        # Use upper bound to avoid missing PRICE_COMMODITY values
        add_electrification_share(scenario, kind="up")
        limit_h2(scenario, "green")

    # Constrain CCS at 2.0 Gt [units?]
    add_CCS_constraint(scenario, 2.0, "upper")

    # NB this could occur earlier in the workflow, e.g. before the "M built" step/
    #    build_materials(), above. However, since that step currently requires switching
    #    to a different branch, we keep it here so it can be run together with all the
    #    other steps on the current branch.
    update_h2_blending(scenario)

    # NB ditto above
    # Correct a known error in the base scenarios used for NAVIGATE: move
    # relation_activity entries for t=hp_gas_i from r=CO2_r_c to r=CO2_ind.
    replace_par_data(
        scenario,
        "relation_activity",
        dict(technology="hp_gas_i", relation="CO2_r_c"),
        dict(relation={"CO2_r_c": "CO2_ind"}),
    )

    scenario.set_as_default()
    log_scenario_info("adjust_materials", scenario)

    return scenario


#: Common settings to use when invoking MESSAGEix-Buildings. The value for
#: "sturm_scenario" is a placeholder, replaced in :func:`build_solve_buildings`.
BUILDINGS_CONFIG = buildings.Config(
    max_iterations=1,
    run_access=False,
    sturm_method="Rscript",
    sturm_scenario="",
)


def _strip(scenario: Scenario):
    """Remove certain technologies identified as irrelevant to NAVIGATE."""
    from message_ix_models import Spec
    from message_ix_models.model import build

    # The spec only contains a list of technologies to remove
    tech_expr = re.compile("^RC(spec|therm)_")
    s = Spec()
    s.remove.set["technology"] = [
        t for t in get_codes("technology") if tech_expr.match(t.id)
    ]

    build.apply_spec(scenario, s, fast=True)


def build_solve_buildings(
    context: Context,
    scenario: Scenario,
    navigate_scenario: str,
    config: Optional[dict] = None,
) -> Scenario:
    """Workflow steps 5–7."""
    from message_ix_models.model.buildings import sturm

    _strip(scenario)

    # Configure
    context.buildings = replace(
        BUILDINGS_CONFIG,
        sturm_scenario=sturm.scenario_name(navigate_scenario),
        **(config or {}),
    )

    return buildings.build_and_solve(context)


@contextmanager
def avoid_locking(scenario: Scenario):
    """Avoid locking the scenario in the database.

    .. todo:: move upstream to :mod:`ixmp`.
    """
    mp = scenario.platform
    try:
        yield
    except Exception as e:
        log.info(f"Avoid locking {scenario!r} before raising {e}")
        try:
            scenario.discard_changes()
            log.info("Discard scenario changes")
        except Exception:
            pass
        finally:
            del scenario
        mp.close_db()
        log.info("Close database connection")
        raise


def add_macro(context: Context, scenario: Scenario) -> Scenario:
    """Invoke :meth:`.Scenario.add_macro`."""
    from sdmx.model import Annotation, Code

    from message_ix_models.model import macro
    from message_ix_models.model.build import _add_unit
    from message_ix_models.model.buildings.rc_afofi import get_afofi_commodity_shares

    log_scenario_info("add_macro 1", scenario)

    # List of commodities and associated MACRO sectors

    def _c(id, sector):
        return Code(id=id, annotations=[Annotation(id="macro-sector", text=sector)])

    commodities = [
        _c("i_therm", "i_therm"),
        _c("i_spec", "i_spec"),
        _c("afofi_spec", "rc_spec"),
        _c("afofi_therm", "rc_therm"),
        _c("transport", "transport"),
    ]

    # Generate some MACRO data. These values are identical to those found in
    # P:/ene.model/MACRO/python/R12-CHN-5y_macro_data_NGFS_w_rc_ind_adj_mat.xlsx
    context.model.regions = identify_nodes(scenario)
    data = dict(
        config=macro.generate("config", context, commodities),
        aeei=macro.generate("aeei", context, commodities, value=0.02),
        drate=macro.generate("drate", context, commodities, value=0.05),
        depr=macro.generate("depr", context, commodities, value=0.05),
        lotol=macro.generate("lotol", context, commodities, value=0.05),
    )

    # Load other MACRO data from file
    data2 = macro.load(private_data_path("macro", "navigate"))
    data.update(data2)

    # Scale the demand_ref data for the "rc_{spec,therm}" MACRO sectors (~
    # "afofi_{spec,therm}" MESSAGE commodities)
    # Retrieve the commodity shares of AFOFI in RC; convert to a data frame.
    c_share = (
        get_afofi_commodity_shares()
        .to_dataframe()
        .rename_axis(index=["node", "sector"])
        .reset_index()
    )
    # - Merge with existing demand_ref data.
    # - Use a scaling factor of 1.0 for unrelated sectors.
    # - Recompute "value".
    # - Drop the temporary column.
    data["demand_ref"] = cast(
        "pandas.DataFrame",
        data["demand_ref"]
        .merge(c_share, how="left", on=["node", "sector"])
        .fillna({"afofi share": 1.0})
        .eval("value = value * `afofi share`"),
    ).drop("afofi share", axis=1)

    # Add units present in the MACRO input data which may yet be missing on the platform
    # FIXME use consistent units in the MACRO input data and message-ix-models
    for df in data2.values():
        unit = df["unit"].unique()[0]
        _add_unit(scenario.platform, unit, unit)

    log_scenario_info("add_macro 2", scenario)
    assert scenario.has_solution()

    # Calibrate; keep same URL, just a new version
    with avoid_locking(scenario):
        result = scenario.add_macro(
            data,
            scenario=scenario.scenario,
            # Skip convergence check: this uses a throwaway clone, but in the NAVIGATE
            # workflow a solved scenario is needed anyway for subsequent steps. So we
            # solve (see generate, below), confirm convergence there, and then the
            # workflow proceeds.
            check_convergence=False,
        )
    result.set_as_default()
    return result


def report(
    context: Context,
    scenario: Scenario,
    *,
    other_scenario_info: Optional[dict] = None,
    use_legacy_reporting: bool = True,
) -> Scenario:
    """Workflow steps 8–10.

    Parameters
    ----------
    use_legacy_reporting : bool
        :any:`True` (the default) to invoke :mod:`message_ix_models.report.legacy`;
        :any:`False` to skip.
    """
    from message_ix_models.report import (
        _invoke_legacy_reporting,
        log_before,
        prepare_reporter,
        register,
    )

    # Identify other scenario for .navigate.report.callback
    context.navigate.copy_ts = other_scenario_info or dict()

    # Step 8
    register("projects.navigate")
    rep, _ = prepare_reporter(context)

    path = context.get_local_path("report", "navigate-all.svg")
    rep.visualize(filename=path, key="navigate all")
    log.info(f"Visualization written to {path}")

    # Remove time-series data preceding the first model period
    key = "navigate remove ts"
    log_before(context, rep, key)
    rep.get(key)

    # Possibly copy time-series data for the period before the first model period; i.e.
    # 2020 in policy scenarios wherein the first_model_year is advanced to 2025.
    # NB this could also be done by making the "navigate remove ts" operation
    #    configurable, and assuming the time series data are already present in the
    #    current scenario, i.e. it has been cloned from `other_scenario_info`.
    key = "navigate copy ts"
    log_before(context, rep, key)
    rep.get(key)

    # Perform reporting for buildings, materials, and transport
    key = "navigate all"
    log_before(context, rep, key)
    rep.get(key)

    # Display information about the result
    log.info(
        f"File output(s), if any, written under:\n{rep.graph['config']['output_dir']}"
    )

    # Step 9
    # Configuration for legacy reporting; matches values in data/report/navigate.yaml
    # used when running the workflow steps manually.
    context.buildings = BUILDINGS_CONFIG
    context.report.legacy = dict(
        use=use_legacy_reporting,  # NB currently ignored by the function
        merge_hist=True,
        merge_ts=True,
        # Specify output directory
        out_dir=context.get_local_path("report", "legacy"),
        # NB(PNK) this is not an error; .iamc_report_hackathon.report() expects a string
        #         containing "True" or "False" instead of an actual bool.
        ref_sol="False",
        run_config="navigate-rc.yaml",
    )
    if use_legacy_reporting:
        _invoke_legacy_reporting(context)

    return scenario


def prep_submission(context: Context, *scenarios: Scenario):
    """Workflow step 10 (only)."""
    from message_data.tools.prep_submission import main

    # Generate configuration
    config = gen_config(
        context, Path("~/vc/iiasa/navigate-workflow").expanduser(), scenarios
    )
    # Invoke prep_submission
    main(config)

    log.info(f"Merged output written to {config.out_fil}")


def _cache(context, scenario, name, data):
    # Parts of the file name: function name, hash of arguments and code
    name_parts = [
        name,
        hash_args(scenario.model, scenario.scenario),
        str(scenario.version),
    ]
    # Path to the cache file
    path = context.get_cache_path("-".join(name_parts)).with_suffix(".pkl")

    log.info(f"Dump {name!r} data for {scenario.url} to:")
    log.info(str(path))
    with open(path, "wb") as f:
        pickle.dump(data, f)


def _load_latest_cache(context, info, name):
    # Parts of the file name: cache name
    name_parts = [name, hash_args(info["model"], info["scenario"]), "*"]
    # Path to the cache file
    paths = sorted(context.get_cache_path().glob("-".join(name_parts) + ".pkl"))
    # print(paths)
    path = paths[-1]

    log.info(f"Read {name!r} data")
    log.info(f"for {info}")
    log.info(f"from {path}")

    with open(path, "rb") as f:
        return pickle.load(f)


def compute_minimum_emissions(
    context: Context, scenario: Scenario, from_period=2020, to_period=2025, k=0.95
):
    """Limit global CO2 emissions in `to_period` to `k` × emissions in `from_period`."""
    from message_data.projects.engage.runscript_main import (
        glb_co2_relation as RELATION_GLOBAL_CO2,
    )
    from numpy.testing import assert_allclose

    years = [from_period, to_period]

    # Retrieve EMISS values; analogous to .engage.ScenarioRunner.retr_CO2_trajectory
    emiss = scenario.var(
        "EMISS",
        filters=dict(emission="TCE_CO2", year=years, node="World", type_tec="all"),
    )
    # Retrieve values for the global CO2 relation
    common = dict(relation=RELATION_GLOBAL_CO2, node_rel="R12_GLB")
    rel = scenario.var("REL", filters=dict(year_rel=years, **common))

    # Log both for debugging
    log.info(f"EMISS:\n{emiss}")
    log.info(f"REL:\n{rel}")

    # Ensure the values are consistent
    emiss = emiss.query(f"year == {from_period}")
    value1 = emiss.at[0, "lvl"]
    value2 = rel.query(f"year_rel == {from_period}").at[0, "lvl"]

    try:
        assert_allclose(value1, value2, rtol=1e-5)
    except AssertionError as e:
        log.warning(repr(e))
    value = min(value1, value2)

    # Set relation_lower value; analogous to add_CO2_emission_constraint() as called
    # from .engage.workflow.step_0;
    name = "relation_lower"
    df = make_df(name, **common, year_rel=to_period, value=k * value, unit="tC")

    msg = (
        f"Limit emissions in {to_period} ≥ {k} × emissions in {from_period}: {name}\n"
        + df.to_string()
    )
    log.info(msg)

    # Cache the resulting data. We cannot add this directly to scenario() here, because
    # that is only possible with scenario.remove_solution() which, in turn, defeats the
    # behaviour of Scenario.clone(…, shift_first_model_year=…) that is essential for the
    # feasibility of policy scenarios
    _cache(context, scenario, "limit-drop", {name: df})


def add_minimum_emissions(context, scenario, info: dict) -> None:
    from message_data.projects.engage.runscript_main import (
        glb_co2_relation as RELATION_GLOBAL_CO2,
    )
    from message_data.tools.utilities import add_CO2_emission_constraint

    data = _load_latest_cache(context, info, "limit-drop")

    add_CO2_emission_constraint(
        scenario,
        relation_name=RELATION_GLOBAL_CO2,
        reg=f"{identify_nodes(scenario)}_GLB",
        constraint_value=0.0,
        type_rel="lower",
    )

    with scenario.transact():
        add_par_data(scenario, data)

    # This step is preceded by a clone; ensure the new/modified version is default
    scenario.set_as_default()


def tax_emission(context: Context, scenario: Scenario, price: float) -> "Scenario":
    """Workflow callable for :mod:`.tools.utilities.add_tax_emission`.

    .. note:: This requires the emissions accounting established by either
       :func:`.engage.workflow.step_0` or :func:`.model.workflow.step_0`.

       In :func:`.generate` in this file, the former is called earlier in the workflow.
    """
    from message_ix_models.tools import add_tax_emission

    try:
        scenario.remove_solution()
    except ValueError:
        pass

    add_tax_emission.main(scenario, price)

    return scenario


def iter_scenarios(
    context, filters
) -> Generator[tuple[str, Optional[str], Optional[str], Optional[str]], None, None]:
    """Iterate over filtered scenario codes while unpacking information.

    Yields a sequence of 4-tuples:

    1. Short label or identifier.
    2. The value of the ``navigate_climate_policy`` annotation, if any.
    3. The value of the ``navigate_T35_policy`` annotation, if any.
    4. The value of the ``navigate_WP6_production`` annotation, if any.

    Only the scenarios matching `filters` are included; see
    :func:`navigate.iter_scenario_codes`. Certain other scenarios are also skipped:

    - T3.5 scenarios with regionally differentiated carbon prices (indicated by "_d") in
      the name.
    - Older T6.2 scenario codes that appear in the official list but are not annotated.
    """
    from message_ix_models.project.navigate import iter_scenario_codes

    for code in iter_scenario_codes(context, filters):
        # Short label from the code ID
        match = re.match("(NAV_Dem|PC|PEP)-(.*)", code.id)
        assert match
        label = match.group(2)

        info: list[Optional[str]] = []

        # Values for 3 annotations
        for name in ("climate_policy", "T35_policy", "WP6_production"):
            try:
                info.append(str(code.get_annotation(id=f"navigate_{name}").text))
            except KeyError:
                info.append(None)  # Annotation does not exist on `code`

        # Skip scenarios not implemented
        if label.endswith("_d") or (info[1] is info[2] is None):
            continue

        yield tuple([label] + info)  # type: ignore [misc]


# FIXME Reduce complexity from 13 to ≤11
def generate(context: Context) -> Workflow:  # noqa: C901
    """Create the NAVIGATE workflow for T3.5, T6.1, and T6.2."""
    # NB Step numbers below ("Step 1") refer to the list of steps in the documentation.
    #    This list is not yet updated to include many intermediate steps with
    #    adjustments needed for functional NAVIGATE workflows, so some .add_step() calls
    #    don't have an associated number.
    # TODO Renumber and update docs accordingly

    wf = Workflow(context)

    # Use MESSAGE if .model.transport is included. Otherwise, use MESSAGE-MACRO for
    # additional demand-side flexibility in meeting low climate targets
    solve_model = "MESSAGE" if context.navigate.transport else "MESSAGE-MACRO"

    # Collections of names of intermediate steps in the workflow
    M_built = {}  # Mapping from WP6_production → result of step 2
    baseline_solved = {}  # Mapping from short ID (`s`) → result of step 7
    # Mapping from short IDs → 2-tuple with:
    # 1. Name of the final step in the policy sequence (if any).
    # 2. Args for report(…, other_scenario_info=…) —either None or model/scenario name.
    to_report: dict[str, tuple[str, Optional[Mapping]]] = {}

    # Step 1
    wf.add_step(
        "base",
        None,
        target="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#21",
    )

    # NB some preparatory changes could be done at this point, but are instead done in
    # adjust_materials():
    # - update_h2_blending().
    # - replace_par_data() to move hp_gas_i entries from CO2_r_c to CO2_ind.
    #
    # This is because the current materials code used to produce the "M built" scenario
    # is not yet merged to `dev` or `main`, and cannot be run at the same time as the
    # current code. So we typically use the "--from=" CLI option to start from the
    # result of the "M […] built" step, below.

    # Step 2 and related: different base scenarios according to WP6_production
    _s = "MESSAGEix-GLOBIOM 1.1-M-R12-NAVIGATE/baseline_add_material#54"
    for WP6_production, label, target in (
        ("default", "def", _s),
        ("advanced", "adv", _s),
        (None, "T3.5", "MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE"),
    ):
        # Step 2: Add materials
        name = wf.add_step(f"M {label} built", "base", build_materials, target=target)

        # Adjust contents of the base model
        name = wf.add_step(
            f"M {label} adjusted",
            name,
            adjust_materials,
            clone=True,
            target=f"MESSAGEix-GLOBIOM 1.1-M-R12 (NAVIGATE)/{label}",
            WP6_production=WP6_production,
        )

        # Update GLOBIOM
        # NB use clean=True to strip all GLOBIOM data prior to re-adding; this takes
        #    about 1 hour on hpg914
        name = wf.add_step(f"M {label} + GLOBIOM", name, add_globiom, clean=False)

        # Store step name as starting point for further steps, below
        M_built[WP6_production] = name

    # Steps 3–7: iterate over all possible "NPi" (baseline) scenarios
    filters = {
        "navigate_task": {"T3.5", "T6.1", "T6.2"},
        "navigate_climate_policy": "NPi",
    }
    for s, _, T35_policy, WP6_production in iter_scenarios(context, filters):
        # Identify the starting point for the next steps
        name = M_built[WP6_production]

        # Variant label for model name
        variant = "M" + ("T" if context.navigate.transport else "")
        model = f"MESSAGEix-GLOBIOM 1.1-{variant}-R12 (NAVIGATE)"

        # Step 3: Add transport
        if context.navigate.transport:
            name = wf.add_step(
                f"{variant} {s} built",
                name,
                build_transport,
                target=f"{model}/{s}",
                clone=True,
                # Passed directly to .transport.build.main
                fast=True,
                navigate_scenario=T35_policy,
            )

        # Step 4: Solve
        config = WfConfig(reserve_margin=False, solve=dict(model="MESSAGE"))
        name = wf.add_step(f"{variant} {s} solved", name, solve, config=config)

        # Steps 5–7: Add and solve buildings
        variant = "B" + variant
        model = f"MESSAGEix-GLOBIOM 1.1-{variant}-R12 (NAVIGATE)"
        target = f"{model}/{s}"
        name = wf.add_step(
            f"{variant} {s} solved",
            name,
            build_solve_buildings,  # type: ignore
            target=target,
            clone=True,
            navigate_scenario=s,
        )

        # Calibrate MACRO
        if solve_model == "MESSAGE-MACRO":
            target = f"{target}+MACRO"
            name = wf.add_step(
                f"{variant} {s} with MACRO",
                name,
                add_macro,
                target=target,
                clone=dict(keep_solution=True),
            )
            # Add ENGAGE-style emissions accounting
            # FIXME this step will not run if `solve_model` is "MESSAGE"
            name = wf.add_step(f"{variant} {s} with EA", name, engage.step_0)
            config = WfConfig(reserve_margin=False, solve=dict(model=solve_model))
            name = wf.add_step(f"{name} solved", name, solve, config=config)

        # Compute a minimum for 2025 emissions based on 2020 values in the solution.
        # Values are cached, but not yet added to the scenario. That would require
        # scenario.remove_solution(), but then the solution would be no longer present
        # in a subsequent step that uses clone(shift_first_model_year=2025). Without a
        # solution at that point, the 2020 solution data is not copied to historical
        # parameters; an essential step for the feasibility of policy scenarios
        name = wf.add_step(f"{s} 2025 limit computed", name, compute_minimum_emissions)

        # Store the step name as a starting point for climate policy steps, below
        baseline_solved[(T35_policy, WP6_production)] = name

    # Now iterate over *all* end scenarios: baselines and climate policy scenarios
    filters.pop("navigate_climate_policy")
    for s, climate_policy, T35_policy, WP6_production in iter_scenarios(
        context, filters
    ):
        # Identify the base scenario for the next steps
        base = baseline_solved[(T35_policy, WP6_production)]
        base_info, _ = wf.guess_target(base, "scenario")

        # Select the indicated .model.workflow.Config or .engage.workflow.PolicyConfig
        # object, if any
        config = get_policy_config(climate_policy)
        # Use MESSAGE or MESSAGE-MACRO as appropriate
        config.solve.update(model=solve_model)

        if not (len(getattr(config, "steps", [])) or climate_policy == "Ctax"):
            # No policy; simply add this baseline scenario to the list of scenarios to
            # be reported
            to_report[s] = (base, None)
            continue

        name = base.replace("computed", "added")
        try:
            # Shift first model year and add minimum constraint on emissions
            wf.add_step(
                name,
                base,
                add_minimum_emissions,
                target=f"{base_info['model']}/{base_info['scenario']} "
                + str(context.navigate.policy_year),
                clone=dict(shift_first_model_year=context.navigate.policy_year),
                info=base_info,
            )
        except KeyExistsError:
            pass  # Already added

        if solve_model == "MESSAGE-MACRO":
            # Provide a reference scenario from which to copy (MACRO) DEMAND variable
            # data as a better starting point for (MESSAGE) demand parameter in
            # MESSAGE-MACRO iteration
            config.demand_scenario.update(base_info)

        if isinstance(config, engage.PolicyConfig):
            # Add 1 or more steps for ENGAGE-style climate policy workflows
            if climate_policy == "15C":
                # Use the Ctax-ref scenario as a better starting point
                # NB this implies the referenced scenario must be solved first. This
                #    value is only used in the first ENGAGE step; after this,
                #    .engage.workflow.add_steps() overwrites with values from the prior
                #    step.
                config.demand_scenario.update(
                    model=base_info["model"], scenario="Ctax-ref"
                )
            elif climate_policy == "20C T6.2":
                # Provide a reference scenario from which to copy tax_emission data
                # Model and scenario for the scenario produced by the base step
                config.tax_emission_scenario.update(model=base_info["model"])

            # Invoke the function from .engage.workflow to add the ENGAGE step(s)
            # NB this can occur here so long as PolicyConfig.method = "calc" is NOT used
            #    by any of the objects in CLIMATE_POLICY. If "calc" appears, then
            #    engage.step_1 requires data which is currently only available from
            #    legacy reporting output, and the ENGAGE steps must take place after
            #    step 9 (running legacy reporting, below), which is not implemented.
            name = engage.add_steps(wf, base=name, config=config, name=s)
        elif climate_policy == "Ctax":
            # Add a carbon tax (not an implementation of an ENGAGE climate policy)
            name = wf.add_step(
                s,
                name,
                tax_emission,
                target=f"{model}/{s}",
                clone=True,
                price=context.navigate.carbon_tax,
            )
            # Solve
            name = wf.add_step(
                f"{s} solved", name, solve, config=config, set_as_default=True
            )
        else:
            raise ValueError(climate_policy)

        # Re-solve the buildings model(s) to pick up price changes

        # Construct the target scenario URL
        info = wf.guess_target(name, "scenario")[0]
        target = "{model}/{scenario}+B".format(**info)

        # Configuration for solving MESSAGE(-MACRO) within the Buildings sub-workflow.
        # NB This relies on a corresponding change in .buildings.pre_solve() that
        #    invokes transfer_demands(); see comment there.
        sc = WfConfig(solve=config.solve, reserve_margin=False)
        # If running ENGAGE sub-workflow, copy demands from the latest step
        if isinstance(config, engage.PolicyConfig):
            sc.demand_scenario.update(info)
        sc.solve["solve_options"].setdefault("predual", 1)

        # - Use the same NAVIGATE buildings scenario as the `base`.
        # - Use the same Scenario.solve() keyword arguments, including `solve_model` and
        #   ``solve_options["barcrossalg"]``.
        name = wf.add_step(
            f"{name} again",
            name,
            build_solve_buildings,
            # Clone before this step
            target=target,
            clone=True,
            # Keyword arguments for build_solve_buildings
            navigate_scenario=s,
            # NB(PNK) This is duplicative, temporarily: "solve" will probably go away
            config=dict(solve_config=sc, solve=sc.solve),
        )

        # Store info to set up reporting, include other scenario from which to copy time
        # series data for y=2020
        to_report[s] = (name, base_info)

    add_reporting_steps(wf, to_report)

    return wf


def add_reporting_steps(
    wf: Workflow, to_report: Mapping[str, tuple[str, Optional[Mapping]]]
) -> list[str]:
    """Add reporting and prep-solution steps to `wf` for each item in `to_report`.

    .. todo:: Migrate to :mod:`message_ix_models`.
    """
    added = []

    for s, (name, other) in to_report.items():
        # Steps 8–9: Report individual scenario (both genno and legacy reporting)
        added.append(
            wf.add_step(f"{s} reported", name, report, other_scenario_info=other)
        )

        # Step 10: Prepare individual scenario for submission
        wf.add_step(f"{s} prepped", added[-1], prep_submission)

    # Key to invoke steps 8–9 for all scenarios as a batch
    wf.add_single("all reported", *added)

    # Key to invoke step 10 for all scenarios as a batch
    wf.add_single("all prepped", prep_submission, "context", *added)
    wf.default_key = "all prepped"

    return added
