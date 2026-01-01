import logging
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from copy import deepcopy
from itertools import product
from typing import TYPE_CHECKING, cast

import message_ix
import pandas as pd
from genno import Quantity
from genno.operator import mul, relabel, rename_dims

try:
    from ixmp.report.operator import data_for_quantity
    from message_ix.report.operator import as_message_df
except ImportError:  # ixmp/message_ix v3.7.0
    from ixmp.reporting.computations import (  # type: ignore [import-not-found,no-redef]
        data_for_quantity,
    )
    from message_ix.reporting.computations import (  # type: ignore [import-not-found,no-redef]
        as_message_df,
    )
from sdmx.model.v21 import Annotation, Code

from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.model import build
from message_ix_models.model.bmt.utils import subtract_material_demand
from message_ix_models.model.structure import (
    generate_set_elements,
    get_codes,
    get_region_codes,
)
from message_ix_models.util import (
    load_package_data,
    make_io,
    merge_data,
    nodes_ex_world,
    private_data_path,
)

from .rc_afofi import get_afofi_commodity_shares, get_afofi_technology_shares

# from message_data.projects.ngfs.util import add_macro_COVID  # Unused

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import MutableParameterData, ParameterData

log = logging.getLogger(__name__)

#: STURM commodity names to be converted for use in MESSAGEix-Materials; see
#: :func:`materials`.
BUILD_COMM_CONVERT = [
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
#:
#: .. todo:: Move to and read from :file:`data/buildings/set.yaml`.
MATERIALS = ["steel", "cement", "aluminum"]


def adapt_emission_factors(data: "MutableParameterData") -> None:
    """Adapt ``relation_activity`` values in `data` that represent emission factors.

    In MESSAGEix-GLOBIOM, ``relation_activity`` entries for, for instance, r=CO_Emission
    are computed as (emission factor for fuel, species) × (input efficiency of
    technology consuming the fuel). Because the MESSAGE-Buildings representation sets
    the latter to 1.0, the relation_activity entries must be recomputed.

    This function updates the values in :py:`data["relation_activity"]`, assuming that
    :py:`data["input"]` contains the *original* (base model, MESSAGEix-GLOBIOM) input
    efficiencies. Then it sets :py:`data["input"]["value"]` to 1.0.

    .. todo:: When available in :mod:`message_ix_models`, simply read the values for
       each (fuel, species) from a file, rather than performing this calculation.
    """

    def assert_value_unique(dfgb):
        """Ensure that each group of `dfgb` contains only 1 unique "value"."""
        assert (1 == dfgb.nunique()["value"]).all()
        return dfgb

    # Common dimensions of "relation_activity" and "input", to merge on
    cols = ["node_loc", "technology", "year_act", "mode"]
    # Relations to omit from calculation
    omit = ["HFC_emission", "HFC_foam_red"]

    # - Group "input" by `cols`.
    # - Take the first value in each group; given all values are the same within groups.
    # - Rename "value" to "input" (avoiding clash with "value" in relation_activity).
    # - Drop columns not present in relation_activity.
    input_ = (
        data["input"]
        .groupby(cols)
        .pipe(assert_value_unique)
        .nth(0)
        .rename(columns={"value": "input"})
        .drop(
            "year_vtg node_origin commodity level time time_origin unit".split(), axis=1
        )
    )

    # - Omit certain relations.
    # - Merge `input_` into "relation_activity" data to add an "input" column.
    # - Divide by base-model input efficiency to recover emissions factors per fuel.
    # - Drop "input" column.
    ra = "relation_activity"
    data[ra] = cast(
        pd.DataFrame,
        data[ra][~data[ra].relation.isin(omit)]
        .merge(input_, how="left", on=cols)
        .astype({"year_rel": int})
        .eval("value = value / input"),
    ).drop("input", axis=1)

    # Set input efficiencies to 1.0 per MESSAGE-Buildings representation
    data["input"] = data["input"].assign(value=1.0)


def get_spec(context: Context) -> Spec:
    """Return the specification for MESSAGEix-Buildings.

    Parameters
    ----------
    context : .Context
        The key ``regions`` determines the regional aggregation used.

    .. todo:: Expand to handle :data:`BUILD_COMM_CONVERT`.
    """
    load_config(context)

    s = deepcopy(context["buildings spec"])

    # Read config and save to context.buildings
    from message_ix_models.model.buildings.config import Config

    config = Config()
    context.buildings = config

    if context.buildings.with_materials:
        s.require.set["commodity"].extend(MATERIALS)

    # commented: See docstring of bio_backstop and comments in prepare_data, below
    # s.add.set["technology"].append(Code(id="bio_backstop"))

    # The set of required nodes varies according to context.regions
    s.require.set["node"].extend(map(str, get_region_codes(context.regions)))

    return s


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


def get_techs(spec: Spec, commodity=None) -> list[str]:
    """Return a list of buildings technologies."""
    codes: Iterable[Code] = spec.add.set["technology"]
    if commodity:
        codes = filter(lambda s: s.id.startswith(commodity), codes)

    return sorted(map(str, codes))


def get_tech_groups(
    spec: Spec, include="commodity enduse", legacy=False
) -> Mapping[str, Sequence[str]]:
    """Return groups of buildings technologies from `spec`.

    These are suitable for aggregation, e.g. in data preparation or reporting.

    Parameters
    ----------
    spec
        The result of :func:`get_spec`.
    include : str or sequence of str
        May include specific values to control what is returned:

        - "commodity": include keys like "resid gas", where "gas" is a commodity,
          collecting technologies which consume this commodity.
        - "enduse": include keys like "comm other_uses", where "other_uses" is a
          buildings energy end-use, collecting technologies which represent this
          end-use.
    legacy
        if :data:`True`, apply mapping from commodity names to labels used in legacy
        reporting code; e.g. "electr" becomes "elec".
    """
    if legacy:
        try:
            # FIXME This COMMODITY dictionary is not present in the version of the
            #       legacy reporting migrated to message_ix_models. It, or this code,
            #       must be updated in order to be usable.
            from message_ix_models.report.legacy.default_tables import (  # type: ignore [attr-defined]
                COMMODITY,
            )
        except ImportError:
            COMMODITY = dict()
    else:
        COMMODITY = dict()

    # Results
    techs = defaultdict(list)

    # Expression to match technology IDs generated per buildings/set.yaml
    # - The 'c' (commodity) group matches only the "lightoil" in "lightoil_lg"
    expr = re.compile(
        "^(?P<c>.*?)(_lg)?_((?P<sector>comm|resid)_(?P<enduse>.*)|afofi)$"
    )

    def _store(value, c, e, s):
        """Update 1 or more lists in `techs` with `value`."""
        techs[s].append(value)
        if "commodity" in include:
            techs[f"{s} {c}"].append(value)
        if "enduse" in include and e:
            techs[f"{s} {e}"].append(value)

    # Iterate over technologies
    for t in spec.add.set["technology"]:
        # Extract commodity, end-use, and sector from `expr`
        match = expr.match(t.id)
        if match is None:
            continue

        sector = match.group("sector") or "afofi"
        commodity, enduse = match.group("c", "enduse")  # For sector=AFOFI, enduse=None

        # Omit technologies for the buildings-materials linkage
        if commodity in {"construction", "demolition"}:
            continue

        # For some base model technologies, e.g. `hp_el_rc`, thus for `hp_el_afofi`, the
        # ID does not contain the ID of the input commodity. Look up the actual input
        # commodity from annotations in technology.yaml.
        try:
            commodity = t.eval_annotation("input")[0]
        except TypeError:
            pass  # No such annotation

        # Maybe map to labels used in legacy reporting
        commodity = COMMODITY.get(commodity, commodity)

        # Update lists
        _store(match.string, commodity, enduse, sector)
        # Also update "rc" totals
        _store(match.string, commodity, enduse, "rc")

    return techs


def load_config(context: Context) -> None:
    """Load MESSAGEix-Buildings configuration from file and store on `context`.

    Model structure information is loaded from :file:`data/buildings/set.yaml` and
    derived from the base model structures.

    This function does most of the work for :func:`get_spec` (the parts that do not vary
    vary according to :class:`.buildings.Config`) and stores the result as the
    :class:`Context` key "buildings spec".
    """
    if "buildings spec" in context:
        return

    set_info = cast("MutableMapping", load_package_data("buildings", "set.yaml"))

    # Generate set elements from a product of others
    for set_name, info in set_info.items():
        generate_set_elements(set_info, set_name)

    # Currently unused, and generates issues when caching functions where context is an
    # argument
    set_info["technology"].pop("indexers", None)

    # Create a spec
    s = Spec()

    for set_name, info in set_info.items():
        # Elements to add, remove, and require
        for action in ("add", "remove", "require"):
            s[action].set[set_name].extend(info.get(action, []))

    # Generate commodities that replace corresponding rc_* in the base model
    for c in filter(lambda x: x.id.startswith("rc_"), get_codes("commodity")):
        new = deepcopy(c)
        new.id = c.id.replace("rc_", "afofio_")
        s.add.set["commodity"].append(new)

    # Generate technologies that replace corresponding *_rc|RC in the base model
    # Match both _RC/_rc at end and _RC_RT/_rc_RT patterns
    expr = re.compile("_(rc|RC)(_RT)?$")

    # Technologies that should not be transformed to afofi
    exclude_techs = {}

    for t in filter(lambda x: expr.search(x.id), get_codes("technology")):
        # Skip technologies that should not be transformed
        if t.id in exclude_techs:
            continue

        # Generate a new Code object, preserving annotations
        new = deepcopy(t)
        # Replace _RC or _rc with _afofio, preserving _RT suffix if present
        # e.g., sp_el_RC_RT -> sp_el_afofio_RT, loil_rc -> loil_afofio
        if t.id.endswith("_RT"):
            # Replace _RC_RT or _rc_RT with _afofio_RT
            new.id = re.sub("_(rc|RC)_RT$", "_afofio_RT", t.id)
        else:
            # Replace _RC or _rc at end with _afofio
            new.id = re.sub("_(rc|RC)$", "_afofio", t.id)
        new.annotations.append(Annotation(id="derived-from", text=t.id))

        # This will be added
        s.add.set["technology"].append(new)

        # The original technology will be removed
        s.remove.set["technology"].append(t)

    # Store
    context["buildings spec"] = s


# def merge_data(
#     base: MutableMapping[str, pd.DataFrame], *others: Mapping[str, pd.DataFrame]
# ) -> None:
#     import message_ix_models.util

#     message_ix_models.util.merge_data(base, *others)


def bio_backstop(scen: "Scenario", nodes=["R12_AFR", "R12_SAS"]) -> "ParameterData":
    """Create a backstop supply of (biomass, primary) to avoid infeasibility.

    This is not currently in use; see comments in :func:`prepare_data`.

    This function simplified from a version in the MESSAGE_Buildings/util/ directory,
    itself modified from an old/outdated (before 2022-03) version of
    :mod:`.add_globiom`.

    See https://iiasa-ece.slack.com/archives/C03M5NX9X0D/p1659623091532079 for
    discussion.
    """
    # Retrieve technology for which will be used to create the backstop
    filters = dict(technology="elec_rc", node_loc="R12_NAM", year_act=2020)

    data = defaultdict(list)

    for node, name in product(nodes, ["output", "var_cost"]):
        values = dict(technology="bio_backstop", node_loc=node)

        if name == "output":
            values.update(commodity="biomass", node_dest=node, level="primary")
        elif name == "var_cost":
            # 2023-07-24 PNK: reduced from 1e5 to address MACRO calibration issues
            values.update(value=1e1)

        data[name].append(scen.par(name, filters=filters).assign(**values))

    result: "ParameterData" = {k: pd.concat(v) for k, v in data.items()}

    log.debug(repr(result))

    return result


def scale_and_replace(
    scenario: "Scenario",
    replace: dict,
    q_scale: Quantity,
    relations: list[str],
    relax: float = 0.0,
) -> Mapping[str, pd.DataFrame]:
    """Return scaled parameter data for certain technologies.

    The function acts on the list of parameters below.

    - For some parameters (scale is None), data are copied.
    - For other parameters, data are scaled by multiplication with `q_scale`.

      - For parameters with a relative sense, e.g. ``growth_activity_lo``, no further
        scaling is applied.
      - For parameters with an absolute sense, e.g. ``bound_activity_lo``, values are
        additionally scaled by a “relaxation” factor of (1 + `relax`) for upper bounds
        or (1 - `relax`) for lower bounds. Setting `relax` to 0 (the default) disables
        this behaviour.

    These operations are applied to all data for which the ``technology`` IDs appears
    in ``replace["technology"]``.

    Finally, ``replace`` is applied to optionally replace technology IDs or IDs for
    other dimensions.

    Returns
    -------
    dict of (str -> .DataFrame)
        Keys are parameter names;
    """

    # Filters for retrieving data
    f_long = dict(filters={"technology": list(replace["technology"].keys())})
    f_short = dict(filters={"t": list(replace["technology"].keys())})

    dims = dict(mode="m", node_loc="nl", technology="t", time="h", year_act="ya")

    # Use "nl" on scaling quantity to align with parameters modified
    _q_scale = rename_dims(q_scale, {"n": "nl"}) if "n" in q_scale.dims else q_scale

    # Copy data for certain parameters with renamed technology & commodity
    result = dict()
    for name, scale in (
        ("capacity_factor", None),
        ("emission_factor", None),
        ("fix_cost", None),
        ("input", None),
        ("inv_cost", None),
        ("output", None),
        ("relation_activity", None),
        ("technical_lifetime", None),
        ("var_cost", None),
        # Historical
        ("historical_activity", 1.0),
        # Constraints
        ("growth_activity_lo", None),
        ("growth_activity_up", None),
        ("bound_activity_lo", 1 - relax),
        ("bound_activity_up", 1 + relax),
        ("bound_new_capacity_lo", 1 - relax),
        ("bound_new_capacity_up", 1 + relax),
        ("bound_total_capacity_lo", 1 - relax),
        ("bound_total_capacity_up", 1 + relax),
        ("growth_activity_lo", None),
        ("growth_activity_up", None),
        ("initial_activity_lo", 1 - relax),
        ("initial_activity_up", 1 + relax),
        ("soft_activity_lo", None),
        ("soft_activity_up", None),
        ("growth_new_capacity_lo", None),
        ("growth_new_capacity_up", None),
        ("initial_new_capacity_lo", 1 - relax),
        ("initial_new_capacity_up", 1 + relax),
        ("soft_new_capacity_lo", None),
        ("soft_new_capacity_up", None),
    ):
        if scale is None:
            # Prepare filters
            _f = deepcopy(f_long)
            if name == "relation_activity":
                # Only copy relation_activity data for certain relations
                _f["filters"].update(relation=relations)

            df = scenario.par(name, **_f)
        else:
            # - Retrieve data as a genno.quantity.
            # - Multiply by scaling factors.
            q = (
                data_for_quantity("par", name, "value", scenario, config=f_short)
                * _q_scale
                * Quantity(scale)
            )
            # Convert back to message_ix data frame. as_message_df() returns dict ->
            # (str -> pd.DataFrame), so pop the single value
            df = as_message_df(q, name, dims, {}).pop(name)

        if not len(df):
            continue

        result[name] = df.replace(replace)

        # DEBUG
        # if name in (
        #     "historical_activity",
        #     "output",
        # ):
        #     print(name)
        #     print(result[name].to_string())

    log.info(f"Data for {len(result)} parameters")

    return result


def prepare_data(
    scenario: message_ix.Scenario,
    info: ScenarioInfo,
    demand: pd.DataFrame,
    prices: pd.DataFrame,
    sturm_r: pd.DataFrame,
    sturm_c: pd.DataFrame,
    with_materials: bool,
    relations: list[str],
) -> "ParameterData":
    """Derive data for MESSAGEix-Buildings from `scenario`."""

    # Data frames for each parameter
    result: "MutableParameterData" = dict()

    # Mapping from original to generated commodity names
    c_map = {f"rc_{name}": f"afofi_{name}" for name in ("spec", "therm")}

    # Retrieve shares of AFOFI within rc_spec or rc_therm; dimensions (c, n). These
    # values are based on 2010 and 2015 data; see the code for details.
    c_share = get_afofi_commodity_shares()

    # Retrieve existing demands
    filters: dict[str, Iterable] = dict(c=["rc_spec", "rc_therm"], y=info.Y)
    afofi_dd = data_for_quantity(
        "par", "demand", "value", scenario, config=dict(filters=filters)
    )

    # On a second pass (after main() has already run once), rc_spec and rc_therm have
    # been stripped out, so `afofi_dd` is empty; skip manipulating it.
    if len(afofi_dd):
        # - Compute a share (c, n) of rc_* demand (c, n, …) = afofi_* demand
        # - Relabel commodities.
        tmp = relabel(mul(afofi_dd, c_share), {"c": c_map})

        # Convert back to a MESSAGE data frame
        dims = dict(commodity="c", node="n", level="l", year="y", time="h")
        # TODO Remove typing exclusion once message_ix is updated for genno 1.25
        result.update(as_message_df(tmp, "demand", dims, {}))  # type: ignore [arg-type]

        # Copy technology parameter values from rc_spec and rc_therm to new afofi.
        # Again, once rc_(spec|therm) are stripped, .par() returns nothing here, so
        # rc_techs is empty and the following loop does not run

        # Identify technologies that output to rc_spec or rc_therm
        rc_techs = scenario.par(
            "output", filters={"commodity": ["rc_spec", "rc_therm"]}
        )["technology"].unique()

        # Mapping from source to generated names for scale_and_replace
        replace = {
            "commodity": c_map,
            "technology": {t: re.sub("(rc|RC)", "afofi", t) for t in rc_techs},
        }
        # Compute shares with dimensions (t, n) for scaling parameter data
        t_shares = get_afofi_technology_shares(c_share, replace["technology"].keys())

        merge_data(
            result,
            # TODO Remove exclusion once message-ix-models >2025.1.10 is released
            scale_and_replace(  # type: ignore [arg-type]
                scenario, replace, t_shares, relations=relations, relax=0.05
            ),
        )

    # Create new technologies for building energy

    # Mapping from commodity to base model's *_rc technology
    rc_tech_fuel = {"lightoil": "loil_rc", "electr": "elec_rc", "d_heat": "heat_rc"}

    data = defaultdict(list)
    # Add for fuels above
    for fuel in prices["commodity"].unique():
        # Find the original rc technology for the fuel
        tech_orig = rc_tech_fuel.get(fuel, f"{fuel}_rc")

        # Create the technologies for the new commodities
        for commodity in filter(
            re.compile(f"[_-]{fuel}").search, demand["commodity"].unique()
        ):
            # Fix for lightoil gas included
            if "lightoil-gas" in commodity:
                tech_new = f"{fuel}_lg_" + commodity.replace("_lightoil-gas", "")
            else:
                tech_new = f"{fuel}_" + commodity.replace(f"_{fuel}", "")


            # Modify data
            for name, filters, extra in (  # type: ignore
                ("input", {}, {}),  # NB value=1.0 is done by adapt_emission_factors()
                ("output", {}, dict(commodity=commodity, value=1.0)),
                ("capacity_factor", {}, {}),
                ("emission_factor", {}, {}),
                ("relation_activity", dict(relation=relations), {}),
            ):
                filters["technology"] = [tech_orig]
                data[name].append(
                    scenario.par(name, filters=filters).assign(
                        technology=tech_new, **extra
                    )
                )

    # - Concatenate data frames together.
    # - Adapt relation_activity values that represent emission factors.
    # - Merge to results.
    tmp = {k: pd.concat(v) for k, v in data.items()}
    adapt_emission_factors(tmp)
    merge_data(result, tmp)

    log.info(
        "Prepared:\n" + "\n".join(f"{len(v)} obs for {k!r}" for k, v in result.items())
    )

    if with_materials:
        # Set up buildings-materials linkage
        merge_data(result, materials(scenario, info, sturm_r, sturm_c))

    # commented: This is superseded by .navigate.workflow.add_globiom_step
    # # Add data for a backstop supply of (biomass, secondary)
    # merge_data(result, bio_backstop(scenario))

    return result


def prepare_data_B(
    scenario: message_ix.Scenario,
    info: ScenarioInfo,
    prices: pd.DataFrame,
    sturm_r: pd.DataFrame,
    sturm_c: pd.DataFrame,
    demand_static: pd.DataFrame = None,
    with_materials: bool = True,
    relations: list[str] = [],
) -> "ParameterData":
    """Derive data for MESSAGEix-Buildings from `scenario`. 
    Function-wise same as prepare_data(). 

    Input data:
    - Use the MESSAGE-format report of MESSAGEix-Buildings (demand_resid and demand_comm)
    - Static external demand that is not updated in each iteration (demand_static)

    Buildings demand includes:
    - A: Resid and Comm cool/heat/hotwater demand (from STURM, with price iteration)
    - B: Resid app/cook demand (from ACCESS, no iteration)
    - C: Resid non-commecial biomass demand (from ACCESS, no iteration)
    - D: Resid and Comm material demand (from STURM, no iteration)
    - E: Residual AFOFIO demand (external)
    """

    # Data frames for each parameter
    result: "MutableParameterData" = dict()

    # Reset index 
    for df in [sturm_r, sturm_c, demand_static]:
        if df is not None and "node" not in df.columns:
            df.reset_index(inplace=True)

    # Add 2110 data by copying from 2100 if missing
    for df_name, df in [("sturm_r", sturm_r), ("sturm_c", sturm_c), ("demand_static", demand_static)]:
        if df is not None and "year" in df.columns:
            if 2110 not in df["year"].values and 2100 in df["year"].values:
                # Copy 2100 data to 2110
                df_2100 = df[df["year"] == 2100].copy()
                df_2100["year"] = 2110
                # Update the original dataframe
                if df_name == "sturm_r":
                    sturm_r = pd.concat([sturm_r, df_2100], ignore_index=True)
                elif df_name == "sturm_c":
                    sturm_c = pd.concat([sturm_c, df_2100], ignore_index=True)
                elif df_name == "demand_static":
                    demand_static = pd.concat([demand_static, df_2100], ignore_index=True)
                log.info(f"Added 2110 data by copying from 2100 for {df_name}")

    # Step 1: generate new technologies and commodities for Buildings demands (part A, B)
    # Prepare demand data
    commodity_info = cast("MutableMapping", load_package_data("buildings", "commodity.yaml"))
    buildings_commodities = set(commodity_info.keys()) 
    #TODO: another way is to use the add in set.yaml
    demand = pd.concat([sturm_r, sturm_c, demand_static], ignore_index=True)
    demand = demand[demand["commodity"].isin(buildings_commodities)]

    result["demand"] = demand

    # Quit building if the scenario already has Buildings demands
    try:
        existing_commodities = set(scenario.par("demand")["commodity"].unique())
        if existing_commodities & buildings_commodities:
            log.info(f"Scenario already has Buildings demands. Skipping technology generation.")
            return result
    except (KeyError, ValueError):
        pass

    # Mapping from commodity to base model's *_rc technology
    rc_tech_fuel = {"lightoil": "loil_rc", "electr": "elec_rc", "d_heat": "heat_rc"}
    data = defaultdict(list)

    # Generate input, output, capacity_factor, emission_factor, relation_activity for new technologies
    # Deal with 2 exceptions:
    # - Rooftop technologies for input
    # - Lightoil gas
    for fuel in prices["commodity"].unique():
        # Find the original rc technology for the fuel
        tech_orig = rc_tech_fuel.get(fuel, f"{fuel}_rc")

        # Create the technologies for the new commodities
        for commodity in filter(
            re.compile(f"[_-]{fuel}").search, demand["commodity"].unique()
        ):
            # Fix for lightoil gas included
            if "lightoil-gas" in commodity:
                tech_new = f"{fuel}_lg_" + commodity.replace("_lightoil-gas", "")
            else:
                tech_new = f"{fuel}_" + commodity.replace(f"_{fuel}", "")
            log.info(f"  Commodity: {commodity} -> Tech: {tech_new}")

            # Modify data
            for name, filters, extra in (  # type: ignore
                ("input", {}, {}),  # NB value=1.0 is done by adapt_emission_factors()
                ("output", {}, dict(commodity=commodity, value=1.0)),
                ("capacity_factor", {}, {}),
                ("emission_factor", {}, {}),
                ("relation_activity", dict(relation=relations), {}),
            ):
                filters["technology"] = [tech_orig]
                input_data = scenario.par(name, filters=filters).assign(
                    technology=tech_new, **extra
                )
                data[name].append(input_data)
                # Deal with rooftop technologies for input
                # All newly created technologies containing "electr" should have:
                # - M1: electr at level "final" (already exists)
                # - M2: electr at level "final_RT" (add this)
                if name == "input" and len(input_data) > 0:
                    # Check if this is an electricity technology with electr input at final level, M1 mode
                    electr_inputs = input_data[
                        (input_data["technology"].str.contains("electr", regex=False, case=False))
                        & (input_data["commodity"] == "electr")
                        & (input_data["level"] == "final")
                        & (input_data["mode"] == "M1")
                    ].copy()
                    if len(electr_inputs) > 0:
                        # Create M2 inputs with level "final_RT"
                        electr_inputs_m2 = electr_inputs.assign(
                            level="final_RT",
                            mode="M2"
                        )
                        data[name].append(electr_inputs_m2)
                        log.info(f"Added {len(electr_inputs_m2)} M2 input rows for technology {tech_new}")

    tmp = {k: pd.concat(v) for k, v in data.items()}
    
    adapt_emission_factors(tmp)
    merge_data(result, tmp)

    # Step 2: generate new technologies and commodities for Buildings demands (part C)
    try:
        existing_commodities = set(scenario.par("demand")["commodity"].unique())
        if "non-comm" not in existing_commodities:
            log.info("Scenario does not have 'non-comm' demand.")
            # TODO: add the chain to build biomass_nc technologies too
            # TODO: not clear about the logic of keeping which version of non-comm
            return result
    except (KeyError, ValueError):
        pass

    # Step 3: generate new technologies and commodities for the residual rc (part E)
    # - replace rc_spec and rc_therm with afofio_spec and afofio_therm
    # - AFOFIO demand read from CSV files
    afofio_demand = demand_static[demand_static["commodity"].isin(["afofio_spec", "afofio_therm"])]
    result["demand"] = pd.concat([result["demand"], afofio_demand], ignore_index=True)

    # Mapping from original to generated commodity names
    c_map = {f"rc_{name}": f"afofio_{name}" for name in ("spec", "therm")}

    # Create AFOFIO technologies by transforming RC technologies
    # Identify technologies that output to rc_spec or rc_therm
    rc_techs = scenario.par(
        "output", filters={"commodity": ["rc_spec", "rc_therm"]}
    )["technology"].unique()

    def transform_tech_name(tech_name: str) -> str:
        if tech_name.endswith("_RT"):
            # Handle _RC_RT or _rc_RT -> _afofio_RT
            return re.sub("_(rc|RC)_RT$", "_afofio_RT", tech_name)
        else:
            # Handle _RC or _rc at end -> _afofio
            return re.sub("_(rc|RC)$", "_afofio", tech_name)

    replace = {
        "commodity": c_map,
        "technology": {
            t: transform_tech_name(t)
            for t in rc_techs
        },
    }

    t_shares = Quantity(1.0, name="afofio tech share") 
    # 1.0 scaling as actual demand data read in

    merge_data(
        result,
        scale_and_replace(  # type: ignore [arg-type]
            scenario, replace, t_shares, relations=relations, relax=0.05
        ),
    )

    # Step 4: build materials for new constructions and demolitions (part D)
    if with_materials:
        # Set up buildings-materials linkage
        merge_data(result, materials(scenario, info, sturm_r, sturm_c))

    # Step 5: other format check and adjustments
    for key, df in result.items(): # convert year columns in all DataFrames in result dict
        year_cols = [col for col in df.columns if col.startswith("year")]
        if year_cols:
            result[key] = df.astype({col: int for col in year_cols})
    result["demand"] = result["demand"].assign( # assign levels
        level=result["demand"].commodity.apply(
            lambda x: "demand" if ("floor" in x or any(mat in x.lower() for mat in MATERIALS)) else "useful"
        )
    )

    return result


def prune_spec(spec: Spec, data: "ParameterData") -> None:
    """Remove extraneous entries from `spec`."""
    for name in ("commodity", "technology"):
        values = set(data["input"][name]) | set(data["output"][name])

        # DEBUG
        # missing = map(
        #     lambda c: c.id, filter(lambda c: c.id not in values, spec.add.set[name])
        # )
        # print("\n".join(sorted(missing)))

        N = len(spec.add.set[name])
        spec.add.set[name] = sorted(
            filter(lambda c: c.id in values, spec.add.set[name])
        )
        log.info(f"Prune {N - len(spec.add.set[name])} {name} codes with no data")

        missing = values - set(spec.add.set[name]) - set(spec.require.set[name])
        if len(missing):
            log.warning(
                f"Missing {len(missing)} values:\n" + "\n".join(sorted(missing))
            )


def main(
    context: Context,
    scenario: message_ix.Scenario,
    demand: pd.DataFrame,
    prices: pd.DataFrame,
    sturm_r: pd.DataFrame,
    sturm_c: pd.DataFrame,
):
    """Set up the structure and data for MESSAGE_Buildings on `scenario`.

    Parameters
    ----------
    scenario
        Scenario to set up.
    """
    # Info about the `scenario` to be modified. If build.main() has already been run on
    # the scenario, this will reflect that, e.g. will include the structures from
    # buildings/set.yaml.
    info = ScenarioInfo(scenario)

    scenario.check_out()

    try:
        # TODO explain what this is for
        scenario.init_set("time_relative")
    except ValueError:
        pass  # Already exists

    # Generate a spec for the model
    spec = get_spec(context)

    # Prepare data based on the contents of `scenario`
    data = prepare_data(
        scenario,
        info,
        demand,
        prices,
        sturm_r,
        sturm_c,
        context.buildings.with_materials,
        relations=spec.require.set["relation"],
        afofi_demand=None,  # Use calculated AFOFI demand
    )

    # Remove unused commodities and technologies
    prune_spec(spec, data)

    # Simple callback for apply_spec()
    def _add_data(s, **kw):
        return data

    # FIXME check whether this works correctly on the re-solve of a scenario that has
    #       already been set up
    options = dict(fast=True)
    build.apply_spec(scenario, spec, _add_data, **options)

    scenario.set_as_default()

    log.info(f"Built {scenario.url} and set as default")


def materials(
    scenario: message_ix.Scenario,
    info: ScenarioInfo,
    sturm_r: pd.DataFrame,
    sturm_c: pd.DataFrame,
) -> "ParameterData":
    """Integrate MESSAGEix-Buildings with MESSAGEix-Materials.

    This function prepares data for `scenario` to work with :mod:`.model.material`.
    Structural changes (addition/removal of technologies and commodities) are handled
    by :func:`get_spec` and :func:`main`.

    The data is for the "output", "input", and "demand" MESSAGE parameters. It includes:

    1. For new technologies like ``(construction|demolition)_(resid|comm)_build``:

       - For ``construction_*`` technologies, input of the commodities steel, aluminum,
         and cement (cf :data:`BUILD_COMM_CONVERT`) from ``l="product"``, and output to
         ``c="(comm|resid)_floor_construction, l="demand"``.
       - For the ``demolition_*`` technologies, no input, but output to both
         ``c="(comm|resid)_floor_demolition, l="demand"`` *and* commodities (same 3) at
         ``l="end_of_life"``.

    2. Adjusted values for existing "demand" parameter data at ``l="demand"`` for steel,
       aluminum, and cement by subtracting the amounts from ``sturm_r`` and ``sturm_c``.
       The demands are not reduced below zero.
    """
    # Accumulate a list of data frames for each parameter
    result = defaultdict(list)

    # Create new input/output for building material intensities
    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
        mode="M1",
        year_vtg=info.Y,
        year_act=info.Y,
    )

    # Iterate over `BUILD_COMM_CONVERT` and nodes (excluding World and *_GLB)
    # NB probably could vectorize over `n`.
    for c, n in product(BUILD_COMM_CONVERT, nodes_ex_world(info.N)):
        rc, *_, typ, comm = c.split("_")  # First, second-to-last, and last entries

        common.update(node_loc=n, node_origin=n, node_dest=n)

        # Select data for (rc, c, n)
        df_mat = (sturm_r if rc == "resid" else sturm_c).query(
            f"commodity == '{c}' and node == '{n}'"
        )
        # Handle missing years: if 2020 missing use 2025; if 2110 missing use 2100
        for target, source in [(2020, 2025), (2110, 2100)]:
            if target not in df_mat["year"].values:
                df_source = df_mat[df_mat["year"] == source]
                if len(df_source) > 0:
                    df_target = df_source.copy()
                    df_target["year"] = target
                    df_mat = pd.concat([df_mat, df_target], ignore_index=True).sort_values("year")
        eff = pd.concat([df_mat.value, df_mat.value.tail(1)]).iloc[-len(info.Y) :]

        if typ == "demand":
            data = make_io(
                (comm, "product", "t"),
                (f"{rc}_floor_construction", "demand", "t"),
                efficiency=eff,
                on="input",
                technology=f"construction_{rc}_build",
                **common,
            )
        elif typ == "scrap":
            data = make_io(
                (comm, "end_of_life", "t"),  # will be flipped to output
                (f"{rc}_floor_demolition", "demand", "t"),
                efficiency=eff,
                on="input",
                technology=f"demolition_{rc}_build",
                **common,
            )
            # Flip input to output (no input for demolition)
            data["output"] = pd.concat(
                [
                    data["output"],
                    data.pop("input").rename(
                        columns={"node_origin": "node_dest", "time_origin": "time_dest"}
                    ),
                ]
            )

        for name, df in data.items():
            result[name].append(df)

    # Add floor construction and demolition demands from sturm_r and sturm_c if not already present
    expr = "(comm|resid)_floor_(construc|demoli)tion"
    existing = pd.concat(result["demand"], ignore_index=True) if result["demand"] else pd.DataFrame()
    if not (len(existing) > 0 and existing["commodity"].str.fullmatch(expr, na=False).any()):
        floor_demand = pd.concat([
            df[df["commodity"].str.fullmatch(expr, na=False)] 
            for df in [sturm_r, sturm_c] if df is not None and len(df) > 0
        ], ignore_index=True)
        if len(floor_demand) > 0:
            result["demand"].append(floor_demand)
    
    # Use the reusable function to subtract material demand
    # One can change the method parameter to use different approaches:
    # - "bm_subtraction": Building material subtraction (default)
    # - "im_subtraction": Infrastructure material subtraction (to be implemented)
    # - "pm_subtraction": Power material subtraction (to be implemented)
    # - "tm_subtraction": Transport material subtraction (to be implemented)
    mat_demand = subtract_material_demand(
        scenario, info, sturm_r, sturm_c, method="bm_subtraction"
    )

    # Add the modified demand to results
    result["demand"].append(mat_demand)

    # Concatenate data frames together
    return {k: pd.concat(v) for k, v in result.items()}


# works in the same way as main() but applicable for ssp baseline scenarios
def build_B(
    context: Context,
    scenario: message_ix.Scenario,
):
    """Set up the structure and data for MESSAGEix_Buildings on `scenario`.

    Parameters
    ----------
    scenario
        Scenario to set up.
    """
    info = ScenarioInfo(scenario)

    from message_ix_models.model.buildings.config import Config

    config = Config()
    context.buildings = config

    scenario.check_out()

    try:
        # TODO explain what this is for
        scenario.init_set("time_relative")
    except ValueError:
        pass  # Already exists

    # Generate a spec for the model
    spec = get_spec(context)

    # Temporary: input for prepare data seperately read from csv
    # prices
    price_path = private_data_path("buildings", "input_prices_R12.csv")
    prices = pd.read_csv(price_path)

    # sturm_r
    sturm_r_path = private_data_path("buildings", "report_MESSAGE_resid_SSP2_nopol_post.csv")
    # sturm_r_path = package_data_path("buildings", "debug-sturm-resid.csv")
    sturm_r = pd.read_csv(sturm_r_path, index_col=0)

    # sturm_c
    sturm_c_path = private_data_path("buildings", "report_MESSAGE_comm_SSP2_nopol_post.csv")
    # sturm_c_path = package_data_path("buildings", "debug-sturm-comm.csv")
    sturm_c = pd.read_csv(sturm_c_path, index_col=0)
    sturm_c.loc[sturm_c["commodity"].str.contains("other_uses", na=False), "value"] = 0

    # static demand
    demand_static_path = private_data_path("buildings", "static_20251227.csv")
    demand_static = pd.read_csv(demand_static_path, index_col=0)
    demand_static.loc[demand_static["commodity"].str.contains("afofio", na=False), "value"] = 0

    # Prepare data based on the contents of `scenario`
    data = prepare_data_B(
        scenario,
        info,
        prices,
        sturm_r,
        sturm_c,
        demand_static,
        context.buildings.with_materials,
        relations=spec.require.set["relation"],
    )

    # Remove unused commodities and technologies
    prune_spec(spec, data)

    # Simple callback for apply_spec()
    def _add_data(s, **kw):
        return data

    # FIXME check whether this works correctly on the re-solve of a scenario that has
    #       already been set up
    options = dict(fast=True)
    build.apply_spec(scenario, spec, _add_data, **options)

    scenario.set_as_default()

    log.info(f"Built {scenario.url} and set as default")
