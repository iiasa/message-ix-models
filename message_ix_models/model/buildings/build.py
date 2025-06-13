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
        new.id = c.id.replace("rc_", "afofi_")
        s.add.set["commodity"].append(new)

    # Generate technologies that replace corresponding *_rc|RC in the base model
    expr = re.compile("_(rc|RC)$")
    for t in filter(lambda x: expr.search(x.id), get_codes("technology")):
        # Generate a new Code object, preserving annotations
        new = deepcopy(t)
        new.id = expr.sub("_afofi", t.id)
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

            # commented: for debugging
            # print(f"{fuel = }", f"{commodity = }", f"{tech_new = }", sep="\n")

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
        # Input or output efficiency:
        # - Duplicate the final (2100) value for 2110.
        # - Take a number of values corresponding to len(info.Y), allowing the first
        #   model year to be 2020 or 2025.
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

    # Retrieve data once
    mat_demand = scenario.par("demand", {"level": "demand"})
    index_cols = ["node", "year", "commodity"]

    # Subtract building material demand from existing demands in scenario
    for rc, base_data, how in (("resid", sturm_r, "right"), ("comm", sturm_c, "outer")):
        new_col = f"demand_{rc}_const"

        # - Drop columns.
        # - Rename "value" to e.g. "demand_resid_const".
        # - Extract MESSAGEix-Materials commodity name from STURM commodity name.
        # - Drop other rows.
        # - Set index.
        df = (
            base_data.drop(columns=["level", "time", "unit"])
            .rename(columns={"value": new_col})
            .assign(
                commodity=lambda _df: _df.commodity.str.extract(
                    f"{rc}_mat_demand_(cement|steel|aluminum)", expand=False
                )
            )
            .dropna(subset=["commodity"])
            .set_index(index_cols)
        )

        # Merge existing demands at level "demand".
        # - how="right": drop all rows in par("demand", …) that have no match in `df`.
        # - how="outer": keep the union of rows in `mat_demand` (e.g. from sturm_r) and
        #   in `df` (from sturm_c); fill NA with zeroes.
        mat_demand = mat_demand.join(df, on=index_cols, how=how).fillna(0)

    # False if main() is being run for the second time on `scenario`
    first_pass = "construction_resid_build" not in info.set["technology"]

    # If not on the first pass, this modification is already performed; skip
    if first_pass:
        # - Compute new value = (existing value - STURM values), but no less than 0.
        # - Drop intermediate column.
        # - Add to combined data.
        result["demand"].append(
            mat_demand.eval("value = value - demand_comm_const - demand_resid_const")
            .assign(value=lambda df: df["value"].clip(0))
            .drop(columns=["demand_comm_const", "demand_resid_const"])
        )

    # Concatenate data frames together
    return {k: pd.concat(v) for k, v in result.items()}
