import logging
import re
from collections import defaultdict
from copy import deepcopy
from itertools import product
from typing import Dict, Iterable, List, Sequence

import message_ix
import pandas as pd
from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.model import build
from message_ix_models.model.structure import (
    generate_set_elements,
    get_codes,
    get_region_codes,
)
from message_ix_models.util import (
    eval_anno,
    load_private_data,
    make_io,
    merge_data,
    nodes_ex_world,
)
from sdmx.model import Annotation, Code

# from message_data.projects.ngfs.util import add_macro_COVID  # Unused

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

    # Temporary
    s.add.set["technology"].append(Code(id="bio_backstop"))

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


def get_techs(spec: Spec, commodity=None) -> List[str]:
    """Return a list of buildings technologies."""
    codes: Iterable[Code] = spec.add.set["technology"]
    if commodity:
        codes = filter(lambda s: s.id.startswith(commodity), codes)

    return sorted(map(str, codes))


def get_tech_groups(
    spec: Spec, include="commodity enduse", legacy=False
) -> Dict[str, Sequence[str]]:
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
            commodity = eval_anno(t, "input")[0]
        except TypeError:
            pass  # No such annotation

        if legacy:
            # Map to labels used in legacy reporting
            commodity = {
                "d_heat": "heat",
                "electr": "elec",
                "ethanol": "eth",
                "fueloil": "foil",
                "hydrogen": "h2",
                "lightoil": "loil",
                "methanol": "meth",
                "solar_pv": "solar",
            }.get(commodity, commodity)

        # Update lists
        _store(match.string, commodity, enduse, sector)
        # Also update "rc" totals
        _store(match.string, commodity, enduse, "rc")

    return techs


def load_config(context):
    if "buildings spec" in context:
        return

    set_info = load_private_data("buildings", "set.yaml")

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
        s.add.set["commodity"].append(Code(id=c.id.replace("rc_", "afofi_")))

    # Generate technologies that replace corresponding *_rc|RC in the base model
    expr = re.compile("^RC|(?<=_)(rc|RC)$")
    for t in filter(lambda x: expr.search(x.id), get_codes("technology")):
        s.add.set["technology"].append(
            Code(
                id=expr.sub("afofi", t.id),
                annotations=[Annotation(id="derived-from", text=t.id)],
            )
        )
        s.remove.set["technology"].append(t)

    # Store
    context["buildings spec"] = s


def bio_backstop(scen):
    """Fill the gap between the biomass demands & potential to avoid infeasibility.

    .. todo:: Replace this with proper & complete use of the current
       :mod:`message_data.tools.utilities.add_globiom`.

       This function simplified from a version in the MESSAGE_Buildings/util/ directory,
       itself modified from an old/outdated (before 2022-03) version of
       :mod:`.add_globiom`.

       See https://iiasa-ece.slack.com/archives/C03M5NX9X0D/p1659623091532079 for
       discussion.
    """
    # Retrieve technology for which will be used to create the backstop
    filters = dict(technology="elec_rc", node_loc="R12_NAM", year_act=2020)

    data = defaultdict(list)

    for node, name in product(["R12_AFR", "R12_SAS"], ["output", "var_cost"]):
        values = dict(technology="bio_backstop", node_loc=node)

        if name == "output":
            values.update(commodity="biomass", node_dest=node, level="primary")
        elif name == "var_cost":
            values.update(value=1e5)

        data[name].append(scen.par(name, filters=filters).assign(**values))

    result = {k: pd.concat(v) for k, v in data.items()}
    log.info(repr(result))
    return result


def prepare_data(
    scenario: message_ix.Scenario,
    info: ScenarioInfo,
    demand: pd.DataFrame,
    prices: pd.DataFrame,
    sturm_r: pd.DataFrame,
    sturm_c: pd.DataFrame,
    with_materials: bool,
    relations: List[str],
) -> Dict[str, pd.DataFrame]:
    """Derive data for MESSAGEix-Buildings from `scenario`."""
    from utils.rc_afofi import return_PERC_AFOFI  # type: ignore

    # Accumulate a list of data frames for each parameter
    result = defaultdict(list)

    # Use code from the MESSAGE_Buildings repo that queries the ECE IEA database
    perc_afofi_therm, perc_afofi_spec = return_PERC_AFOFI()

    # Create new demands for AFOFI based on percentages between 2010 and 2015.
    # NB on a second pass (after main() has already run once), rc_spec and rc_therm have
    #    been stripped out, so `dd_replace` and `afofi_dd` are empty.
    afofi_dd = scenario.par(
        "demand", filters={"commodity": ["rc_spec", "rc_therm"], "year": info.Y}
    ).assign(commodity=lambda df: df["commodity"].str.replace("rc", "afofi"))

    # NB(PNK) this probably does not need to be a loop
    for reg in perc_afofi_therm.index:
        # Boolean mask for rows matching this `reg`
        mask = afofi_dd["node"].str.endswith(reg)

        # NB(PNK) this could probably be simplified using groupby()
        afofi_dd.loc[mask & (afofi_dd.commodity == "afofi_therm"), "value"] = (
            afofi_dd.loc[mask & (afofi_dd.commodity == "afofi_therm"), "value"]
            * perc_afofi_therm.loc[reg][0]
        )
        afofi_dd.loc[mask & (afofi_dd.commodity == "afofi_spec"), "value"] = (
            afofi_dd.loc[mask & (afofi_dd.commodity == "afofi_spec"), "value"]
            * perc_afofi_spec.loc[reg][0]
        )
    result["demand"].append(afofi_dd)

    # Copy technology parameter values from rc_spec and rc_therm to new afofi. Again,
    # once rc_(spec|therm) are stripped, .par() returns nothing here, so rc_techs is
    # empty and the following loop does not run

    rc_techs = scenario.par("output", filters={"commodity": ["rc_spec", "rc_therm"]})[
        "technology"
    ].unique()

    # NB(PNK) this probably does not need to be a loop
    for tech_orig in rc_techs:
        # Filters for retrieving data
        filters = dict(filters={"technology": tech_orig})

        # Derived name of new technology
        tech_new = re.sub("(rc|RC)", "afofi", tech_orig)

        # Copy data for input, capacity_factor, and emission_factor
        for name in ("input", "capacity_factor", "emission_factor"):
            result[name].append(
                scenario.par(name, **filters).assign(technology=tech_new)
            )

        # Replace commodity name in output
        name = "output"
        result[name].append(
            scenario.par(name, **filters).assign(
                technology=tech_new,
                commodity=lambda df: df["commodity"].str.replace("rc", "afofi"),
            )
        )

        # Only copy relation_activity data for certain relations
        name = "relation_activity"
        filters["filters"].update(relation=relations)
        result[name].append(scenario.par(name, **filters).assign(technology=tech_new))

    # Create new technologies for building energy

    # Mapping from commodity to base model's *_rc technology
    rc_tech_fuel = {"lightoil": "loil_rc", "electr": "elec_rc", "d_heat": "heat_rc"}

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
                ("input", {}, dict(value=1.0)),
                ("output", {}, dict(commodity=commodity, value=1.0)),
                ("capacity_factor", {}, {}),
                ("emission_factor", {}, {}),
                ("relation_activity", dict(relation=relations), {}),
            ):
                filters["technology"] = tech_orig
                result[name].append(
                    scenario.par(name, filters=filters).assign(
                        technology=tech_new, **extra
                    )
                )

    # Concatenate data frames together
    data = {k: pd.concat(v) for k, v in result.items()}
    log.info(
        "Prepared:\n" + "\n".join(f"{len(v)} obs for {k!r}" for k, v in data.items())
    )

    if with_materials:
        # Set up buildings-materials linkage
        merge_data(data, materials(scenario, info, sturm_r, sturm_c))

    merge_data(data, bio_backstop(scenario))

    return data


def prune_spec(spec: Spec, data: Dict[str, pd.DataFrame]) -> None:
    """Remove extraneous entries from `spec`."""
    for name in ("commodity", "technology"):
        values = set(data["input"][name]) | set(data["output"][name])

        # DEBUG
        # print(
        #     "\n".join(
        #         sorted(
        #             map(
        #                 lambda c: c.id,
        #                 filter(lambda c: c.id not in values, spec.add.set[name]),
        #             )
        #         )
        #     )
        # )

        N = len(spec.add.set[name])
        spec.add.set[name] = sorted(
            filter(lambda c: c.id in values, spec.add.set[name])
        )
        log.info(f"Prune {N-len(spec.add.set[name])} {name} codes with no data")

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
) -> Dict[str, pd.DataFrame]:
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
                technology=f"construction_{rc}_build",
                **common,
            )
        elif typ == "scrap":
            data = make_io(
                (comm, "end_of_life", "t"),  # will be flipped to output
                (f"{rc}_floor_demolition", "demand", "t"),
                efficiency=eff,
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
                    f"{rc}_mat_demand_(cement|steel|aluminum)"
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
