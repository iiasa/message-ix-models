"""Demand calculation for MESSAGEix-Transport."""
import logging
from functools import partial
from pathlib import Path

import genno.computations
import message_ix
import numpy as np
import pandas as pd
from dask.core import quote
from genno import Computer, Key, KeyExistsError
from ixmp.reporting import RENAME_DIMS
from message_ix import make_df
from message_ix.reporting import Reporter
from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.util import adapt_R11_R14, broadcast, check_support

from message_data.model.transport import computations
from message_data.model.transport.build import generate_set_elements
from message_data.model.transport.data.groups import get_consumer_groups
from message_data.model.transport.plot import DEMAND_PLOTS
from message_data.model.transport.utils import path_fallback
from message_data.tools import gdp_pop

log = logging.getLogger(__name__)


def dummy(info):
    """Dummy demands.

    Parameters
    ----------
    info : .ScenarioInfo
    """
    common = dict(
        year=info.Y,
        value=10 + np.arange(len(info.Y)),
        level="useful",
        time="year",
    )

    dfs = []

    for commodity in generate_set_elements("commodity"):
        try:
            commodity.get_annotation(id="demand")
        except KeyError:
            # Not a demand commodity
            continue

        dfs.append(
            make_df(
                "demand",
                commodity=commodity.id,
                unit="t km" if "freight" in commodity.id else "km",
                **common,
            )
        )

    # # Dummy demand for light oil
    # common['level'] = 'final'
    # dfs.append(
    #     make_df('demand', commodity='lightoil', **common)
    # )

    return pd.concat(dfs).pipe(broadcast, node=info.N[1:])


def from_scenario(scenario: message_ix.Scenario) -> Reporter:
    """Return a Reporter for calculating demand based on `scenario`.

    Parameters
    ----------
    Scenario
        Solved Scenario

    Returns
    -------
    Reporter
    """
    rep = Reporter.from_scenario(scenario)

    prepare_reporter(rep, Context.get_instance())

    return rep


def from_external_data(info: ScenarioInfo, context: Context) -> Computer:
    """Return a Reporter for calculating demand from external data."""
    c = Computer()
    prepare_reporter(c, context, exogenous_data=True, info=info)

    return c


def add_exogenous_data(c: Computer, context: Context) -> None:
    """Add exogenous data to `c` that mocks data coming from an actual Scenario.

    The specific quantities added are:

    - ``GDP:n-y``, from GEA, SSP, or SHAPE data; see :func:`.gdp_pop`.
    - ``PRICE_COMMODITY:n-c-y``, currently mocked based on the shape of ``GDP:n-y``
      using :func:`.dummy_prices`.

      .. todo:: Add an external data source.

    - ``MERtoPPP:n-y``, from :file:`mer-to-ppp.csv`. If ``context.regions`` is “R14”,
      data are adapted from R11 using :func:`.adapt_R11_R14`.

    See also
    --------
    :doc:`/reference/model/transport/data`
    """
    check_support(
        context,
        settings=dict(regions=frozenset(["R11", "R14"])),
        desc="Exogenous data for demand projection",
    )

    si = dict(sums=True, index=True)  # Shorthand

    # Data from files. Add 3 computations per quantity.
    for key, basename, units in (
        # (gdp_k, "gdp", "GUSD/year"),  # Handled below
        (Key("MERtoPPP", "ny"), "mer-to-ppp", ""),
    ):
        # 1. Load the file
        k1 = Key(key.name, tag="raw")
        c.add(
            k1,
            partial(genno.computations.load_file, units=units),
            path_fallback("R11", f"{basename}.csv"),
        )

        # 2. Rename dimensions
        k2 = key.add_tag("R11")
        c.add(k2, computations.rename, k1, quote(RENAME_DIMS))

        # 3. Maybe transform from R11 to another node list
        if context.regions == "R11":
            c.add(key, k2, **si)  # No-op/pass-through
        elif context.regions == "R14":
            c.add(key, adapt_R11_R14, k2, **si)

    gdp_keys = c.add("GDP:n-y", gdp_pop.gdp, "y", "config", **si)
    c.add("PRICE_COMMODITY:n-c-y", (computations.dummy_prices, gdp_keys[0]), **si)


def add_structure(c: Computer, context: Context, info: ScenarioInfo):
    """Add keys to `c` for model structure required by demand computations.

    This uses `info` to mock the contents that would be reported from an already-
    populated Scenario for sets "node", "year", and "cat_year".
    """
    # `info` contains only structure to be added, not existing/required structure. Add
    # information about the year dimension, to be used below.
    # TODO accomplish this by 'merging' the ScenarioInfo/spec.
    if not len(info.set["years"]):
        info.year_from_codes(get_codes(f"year/{context.years}"))
    if not len(info.set["node"]):
        nodes = get_codes(f"node/{context.regions}")
        info.set["node"] = nodes[nodes.index("World")].child

    for key, value in (
        ("n", quote(list(map(str, info.set["node"])))),
        ("nodes", quote(info.set["node"])),
        ("y", quote(info.set["year"])),
        (
            "cat_year",
            pd.DataFrame([["firstmodelyear", info.y0]], columns=["type_year", "year"]),
        ),
    ):
        try:
            # strict=True to raise an exception if `key` exists
            c.add(key, value, strict=True)
        except KeyExistsError:
            # Already present; don't overwrite
            continue


def prepare_reporter(
    rep: Computer,
    context: Context,
    configure: bool = True,
    exogenous_data: bool = False,
    info: ScenarioInfo = None,
) -> None:
    """Prepare `rep` for calculating transport demand.

    Parameters
    ----------
    rep : Reporter
        Must contain the keys ``<GDP:n-y>``, ``<MERtoPPP:n-y>``.
    """
    if configure:
        # Configure the reporter; keys are stored
        rep.configure(transport=context["transport config"])

    # Add message_data.model.transport.computations to look up computation names
    if computations not in rep.modules:
        rep.modules.append(computations)

    # Always ensure structure is available
    add_structure(rep, context, info)

    if exogenous_data:
        add_exogenous_data(rep, context)

    # Transfer transport config to the Reporter
    rep.graph["config"].update(
        {
            "data source": {
                k: context["transport config"]["data source"][k]
                for k in ("gdp", "population")
            },
            "output_path": context.get("output_path", Path.cwd()),
            "regions": context.regions,
        }
    )

    # Keys to refer to quantities
    # Existing keys, prepared by from_scenario() or from_external_data()
    gdp = rep.full_key("GDP")
    mer_to_ppp = rep.full_key("MERtoPPP")
    price_full = rep.full_key("PRICE_COMMODITY").drop("h", "l")

    # Keys for new quantities
    pop = Key("population", "ny")
    cg = Key("cg share", "n y cg".split())
    gdp_ppp = Key("GDP", "ny", "PPP")
    gdp_ppp_cap = gdp_ppp.add_tag("capita")
    pdt_nyt = Key("transport pdt", "nyt")  # Total PDT shared out by mode
    pdt_cap = pdt_nyt.drop("t").add_tag("capita")
    pdt_ny = pdt_nyt.drop("t").add_tag("total")
    price_sel = price_full.add_tag("transport")
    price = price_sel.add_tag("smooth")
    cost = Key("cost", "nyct")
    sw = Key("share weight", "nty")

    _ = dict()

    queue = [
        # Values based on configuration
        (("speed", "speed:t", "config"), _),
        (("whour", "whour:", "config"), _),
        (("_lambda", "lambda:", "config"), _),
        # List of nodes excluding "World"
        # TODO move upstream to message_ix
        (("nodes_ex_world", "n:ex world", "n"), _),
        (("nodes_ex_world", "n:ex world+code", "nodes"), _),
        # List of model years
        (("model_periods", "y:model", "y", "cat_year"), _),
        # Base share data
        (("base_shares", "base shares:n-t-y", "n:ex world", "y", "config"), _),
        # Population data; data source according to config
        ((pop, partial(gdp_pop.population, extra_dims=False), "y", "config"), _),
        # Consumer group sizes
        # TODO ixmp is picky here when there is no separate argument to the callable;
        # fix.
        ((cg, get_consumer_groups, quote(context)), _),
        # PPP GDP, total and per capita
        (("product", gdp_ppp, gdp, mer_to_ppp), _),
        (("ratio", gdp_ppp_cap, gdp_ppp, pop), _),
        # Total demand
        (("pdt_per_capita", pdt_cap, gdp_ppp_cap, "config"), _),
        (("product", pdt_ny, pdt_cap, pop), _),
        # Value-of-time multiplier
        (("votm", "votm:n-y", gdp_ppp_cap), _),
        # Select only the price of transport services
        # FIXME should be the full set of prices
        (("select", price_sel, price_full, dict(c="transport")), _),
        # Smooth prices to avoid zig-zag in share projections
        (("smooth", price, price_sel), _),
        # Transport costs by mode
        (
            (
                "cost",
                cost,
                price,
                gdp_ppp_cap,
                "whour:",
                "speed:t",
                "votm:n-y",
                "y:model",
            ),
            _,
        ),
        # Share weights
        (
            (
                "share_weight",
                sw,
                "base shares:n-t-y",
                gdp_ppp_cap,
                cost,
                "lambda:",
                "n:ex world",
                "y:model",
                "t:transport",
                "cat_year",
                "config",
            ),
            _,
        ),
        # Shares
        (
            (
                "shares:n-t-y",
                partial(computations.logit, dim="t"),
                cost,
                sw,
                "lambda:",
                "y:model",
            ),
            _,
        ),
        # Total PDT shared out by mode
        (("product", pdt_nyt, pdt_ny, "shares:n-t-y"), _),
        # Per capita
        (("ratio", "transport pdt:n-y-t:capita", pdt_nyt, pop), dict(sums=False)),
        # LDV PDT only
        (("select", "transport ldv pdt:n-y:total", pdt_nyt, dict(t=["LDV"])), _),
        # LDV PDT shared out by consumer group
        (
            (
                "product",
                "transport ldv pdt",
                "transport ldv pdt:n-y:total",
                cg,
            ),
            _,
        ),
    ]

    # Plots
    queue.extend(
        [((f"plot {plot.basename}", plot.make_task()), _) for plot in DEMAND_PLOTS]
    )

    rep.add_queue(queue)
