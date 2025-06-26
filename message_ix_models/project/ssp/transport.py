"""Postprocess aviation emissions for SSP 2024."""

import logging
import re
from collections.abc import Hashable
from enum import Enum, auto
from functools import cache
from typing import TYPE_CHECKING, Literal, Optional

import genno
import pandas as pd
from genno import Key

from message_ix_models import Context
from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.iamc import iamc_like_data_for_query, to_quantity
from message_ix_models.util import minimum_version
from message_ix_models.util.genno import Keys

if TYPE_CHECKING:
    import pathlib

    import sdmx.model.common
    from genno import Computer
    from genno.types import AnyQuantity, TQuantity

log = logging.getLogger(__name__)

#: Dimensions of several quantities.
DIMS = "e n t y UNIT".split()

#: Expression used to select and extract :math:`(e, t)` dimension coordinates from
#: variable codes in :func:`v_to_emi_coords`.
EXPR_EMI = re.compile(
    r"^Emissions\|(?P<e>[^\|]+)\|Energy\|Demand\|(?P<t>(Bunkers|Transportation).*)$"
)

#: Expression used to select and extract :math:`(c)` dimension coordinates from variable
#: codes in :func:`v_to_fe_coords`.
EXPR_FE = re.compile(
    r"""^Final.Energy\|
    (?P<t>Bunkers(\|International.Aviation)?|(Transportation(|.\(w/.bunkers\))))
    \|?
    (?P<c>|Liquids\|Oil)
    $""",
    flags=re.VERBOSE,
)

#: Keywords for :func:`.iamc_like_data_for_query` / :func:`.to_quantity`.
IAMC_KW = dict(non_iso_3166="keep", query="Model != ''", unique="MODEL SCENARIO")

#: Common label / :attr:`.Key.name` / :attr:`.Key.tag`.
L = "AIR emi"

#: Fixed keys prepared by :func:`.get_computer` and other functions:
#:
#: - :py:`.bcast`: the output of :func:`.broadcast_t`.
#: - :py:`.input`: input data from file or calling code, converted to Quantity.
#: - :py:`.emi`: computed aviation emissions.
#: - :py:`.emi_in`: input data for aviation and other transport emissions, to be
#:   adjusted or overwritten.
#: - :py:`.fe`: computed final energy data.
#: - :py:`.fe_in`: input data for transport final energy, to be adjusted or overwritten.
K = Keys(
    bcast=f"broadcast:t:{L}",
    input=f"input:n-y-VARIABLE-UNIT:{L}",
    emi=f"emission:e-n-t-y-UNIT:{L}",
    emi_in=f"emission:e-n-t-y-UNIT:{L}+in",
    fe_in=f"fe:c-n-t-y:{L}+in",
    fe_out=f"fe:c-n-t-y:{L}+out",
)


class METHOD(Enum):
    """Method for computing emissions."""

    #: See :func:`.method_A`.
    A = auto()
    #: See :func:`.method_B`.
    B = auto()
    #: See :func:`.method_C`.
    C = auto()


def aviation_emi_share(ref: "TQuantity") -> "TQuantity":
    """Return (dummy) data for the share of aviation in emissions.

    Currently this returns exactly the value `0.2`.

    Parameters
    ----------
    ref :
        Reference quantity. The dimensions and coordinates :math:`(n, e, y)` of the
        returned value exactly match `ref`.

    Returns
    -------
    genno.Quantity
        with dimensions :math:`(n, e, y)`.
    """
    return (
        genno.Quantity(0.2, units="dimensionless")
        .expand_dims({"e": sorted(ref.coords["e"].data)})
        .expand_dims({"n": sorted(ref.coords["n"].data)})
        .expand_dims({"y": sorted(ref.coords["y"].data)})
    )


def broadcast_t_emi(
    version: Literal[1, 2], include_international: bool
) -> "AnyQuantity":
    """Quantity to re-add the |t| dimension for emission data.

    Parameters
    ----------
    version :
        Version of ‘variable’ names supported by the current module.
    include_international :
        If :any:`True`, include "Transportation|Aviation|International" with magnitude
        1.0. Otherwise, omit.

    Return
    ------
    genno.Quantity
        with dimension "t".

        If :py:`version=1`, the values include:

        - +1.0 for t="Transportation|Aviation", a label with missing data.
        - -1.0 for t="Transportation|Road Rail and Domestic Shipping", a label with
          existing data from which the aviation total must be subtracted.

        If :py:`version=2`, the values include:

        - +1.0 for t="Bunkers" and t="Bunkers|International Aviation", labels with zeros
          in the input data file.
        - -1.0 for t="Transportation" and t="Transportation|Road Rail and Domestic
          Shipping", labels with existing data from which the aviation total must be
          subtracted.
    """
    if version == 1:  # pragma: no cover
        value = [1, -1, 1]
        t = [
            "Transportation|Aviation",
            "Transportation|Road Rail and Domestic Shipping",
            "Transportation|Aviation|International",
        ]
        idx = slice(None) if include_international else slice(-1)
    elif version == 2:
        value = [1, 1, -1, -1]
        t = [
            "Bunkers",
            "Bunkers|International Aviation",
            "Transportation",
            "Transportation|Road Rail and Domestic Shipping",
        ]
        idx = slice(None)

    return genno.Quantity(value[idx], coords={"t": t[idx]})


def broadcast_t_fe() -> "AnyQuantity":
    """Quantity to re-add the |t| dimension for final energy data."""
    return genno.Quantity(
        pd.DataFrame(
            [
                ["lightoil", "Bunkers", "", +1.0],
                ["lightoil", "Bunkers|International Aviation", "", +1.0],
                ["lightoil", "Bunkers", "Liquids|Oil", +1.0],
                ["lightoil", "Transportation", "", -1.0],
                ["lightoil", "Transportation", "Liquids|Oil", -1.0],
            ],
            columns=["c", "t", "c_new", "value"],
        ).set_index(["c", "t", "c_new"])["value"]
    )


def e_UNIT(cl_emission: "sdmx.model.common.Codelist") -> "AnyQuantity":
    """Return a quantity for broadcasting.

    Returns
    -------
    genno.Quantity
        with one value :math:`Q_{e, UNIT} = 1.0` for every label |e| in
        `cl_emission`, with "UNIT" being the unit expression to be used with IAMC-
        structured data.

        Values are everywhere 1.0, except for species such as ``N2O`` that must be
        reported in kt rather than Mt.
    """
    # Iterate over codes in the codelist
    data = []
    for e in cl_emission:
        # Retrieve info from annotations
        i = {}
        for k, default in {"report": e.id, "unit-species": e.id, "units": "Mt"}.items():
            try:
                i[k] = str(e.get_annotation(id=k).text)
            except KeyError:
                i[k] = default

        scale_factor = 1.0 if i["units"] == "Mt" else 1e3
        data.append([i["report"], f"{i['units']} {i['unit-species']}/yr", scale_factor])

    dims = "e UNIT value".split()
    return genno.Quantity(
        pd.DataFrame(data, columns=dims).set_index(dims[:-1])[dims[-1]]
    )


def finalize(
    q_all: "TQuantity",
    q_emi_update: "TQuantity",
    q_fe_update: "TQuantity",
    model_name: str,
    scenario_name: str,
) -> pd.DataFrame:
    """Finalize output.

    1. Reattach "Model" and "Scenario" labels.
    2. Reassemble the "Variable" dimension/coords of `q_update`; drop "e" and "t".
    3. Convert both `q_all` and `q_update` to :class:`pandas.Series`; update the former
       with the contents of the latter. This retains all other, unmodified data in
       `q_all`.
    4. Adjust to IAMC ‘wide’ structure.

    Parameters
    ----------
    q_all :
        All data. Quantity with dimensions :math:`(n, y, UNIT, VARIABLE)`.
    q_update :
        Revised data to overwrite corresponding values in `q_all`. Quantity with
        dimensions :data:`DIMS`.
    """

    def _expand(qty):
        return qty.expand_dims(
            {"Model": [model_name], "Scenario": [scenario_name]}
        ).rename({"n": "Region", "UNIT": "Unit", "VARIABLE": "Variable"})

    # Convert `q_all` to pd.Series
    s_all = q_all.pipe(_expand).to_series()

    # - Convert `q_emi_update` to pd.Series
    # - Reassemble "Variable" codes.
    # - Drop dimensions (e, t).
    # - Align index with s_all.
    s_emi_update = (
        q_emi_update.pipe(_expand)
        .to_frame()
        .reset_index()
        .assign(
            Variable=lambda df: "Emissions|" + df["e"] + "|Energy|Demand|" + df["t"]
        )
        .drop(["e", "t"], axis=1)
        .set_index(s_all.index.names)[0]
        .rename("value")
    )
    log.info(f'{len(s_emi_update)} obs to update for Variable="Emission|…"')

    # Likewise for q_fe_update
    dim = {"UNIT": [f"{q_fe_update.units:~}".replace("EJ / a", "EJ/yr")]}
    s_fe_update = (
        q_fe_update.expand_dims(dim=dim)
        .pipe(_expand)
        .to_frame()
        .reset_index()
        .assign(
            Variable=lambda df: ("Final Energy|" + df["t"] + "|" + df["c"]).str.replace(
                r"\|$", "", regex=True
            )
        )
        .drop(["c", "t"], axis=1)
        .set_index(s_all.index.names)[0]
        .rename("value")
    )
    log.info(f'{len(s_fe_update)} obs to update for Variable="Final Energy|…"')

    # - Concatenate s_all, s_emi_update, and s_fe_update as columns of a data frame.
    #   The result has the superset of the indices of the arguments.
    # - Fill along axes. Values from s_*_update end up in the last column.
    # - Select the last column.
    # - Reshape to wide format.
    # - Rename index levels and restore to columns.
    return (
        pd.concat([s_all, s_emi_update, s_fe_update], axis=1)
        .ffill(axis=1)
        .iloc[:, -1]
        .unstack("y")
        .reorder_levels(["Model", "Scenario", "Region", "Variable", "Unit"])
        .reset_index()
    )


@minimum_version("message_ix_models.model.transport.build.get_computer")
def get_computer(
    row0: "pd.Series", method: METHOD, *, platform_name: Optional[str] = None
) -> "Computer":
    """Prepare `c` to process aviation emissions data.

    Parameters
    ----------
    row0 :
        A single sample row of the input data. "Model" and "Scenario" must be in the
        index; these are used to reconstruct the IAMC data structure.
    method :
        Select the calculation method.
    platform_name :
        Configured name of a :class:`.Platform` containing solved MESSAGEix-Transport
        scenarios.

    Returns
    -------
    Computer
        Calling :py:`c.get("target")` triggers the calculation of the result.
    """
    from message_ix_models.model import Config as ModelConfig
    from message_ix_models.model.transport import Config as TransportConfig
    from message_ix_models.model.transport import workflow

    # Create a Computer instance
    c = genno.Computer()
    c.require_compat("message_ix_models.report.operator")

    # Create a Context instance. Only R12 is supported.
    context = Context(model=ModelConfig(regions="R12"))
    # Store in `c` for reference by other operations
    c.add("context", context)
    c.graph["config"].update(regions="R12")

    # Store a model name and scenario name from a single row of the data
    model_name, scenario_name = row0[["Model", "Scenario"]]
    c.add("model name", genno.quote(model_name))
    c.add("scenario name", genno.quote(scenario_name))

    # For method_C
    context.core.dest_scenario["model"] = "ci nightly"
    context.core.platform_info.setdefault("name", platform_name or "ixmp-dev")
    context.report.register("model.transport")

    # For method_C, identify the URL of a solved MESSAGEix-Transport scenario from which
    # to retrieve transport data. These steps mirror .transport.workflow.generate().
    # Retrieve a Code with annotations describing the transport scenario.
    sc = get_scenario_code(model_name, scenario_name)
    # - Create and store a .transport.Config instance.
    # - Update it using the `sc`.
    # - Retrieve a 'label' used to construct a target scenario URL.
    label_full = TransportConfig.from_context(context).use_scenario_code(sc)[1]
    # Construct the target scenario URL
    url = workflow.scenario_url(context, label_full)
    # Optionally apply a regex substitution
    URL_SUB = {
        "LED-SSP1": ("$", "#162"),  # Point to a specific version
        "LED-SSP2": ("$", "#171"),
        "SSP1": ("$", "#771"),
        "SSP2": ("$", "#869"),
        "SSP3": ("$", "#686"),
        "SSP4": ("$", "#639"),
        "SSP5": ("$", "#649"),
        # "SSP5": ("(SSP_2024.5) baseline$", r"\1 baseline#525"),  # Other scenario name
    }
    if pattern_repl := URL_SUB.get(sc.id):
        url = re.sub(pattern_repl[0], pattern_repl[1], url)
    # Use the URL to update context.core.scenario_info
    context.handle_cli_args(url=url)

    log.info(f"method 'C' will use data from {url}")

    # Common structure and utility quantities used by method_[ABC]
    c.add(K.bcast, broadcast_t_emi, version=2, include_international=method == "A")

    # Placeholder for data-loading task. This is filled in later by process_df() or
    # process_file().
    c.add(K.input, None)

    # Select and transform data matching EXPR_EMI
    # Filter on "VARIABLE", extract the (e, t) dimensions
    c.add(K.emi_in[0], "select_expand", K.input, dim_cb={"VARIABLE": v_to_emi_coords})
    # Assign units
    c.add(K.emi_in, "assign_units", K.emi_in[0], units="Mt/year")

    # Select and transform data matching EXPR_FE
    # Filter on "VARIABLE", extract the (c, t) dimensions
    dim_cb = {"VARIABLE": v_to_fe_coords}
    c.add(K.fe_in[0] * "UNITS", "select_expand", K.input, dim_cb=dim_cb)
    # Convert "UNIT" dim labels to Quantity.units
    c.add(K.fe_in[1], "unique_units_from_dim", K.fe_in[0] * "UNITS", dim="UNIT")
    # Change labels; see get_label()
    c.add(K.fe_in, "relabel", K.fe_in[1], labels=get_labels())

    # Call a function to prepare the remaining calculations up to K.emi
    method_func = {METHOD.A: method_A, METHOD.B: method_B, METHOD.C: method_C}[method]
    method_func(c)

    # Adjust the original data by adding the (maybe negative) prepared values at K.emi
    c.add(K.emi["adj"], "add", K.emi_in, K.emi)
    c.add(K.fe_out["adj"], "add", K.fe_in[1], K.fe_out)

    # Add a key "target" to:
    # - Collapse to IAMC "VARIABLE" dimension name.
    # - Recombine with other/unaltered original data.
    c.add(
        "target",
        finalize,
        K.input,
        K.emi["adj"],
        K.fe_out["adj"],
        "model name",
        "scenario name",
    )

    return c


@cache
def get_labels():
    """Return mapper for relabelling input data:

    - c[ommodity]: 'Liquids|Oil' (IAMC 'variable' component) → 'lightoil'.
    - n[ode]: "AFR" → "R12_AFR" etc. "World" is not changed.
    """
    cl = get_codelist("node/R12")
    labels = dict(c={"Liquids|Oil": "lightoil", "": "_T"}, n={})
    for n in filter(lambda n: len(n.child) and n.id != "World", cl):
        labels["n"][n.id.partition("_")[2]] = n.id
    return labels


def get_scenario_code(model_name: str, scenario_name: str) -> "sdmx.model.common.Code":
    """Return a specific code from ``CL_TRANSPORT_SCENARIO``.

    See :func:`.get_cl_scenario`. This function handles (`model_name`, `scenario_name`)
    combinations seen in base model outputs as of 2025-04-02.
    """
    from message_ix_models.model.transport.config import get_cl_scenario

    model_parts = model_name.split("_")

    if model_parts[:2] == ["SSP", "LED"]:
        code_id = "LED-SSP2" if scenario_name.startswith("SSP2") else "LED-SSP1"
    else:
        code_id = model_parts[1]

    return get_cl_scenario()[code_id]


def method_A(c: "Computer") -> None:
    """Prepare calculations up to :data:`K.emi` using :data:`METHOD.A`.

    This method uses a fixed share of data for
    variable=``Emissions|*|Energy|Demand|Transportation``.

    1. Select data with variable names matching :data:`EXPR_EMI`.
    2. Calculate (identical) values for:

       - ``Emissions|*|Energy|Demand|Transportation|Aviation``
       - ``Emissions|*|Energy|Demand|Transportation|Aviation|International``

       …as the product of :func:`aviation_emi_share` and
       ``Emissions|*|Energy|Demand|Transportation``.
    3. Subtract (2) from:
       ``Emissions|*|Energy|Demand|Transportation|Road Rail and Domestic Shipping``
    """
    # Select the total
    indexers = dict(t="Transportation")
    c.add(K.emi[0] / "t", "select", K.emi_in, indexers=indexers, drop=True)

    # Retrieve the aviation share of emissions
    k_share = Key("emi share", tuple("eny"), L)
    c.add(k_share, aviation_emi_share, K.emi_in)

    # - (emission total) × (aviation share) → emissions of aviation
    # - Re-add the "t" dimension with +ve sign for "Aviation" and -ve sign for "Road
    #   Rail and Domestic Shipping"
    c.add(K.emi, "mul", K.emi[0] / "t", k_share, K.bcast)

    # No change to final energy data
    c.add(K.fe_out, genno.Quantity(0.0, units="EJ / a"))


def method_B(c: "Computer") -> None:
    """Prepare calculations up to :data:`K.emi` using :data:`METHOD.B`.

    This method uses the |y0| share of aviation in total transport final energy as
    indicated by :class:`.IEA_EWEB`, with dimensions :math:`(c, n)`, to disaggregate
    total final energy from the input data, then applies emission intensity data to
    compute aviation emissions.

    Excluding data transformations, units, and other manipulations for alignment:

    1. From the :class:`.IEA_EWEB` 2024 edition, select data for :math:`y = 2019`.
    2. Aggregate IEA EWEB data to align with MESSAGEix-GLOBIOM |c|.
    3. Compute the ratio of ``_1`` to ``_2`` (see :func:`.web.transform_C` for how these
       labels are produced). This is the share of aviation in final energy.
    4. Add the steps from :func:`.method_BC_common`.
    """
    from message_ix_models.model.transport import build

    context: Context = c.graph["context"]

    # Add the same structure information and exogenous data used in the build and report
    # workflow steps for MESSAGEix-Transport, in particular:
    # - e::codelist
    # - groups::iea to transport
    # - energy::n-y-product-flow:iea —using .tools.iea.web.IEA_EWEB
    build.get_computer(context, c)

    # Shorthand for keys and sequences of keys
    fe = Keys(
        cnt=f"energy:c-n-t:{L}+0",
        iea="energy:n-product-flow:iea",
        share=f"fe share:c-n:{L}",
    )

    # Prepare data from IEA EWEB: the share of aviation in transport consumption of each
    # 'c[ommodity]'
    # Select data for 2019 only
    c.add(fe.iea[0], "select", fe.iea * "y", indexers=dict(y=2019), drop=True)

    # Only use the aggregation on the 'product' dimension, not on 'flow'
    g = Key("groups:p:iea to transport")
    c.add(g, lambda d: dict(product=d["product"]), "groups::iea to transport")
    # Aggregate IEA 'product' dimension for alignment to MESSAGE 'c[ommodity]'
    c.add(fe.iea[1], "aggregate", fe.iea[0], g, keep=False)

    # Rename dimensions
    c.add(fe.cnt[0], "rename_dims", fe.iea[1], name_dict=dict(flow="t", product="c"))

    # Global total
    c.add("n::world agg", "nodes_world_agg", "config", dim="n", name=None)
    c.add(fe.cnt[1], "aggregate", fe.cnt[0], "n::world agg", keep=False)

    # Ratio of _1 (DOMESAIR - AVBUNK) to _2 (TOTTRANS - AVBUNK)
    c.add(fe.share[0], "select", fe.cnt[1], indexers=dict(t="_1"), drop=True)
    c.add(fe.share[1], "select", fe.cnt[1], indexers=dict(t="_2"), drop=True)
    c.add(fe.share, "div", fe.share[0], fe.share[1])

    # Prepare remaining calculations
    method_BC_common(c, fe.share)


def method_BC_common(c: "Computer", k_fe_share: "Key") -> None:
    """Common steps for :func:`.method_B` and :func:`.method_C`.

    1. From the input data (:data:`K.input`), select the values matching
       :data:`EXPR_FE`, that is, final energy use by aviation.
    2. Load emissions intensity of aviation final energy use from the file
       :ref:`transport-input-emi-intensity`.
    3. Multiply (k_fe_share) × (1) × (2) to compute the estimate of aviation emissions.
    4. Estimate adjustments according to :func:`broadcast_t`.

    Parameters
    ----------
    k_fe_share
        A key with dimensions either :math:`(c, n)` or :math:`(c, n, y)` giving the
        share of aviation in total transport final energy.
    """

    from message_ix_models.model.transport.key import exo

    # Check dimensions of k_fe_share
    exp = {frozenset("cn"), frozenset("cny")}
    if set(k_fe_share.dims) not in exp:  # pragma: no cover
        raise ValueError(f"Dimensions of k_cn={k_fe_share.dims} are not in {exp}")

    # Shorthand for keys and sequences of keys
    k = Keys(
        ei=exo.emi_intensity,  # Dimensions (c, e, t)
        emi0=Key("emission", ("ceny"), L),
        fe=Key("fe", tuple("cny"), f"{L}+BC"),
        units=Key(f"units:e-UNIT:{L}"),
    )

    # Select only total transport consumption of lightoil from K.fe_in
    indexers = {"t": "Transportation (w/ bunkers)"}
    c.add(k.fe[0], "select", K.fe_in, indexers=indexers, drop=True)

    ### Compute estimate of emissions
    # Product of aviation share and FE of total transport → FE of aviation
    c.add(k.fe, "mul", k.fe[0], k_fe_share)

    # Convert exogenous emission intensity data to Mt / EJ
    c.add(k.ei["units"], "convert_units", k.ei, units="Mt / EJ")

    # - (FE of aviation) × (emission intensity) → emissions of aviation.
    # - Drop/partial sum over 1 label ("AIR") on dimension "t".
    c.add(k.emi0[0], "mul", k.fe, k.ei["units"], sums=True)

    # Convert units to megatonne per year
    c.add(k.emi0[1], "convert_units", k.emi0[0], units="Mt / year")

    # - Add "UNIT" dimension and adjust magnitudes for species where units must be kt.
    #   See e_UNIT().
    # - Re-add the "t" dimension with +ve sign for "Aviation" and -ve sign for "Road
    #   Rail and Domestic Shipping".
    # - Drop/partial sum over dimension "c".
    c.add(k.units, e_UNIT, "e::codelist")
    c.add(K.emi[2], "mul", k.emi0[1], k.units, K.bcast)

    # Restore labels: "R12_AFR" → "AFR" etc. "World" is not changed.
    labels = dict(n={v: k for k, v in get_labels()["n"].items()})
    c.add(K.emi[3], "relabel", K.emi[2], labels=labels)
    # Drop data for y0
    c.add(K.emi, "select", K.emi[3], indexers=dict(y=[2020]), inverse=True)

    # Re-add the "t" dimension with +ve and -ve sign for certain labels
    c.add(K.fe_out[0], "mul", k.fe, broadcast_t_fe())
    c.add(K.fe_out[1], "drop_vars", K.fe_out[0] * "c_new", names="c")
    c.add(K.fe_out[2], "rename_dims", K.fe_out[1], name_dict={"c_new": "c"})
    # Restore labels: "R12_AFR" → "AFR" etc. "World" is not changed.
    c.add(K.fe_out[3], "relabel", K.fe_out[2], labels=labels)
    # Drop data for y0
    c.add(K.fe_out, "select", K.fe_out[3], indexers=dict(y=[2020]), inverse=True)


def method_C(c: "Computer") -> None:
    """Prepare calculations up to :data:`K.emi` using :data:`METHOD.C`.

    This method uses a solved MESSAGEix-Transport scenario to compute the share of
    aviation in total transport final energy, with dimensions :math:`(c, n, y)`, and
    the proceeds similarly to :func:`method_B`.

    Excluding data transformations, units, and other manipulations for alignment:

    1. Identify a corresponding base scenario of MESSAGEix-Transport with a solution.
    2. From the model solution data, compute the share of `AIR` in total transport final
       energy.
    3. Apply the steps from :func:`.method_BC_common`.
    """
    from message_ix_models.report import prepare_reporter
    from message_ix_models.util.genno import update_computer

    context: Context = c.graph["context"]

    # - Prepare a Reporter to retrieve model solution data from `target_url`.
    # - Transfer all its tasks to `c`
    update_computer(c, prepare_reporter(context)[0])

    # Prepare `c` to compute the final energy share for aviation
    k = Keys(
        # Added by .transport.base.prepare_reporter()
        base="in:nl-t-ya-c:transport+units",
        share0=f"fe share:c-nl-ya:{L}",
        share1=f"fe share:c-n-y:{L}",
    )

    # Relabel "R12_GLB" (added by .report.transport.aggregate()) to "World"
    labels = {"nl": {"R12_GLB": "World"}}
    c.add(k.base[1], "relabel", k.base[0], labels=labels, sums=True)

    # Select the numerator; drop the 't' dimension
    c.add(k.share0["num"], "select", k.base[1], indexers=dict(t="AIR"), drop=True)
    # Ratio of AIR to the total
    c.add(k.share0, "div", k.share0["num"], k.base[1] / "t")

    # Rename dimensions as expected by method_BC_common
    c.add(k.share1, "rename_dims", k.share0, name_dict={"nl": "n", "ya": "y"})

    method_BC_common(c, k.share1)


def process_df(
    data: pd.DataFrame,
    *,
    method: METHOD = METHOD.B,
    platform_name: Optional[str] = None,
) -> pd.DataFrame:
    """Process `data`.

    Same as :func:`process_file`, except the data is returned as a data frame in the
    same structure as `data`.

    For the meaning of parameters `method` and `platform_name`, see
    :func:`get_computer`.
    """
    # Prepare all other tasks
    c = get_computer(data.iloc[0, :], method, platform_name=platform_name)

    def fillna(df: pd.DataFrame) -> pd.DataFrame:
        """Replace :py:`np.nan` with 0.0 in certain rows and columns."""
        mask = df.Variable.str.fullmatch(
            r"Emissions\|[^\|]+\|Energy\|Demand\|(Bunkers|Transportation).*"
        )
        to_fill = {c: 0.0 for c in df.columns if str(c).isnumeric() and int(c) >= 2020}
        return df.where(~mask, df.fillna(to_fill))

    # Input data: replace NaN with 0
    c.add(K.input[0], fillna, data)
    # Convert `data` to a Quantity with the appropriate structure
    c.add(K.input, to_quantity, K.input[0], **IAMC_KW)

    # Compute and return the result
    return c.get("target")


def process_file(
    path_in: "pathlib.Path",
    path_out: "pathlib.Path",
    *,
    method: METHOD,
    platform_name: Optional[str] = None,
) -> None:
    """Process data from file.

    1. Read input data from `path_in` in IAMC CSV format.
    2. Call :func:`get_computer` and in turn one of :func:`method_A`, :func:`method_B`,
       or :func:`method_C` according to the value of `method`.
    3. Write to `path_out` in the same format as (1).

    Parameters
    ----------
    path_in :
        Input data path.
    path_out :
        Output data path.
    method :
        One of :class:`METHOD`.
    """
    # Peek at `path` for a row containing the model and scenario names
    row0 = pd.read_csv(path_in, nrows=1).iloc[0, :]

    # Prepare all other tasks
    c = get_computer(row0, method)

    # Input data: read from `path_in`
    c.add(K.input, iamc_like_data_for_query, path=path_in, **IAMC_KW)

    # Execute, write the result to `path_out`
    c.get("target").to_csv(path_out, index=False)


@cache
def v_to_fe_coords(value: Hashable) -> Optional[dict[str, str]]:
    """Match ‘variable’ names codes using :data:`EXPR_FE`.

    For use with :func:`.select_expand`.
    """
    if match := EXPR_FE.fullmatch(str(value)):
        return match.groupdict()
    else:
        return None


@cache
def v_to_emi_coords(value: Hashable) -> Optional[dict[str, str]]:
    """Match ‘variable’ names codes using :data:`EXPR_EMI`.

    For use with :func:`.select_expand`.
    """
    if match := EXPR_EMI.fullmatch(str(value)):
        return match.groupdict()
    else:
        return None
