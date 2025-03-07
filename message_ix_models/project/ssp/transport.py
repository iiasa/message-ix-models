"""Postprocess aviation emissions for SSP 2024."""

import logging
import re
from collections.abc import Hashable
from functools import cache
from typing import TYPE_CHECKING, Literal, Optional

import genno
import pandas as pd
from genno import Key
from genno.core.key import single_key

from message_ix_models import Context
from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.iamc import iamc_like_data_for_query, to_quantity
from message_ix_models.util import minimum_version

if TYPE_CHECKING:
    import pathlib

    import sdmx.model.common
    from genno import Computer
    from genno.types import AnyQuantity, KeyLike, TQuantity

log = logging.getLogger(__name__)

#: Dimensions of several quantities.
DIMS = "e n t y UNIT".split()

EXPR_EMI = re.compile(
    r"^Emissions\|(?P<e>[^\|]+)\|Energy\|Demand\|(?P<t>(Bunkers|Transportation).*)$"
)
EXPR_FE = re.compile(r"^Final Energy\|Transportation\|(?P<c>Liquids\|Oil)$")

#: :class:`.IEA_EWEB` flow codes used in the current file.
FLOWS = ["AVBUNK", "DOMESAIR", "TOTTRANS"]

#: Common label / :attr:`.Key.name`
L = "AIR emi"


def aviation_share(ref: "TQuantity") -> "TQuantity":
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


def broadcast_t(version: Literal[1, 2], include_international: bool) -> "AnyQuantity":
    """Quantity to re-add the |t| dimension.

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
    if version == 1:
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
    data = []
    for e in cl_emission:
        try:
            label = str(e.get_annotation(id="report").text)
        except KeyError:
            label = e.id
        try:
            unit = str(e.get_annotation(id="units").text)
        except KeyError:
            unit = "Mt"
        data.append([e.id, f"{unit} {label}/yr", 1.0 if unit == "Mt" else 1e3])

    dims = "e UNIT value".split()
    return genno.Quantity(
        pd.DataFrame(data, columns=dims).set_index(dims[:-1])[dims[-1]]
    )


def finalize(
    q_all: "TQuantity", q_update: "TQuantity", model_name: str, scenario_name: str
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

    # - Convert `q_update` to pd.Series
    # - Reassemble "Variable" codes.
    # - Drop dimensions (e, t).
    # - Align index with s_all.
    s_update = (
        q_update.pipe(_expand)
        .to_frame()
        .reset_index()
        .assign(
            Variable=lambda df: "Emissions|" + df["e"] + "|Energy|Demand|" + df["t"]
        )
        .drop(["e", "t"], axis=1)
        .set_index(s_all.index.names)[0]
        .rename("value")
    )
    log.info(f"{len(s_update)} obs to update")

    # Update `s_all`. This yields an 'outer join' of the original and s_update indices.
    s_all.update(s_update)

    return (
        s_all.unstack("y")
        .reorder_levels(["Model", "Scenario", "Region", "Variable", "Unit"])
        .reset_index()
    )


@minimum_version("genno 1.28")
def prepare_computer(c: "Computer", k_input: Key, method: str) -> "KeyLike":
    """Prepare `c` to process aviation emissions data.

    Returns
    -------
    str
        "target". Calling :py:`c.get("target")` triggers the calculation.
    """
    c.require_compat("message_ix_models.report.operator")

    # Common structure and utility quantities used by prepare_method_[AB]
    c.add(
        f"broadcast:t:{L}", broadcast_t, version=2, include_international=method == "A"
    )

    k_emi_in = Key(L, DIMS, "input")

    # Select and transform data matching EXPR_EMI
    # Filter on "VARIABLE", expand the (e, t) dimensions from "VARIABLE"
    c.add(k_emi_in[0], "select_expand", k_input, dim_cb={"VARIABLE": v_to_emi_coords})
    c.add(k_emi_in[1], "assign_units", k_emi_in[0], units="Mt/year")

    # Call a function to prepare the remaining calculations
    prepare_func = {"A": prepare_method_A, "B": prepare_method_B}[method]
    k = prepare_func(c, k_input, k_emi_in.last)
    # This should return a key like "*:e-n-t-y-UNIT:*"; check
    assert set(DIMS) == set(k.dims), k.dims

    # Add to the input data
    k_adj = c.add(Key(L, DIMS, "adj"), "add", k_emi_in.last, k)

    # - Collapse to IAMC "VARIABLE" dimension name
    # - Recombine with other data
    c.add("target", finalize, k_input, k_adj, "model name", "scenario name")
    return "target"


def prepare_method_A(
    c: "genno.Computer", k_input: "genno.Key", k_emi_in: "genno.Key"
) -> "genno.Key":
    """Prepare calculations using method 'A'.

    1. Select data with variable names matching :data:`EXPR_EMI`.
    2. Calculate (identical) values for:

       - ``Emissions|*|Energy|Demand|Transportation|Aviation``
       - ``Emissions|*|Energy|Demand|Transportation|Aviation|International``

       …as the product of :func:`aviation_share` and
       ``Emissions|*|Energy|Demand|Transportation``.
    3. Subtract (2) from:
       ``Emissions|*|Energy|Demand|Transportation|Road Rail and Domestic Shipping``
    """
    # Shorthand
    k = Key("result", DIMS)

    # Select the total
    c.add(k[0] / "t", "select", k_emi_in, indexers={"t": "_T"}, drop=True)

    # Retrieve the aviation share of emissions
    k_share = Key(f"{L} share", tuple("eny"))
    c.add(k_share, aviation_share, k_emi_in)

    # - (emission total) × (aviation share) → emissions of aviation
    # - Re-add the "t" dimension with +ve sign for "Aviation" and -ve sign for "Road
    #   Rail and Domestic Shipping"
    c.add(k[1], "mul", k_emi_in, k_share, f"broadcast:t:{L}")

    return k[1]


def prepare_method_B(
    c: "genno.Computer", k_input: "genno.Key", k_emi_in: "genno.Key"
) -> "genno.Key":
    """Prepare calculations using method 'B'.

    Excluding data transformations, units, and other manipulations for alignment:

    1. From the :class:`.IEA_EWEB` 2024 edition, select data for :math:`y = 2019` and
       the :data:`FLOWS`.
    2. Aggregate IEA EWEB to align with MESSAGEix-GLOBIOM |c|.
    3. Reverse the sign of values for flow=AVBUNK. These are negative in the source
       data, but their absolute value must be added to values for flow=DOMESAIR.
    4. Compute the ratio :math:`(AVBUNK + DOMESAIR) / TOTTRANS`, the share of aviation
       in final energy.
    5. From the input data (`k_input`), select the values matching :data:`EXPR_FE`, that
       is, final energy use by aviation.
    6. Load emissions intensity of aviation final energy use from the file
       :ref:`transport-input-emi-intensity`.
    7. Multiply (4) × (5) × (6) to compute the estimate of aviation emissions.
    8. Estimate adjustments according to :func:`broadcast_t`.
    9. Adjust `k_emi_in` by adding (7) and (8).
    """
    from message_ix_models.model.transport import build
    from message_ix_models.model.transport import files as exo
    from message_ix_models.tools.exo_data import prepare_computer

    # Fetch a Context instance
    # NB It is assumed this is aligned with the contents of the input data file
    context = Context.get_instance()

    # Add the same structure information used in the build and report workflow steps for
    # MESSAGEix-Transport, notably <e::codelist> and <groups::iea to transport>
    build.get_computer(context, c)

    # Shorthand for keys and sequences of keys
    k_ei = exo.emi_intensity
    k_fe_in = Key("fe", ("c", "n", "y", "UNIT"), "input")
    k_cnt = Key("energy", ("c", "n", "t"), L)
    k_cn = k_cnt / "t"

    ### Prepare data from IEA EWEB: the share of aviation in transport consumption of
    ### each 'c[ommodity]'

    # Fetch data from IEA EWEB
    kw = dict(provider="IEA", edition="2024", flow=FLOWS, transform="B", regions="R12")
    k_iea = prepare_computer(context, c, "IEA_EWEB", kw, strict=False)[0]
    k_fnp = Key(k_iea / "y")  # flow, node, product

    # Select data for 2019 only
    c.add(k_fnp[0], "select", k_iea, indexers=dict(y=2019), drop=True)

    # Only use the aggregation on the 'product' dimension, not on 'flow'
    c.add(
        "groups:p:iea to transport",
        lambda d: {"product": d["product"]},
        "groups::iea to transport",
    )
    # Aggregate IEA 'product' dimension for alignment to MESSAGE 'c[ommodity]'
    c.add(k_fnp[1], "aggregate", k_fnp[0], "groups:p:iea to transport", keep=False)

    # Rename dimensions
    c.add(k_cnt[0], "rename_dims", k_fnp[1], name_dict=dict(flow="t", product="c"))

    # Reverse sign of AVBUNK
    q_sign = genno.Quantity([-1.0, 1.0, 1.0], coords={"t": FLOWS})
    c.add(k_cnt[1], "mul", k_cnt[0], q_sign)

    # Compute ratio of ('AVBUNK' + 'DOMESAIR') to 'TOTTRANS'
    # TODO Confirm that this or another numerator is appropriate
    c.add(k_cnt[2], "select", k_cnt[1], indexers=dict(t=["AVBUNK", "DOMESAIR"]))
    c.add(k_cn[0], "sum", k_cnt[2], dimensions=["t"])
    c.add(k_cn[1], "select", k_cnt[1], indexers=dict(t="TOTTRANS"), drop=True)
    c.add(k_cn[2], "div", k_cn[0], k_cn[1])

    ### Prepare data from the input data file: total transport consumption of light oil

    # Filter on "VARIABLE", extract (e) dimension
    c.add(k_fe_in[0], "select_expand", k_input, dim_cb={"VARIABLE": v_to_fe_coords})

    # Convert "UNIT" dim labels to Quantity.units
    c.add(k_fe_in[1] / "UNIT", "unique_units_from_dim", k_fe_in[0], dim="UNIT")

    # Relabel:
    # - c[ommodity]: 'Liquids|Oil' (IAMC 'variable' component) → 'lightoil'
    # - n[ode]: 'AFR' → 'R12_AFR' etc.
    labels = dict(
        c={"Liquids|Oil": "lightoil"},
        n={n.id.partition("_")[2]: n.id for n in get_codelist("node/R12")},
    )
    c.add(k_fe_in[2] / "UNIT", "relabel", k_fe_in[1] / "UNIT", labels=labels)

    ### Compute estimate of emissions
    # Product of aviation share and FE of total transport → FE of aviation
    k_ = c.add(f"{L} fe", "mul", k_fe_in.last / "UNIT", k_cn.last)

    # Convert exogenous emission intensity data to Mt / EJ
    c.add(k_ei + "conv", "convert_units", k_ei, units="Mt / EJ")

    # - (FE of aviation) × (emission intensity) → emissions of aviation.
    # - Drop/partial sum over 1 label ("AIR") on dimension "t".
    _ = single_key(c.add(f"{L}::0", "mul", k_, k_ei + "conv", sums=True))
    k_ = Key(_) / "t"

    # Convert units to megatonne per year
    k_ = c.add(Key(L, k_.dims, "1"), "convert_units", k_, units="Mt / year")

    # - Add "UNIT" dimension and adjust magnitudes for species where units must be kt.
    #   See e_UNIT().
    # - Re-add the "t" dimension with +ve sign for "Aviation" and -ve sign for "Road
    #   Rail and Domestic Shipping".
    # - Drop/partial sum over dimension "c".
    k_units = c.add(Key(f"{L} units:e-UNIT"), e_UNIT, "e::codelist")
    _ = single_key(c.add(f"{L}::2", "mul", k_, k_units, f"broadcast:t:{L}"))
    k_ = Key(_) / "c"

    # Change labels: restore e.g. "AFR" given "R12_AFR"
    labels = dict(n={v: k for k, v in labels["n"].items()})
    k_result = single_key(c.add(Key(L, k_.dims, "3"), "relabel", k_, labels=labels))
    return Key(k_result)


def process_df(data: pd.DataFrame, *, method: str = "B") -> pd.DataFrame:
    """Process `data`.

    Same as :func:`process_file`, except the data is returned as a data frame in the
    same structure as `data`.
    """
    c = genno.Computer()

    # Peek at `data` to identify the model and scenario names
    c.add("model name", genno.quote(data["Model"].iloc[0]))
    c.add("scenario name", genno.quote(data["Scenario"].iloc[0]))

    # Convert `data` to a Quantity with the appropriate structure
    k_input = genno.Key("input", ("n", "y", "VARIABLE", "UNIT"))
    c.add(
        k_input,
        to_quantity,
        data,
        non_iso_3166="keep",
        query="Model != ''",
        unique="MODEL SCENARIO",
    )

    # Prepare all other tasks
    prepare_computer(c, k_input, method)

    # Compute and return the result
    return c.get("target")


def process_file(
    path_in: "pathlib.Path", path_out: "pathlib.Path", *, method: str
) -> None:
    """Process data from file.

    1. Read input data from `path_in` in IAMC CSV format.
    2. Call :func:`prepare_computer` and in turn either :func:`prepare_method_A` or
       :func:`prepare_method_B` according to the value of `method`.
    3. Write to `path_out` in the same format as (1).

    Parameters
    ----------
    path_in :
        Input data path.
    path_out :
        Output data path.
    method :
        Either 'A' or 'B'.
    """
    c = genno.Computer()

    # Read the data from `path`
    k_input = genno.Key("input", ("n", "y", "VARIABLE", "UNIT"))
    c.add(
        k_input,
        iamc_like_data_for_query,
        path=path_in,
        non_iso_3166="keep",
        query="Model != ''",
        unique="MODEL SCENARIO",
    )

    # Peek at `path` to identify the model and scenario names
    df = pd.read_csv(path_in, nrows=1)
    c.add("model name", genno.quote(df["Model"].iloc[0]))
    c.add("scenario name", genno.quote(df["Scenario"].iloc[0]))

    prepare_computer(c, k_input, method)

    # Execute, write the result back to file
    c.get("target").to_csv(path_out, index=False)


@cache
def v_to_fe_coords(value: Hashable) -> Optional[dict[str, str]]:
    """Match ‘variable’ names used in :func:`main`."""
    if match := EXPR_FE.fullmatch(str(value)):
        return match.groupdict()
    else:
        return None


@cache
def v_to_emi_coords(value: Hashable) -> Optional[dict[str, str]]:
    """Match ‘variable’ names used in :func:`main`."""
    if match := EXPR_EMI.fullmatch(str(value)):
        return match.groupdict()
    else:
        return None
