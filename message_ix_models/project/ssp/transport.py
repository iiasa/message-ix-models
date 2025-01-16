"""Postprocess aviation emissions for SSP 2024."""

import re
from typing import TYPE_CHECKING, Hashable

import genno
import pandas as pd
import xarray as xr
from genno import Key, KeySeq
from genno.core.key import single_key

from message_ix_models import Context
from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.iamc import iamc_like_data_for_query
from message_ix_models.util import minimum_version

if TYPE_CHECKING:
    import pathlib

    import sdmx.model.common
    from genno.types import AnyQuantity

#: Dimensions of several quantities.
DIMS = "e n t y UNIT".split()

#: Expression for IAMC ‘variable’ names used in :func:`main`.
EXPR_EMI = r"^Emissions\|(?P<e>[^\|]+)\|Energy\|Demand\|Transportation(?:\|(?P<t>.*))?$"
EXPR_FE = r"^Final Energy\|Transportation\|(?P<c>Liquids\|Oil)$"

#: :class:`.IEA_EWEB` flow codes used in the current file.
FLOWS = ["AVBUNK", "DOMESAIR", "TOTTRANS"]

#: Common label / :attr:`.Key.name`
L = "AIR emi"


def aviation_share(ref: "AnyQuantity") -> "AnyQuantity":
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


def broadcast_t(include_international: bool) -> "AnyQuantity":
    """Quantity to re-add the |t| dimension.

    Parameters
    ----------
    include_international
        If :any:`True`, include "Aviation|International" with magnitude 1.0. Otherwise,
        omit

    Return
    ------
    genno.Quantity
        with dimension "t" and the values:

        - +1.0 for t="Aviation", a label with missing data.
        - -1.0 for t="Road Rail and Domestic Shipping", a label with existing data from
          which the aviation total should be subtracted.
    """
    value = [1, -1, 1]
    t = ["Aviation", "Road Rail and Domestic Shipping", "Aviation|International"]
    idx = slice(None) if include_international else slice(-1)

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


def extract_dims(
    qty: "AnyQuantity", dim_expr: dict, *, drop: bool = True, fillna: str = "_T"
) -> "AnyQuantity":
    """Extract dimensions from IAMC-like ‘variable’ names using regular expressions."""
    import pandas as pd

    dims = list(qty.dims)

    dfs = [qty.to_frame().reset_index()]
    for dim, expr in dim_expr.items():
        pattern = re.compile(expr)
        dfs.append(dfs[0][dim].str.extract(pattern).fillna(fillna))
        dims.extend(pattern.groupindex)
        if drop:
            dims.remove(dim)

    return genno.Quantity(pd.concat(dfs, axis=1).set_index(dims)["value"])


def extract_dims1(qty: "AnyQuantity", dim: dict) -> "AnyQuantity":  # pragma: no cover
    """Extract dimensions from IAMC-like ‘variable’ names expressions.

    .. note:: This incomplete, non-working version of :func:`extract_dims` uses
       :mod:`xarray` semantics.
    """
    from collections import defaultdict

    result = qty
    for d0, expr in dim.items():
        d0_new = f"{d0}_new"
        pattern = re.compile(expr)

        indexers: dict[Hashable, list[Hashable]] = {g: [] for g in pattern.groupindex}
        indexers[d0_new] = []

        coords = qty.coords[d0].data.astype(str)
        for coord in coords:
            if match := pattern.match(coord):
                groupdict = match.groupdict()
                coord_new = coord[match.span()[1] :]
            else:
                groupdict = defaultdict(None)
                coord_new = coord

            for g in pattern.groupindex:
                indexers[g].append(groupdict[g])
            indexers[d0_new].append(coord_new)

        for d1, labels in indexers.items():
            i2 = {d0: xr.DataArray(coords, coords={d1: labels})}
            result = result.sel(i2)

    return result


def finalize(
    q_all: "AnyQuantity",
    q_update: "AnyQuantity",
    model_name: str,
    scenario_name: str,
    path_out: "pathlib.Path",
) -> None:
    """Finalize output.

    1. Reattach "Model" and "Scenario" labels.
    2. Reassemble the "Variable" dimension/coords of `q_update`; drop "e" and "t".
    3. Convert both `q_all` and `q_update` to :class:`pandas.Series`; update the former
       with the contents of the latter. This retains all other, unmodified data in
       `q_all`.
    4. Adjust to IAMC ‘wide’ structure and write to `path_out`.

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

    s_all = q_all.pipe(_expand).to_series()

    s_all.update(
        q_update.pipe(_expand)
        .to_frame()
        .reset_index()
        .assign(
            Variable=lambda df: (
                "Emissions|" + df["e"] + "|Energy|Demand|Transportation|" + df["t"]
            ).str.replace("|_T", ""),
        )
        .drop(["e", "t"], axis=1)
        .set_index(s_all.index.names)[0]
        .rename("value")
    )

    (
        s_all.unstack("y")
        .reorder_levels(["Model", "Scenario", "Region", "Variable", "Unit"])
        .reset_index()
        .to_csv(path_out, index=False)
    )


@minimum_version("genno 1.25")
def main(path_in: "pathlib.Path", path_out: "pathlib.Path", method: str) -> None:
    """Postprocess aviation emissions for SSP 2024.

    1. Read input data from `path_in`.
    2. Call either :func:`prepare_method_A` or :func:`prepare_method_B` according to the
       value of `method`.
    3. Write to `path_out`.

    Parameters
    ----------
    path_in :
        Input data path.
    path_out :
        Output data path.
    method :
        Either 'A' or 'B'.
    """
    import pandas as pd

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
    c.add("path out", path_out)

    # Common structure and utility quantities used by prepare_method_[AB]
    c.add(f"broadcast:t:{L}", broadcast_t, include_international=method == "A")

    k_emi_in, e_t = KeySeq(L, DIMS, "input"), tuple("et")

    # Select and transform data matching EXPR_EMI
    # Filter on "VARIABLE"
    c.add(k_emi_in[0] / e_t, select_re, k_input, indexers={"VARIABLE": EXPR_EMI})
    # Extract the "e" and "t" dimensions from "VARIABLE"
    c.add(k_emi_in[1], extract_dims, k_emi_in[0] / e_t, dim_expr={"VARIABLE": EXPR_EMI})
    c.add(k_emi_in[2], "assign_units", k_emi_in[1], units="Mt/year")

    # Call a function to prepare the remaining calculations
    # This returns a key like "*:e-n-t-y-UNIT:*"
    prepare_func = {"A": prepare_method_A, "B": prepare_method_B}[method]

    k = prepare_func(c, k_input, k_emi_in.prev)

    assert set(DIMS) == set(k.dims), k.dims

    # Add to the input data
    k_adj = c.add(Key(L, DIMS, "adj"), "add", k_emi_in.prev, k)

    # - Collapse to IAMC "VARIABLE" dimension name
    # - Recombine with other data
    # - Write back to the file
    c.add("target", finalize, k_input, k_adj, "model name", "scenario name", "path out")

    # Execute
    c.get("target")


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
    k = KeySeq("result", DIMS)

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
    3. Reverse the sign of flow=AVBUNK; these values are negative in the source data.
    4. Compute the ratio :math:`(AVBUNK + DOMESAIR) / TOTTRANS`, the share of aviation
       in final energy.
    5. From the input data (`k_input`), select the values matching :data:`EXPR_FE`, that
       is, final energy use by aviation.
    6. Load emissions intensity of aviation final energy use :file:`emi-intensity.csv` /
       :data:`emi_intensity`.
    7. Multiply (4) × (5) × (6) to compute the estimate of
       ``Emissions|*|Energy|Demand|Transportation|Aviation``.
    8. Subtract (7) from:
       ``Emissions|*|Energy|Demand|Transportation|Road Rail and Domestic Shipping``.
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
    k_fe_in = KeySeq("fe", ("c", "n", "y", "UNIT"), "input")
    k_cnt = KeySeq("energy", ("c", "n", "t"), L)
    k_cn = k_cnt / "t"

    ### Prepare data from IEA EWEB: the share of aviation in transport consumption of
    ### each 'c[ommodity]'

    # Fetch data from IEA EWEB
    kw = dict(provider="IEA", edition="2024", flow=FLOWS, transform="B", regions="R12")
    k_iea = prepare_computer(context, c, "IEA_EWEB", kw, strict=False)[0]
    k_fnp = KeySeq(k_iea / "y")  # flow, node, product

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

    # Filter on "VARIABLE"
    c.add(k_fe_in[0] / "c", select_re, k_input, indexers={"VARIABLE": EXPR_FE})

    # Extract the "e" dimensions from "VARIABLE"
    c.add(k_fe_in[1], extract_dims, k_fe_in[0] / "c", dim_expr={"VARIABLE": EXPR_FE})

    # Convert "UNIT" dim labels to Quantity.units
    c.add(k_fe_in[2] / "UNIT", "unique_units_from_dim", k_fe_in[1], dim="UNIT")

    # Relabel:
    # - c[ommodity]: 'Liquids|Oil' (IAMC 'variable' component) → 'lightoil'
    # - n[ode]: 'AFR' → 'R12_AFR' etc.
    labels = dict(
        c={"Liquids|Oil": "lightoil"},
        n={n.id.partition("_")[2]: n.id for n in get_codelist("node/R12")},
    )
    c.add(k_fe_in[3] / "UNIT", "relabel", k_fe_in[2] / "UNIT", labels=labels)

    ### Compute estimate of emissions
    # Product of aviation share and FE of total transport → FE of aviation
    k_ = c.add(f"{L} fe", "mul", k_fe_in.prev / "UNIT", k_cn.prev)

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


def select_re(qty: "AnyQuantity", indexers: dict) -> "AnyQuantity":
    """Select from `qty` using regular expressions for each dimension."""
    new_indexers = dict()
    for dim, expr in indexers.items():
        new_indexers[dim] = list(
            map(str, filter(re.compile(expr).match, qty.coords[dim].data.astype(str)))
        )
    return qty.sel(new_indexers)
