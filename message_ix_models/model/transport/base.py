"""Data preparation for the MESSAGEix-GLOBIOM base model."""

from functools import partial
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import genno
import numpy as np
import pandas as pd
from genno import Computer, Key, quote
from genno.core.key import single_key

from message_ix_models.util import minimum_version

from .key import gdp_exo
from .key import report as k_report
from .util import EXTRAPOLATE

if TYPE_CHECKING:
    from collections.abc import Hashable, Iterable

    import message_ix
    from genno.core.key import KeyLike
    from genno.types import AnyQuantity, TQuantity

    Coords = dict[Hashable, Iterable[Hashable]]

#: Key to trigger the computations set up by :func:`.prepare_computer`
TARGET = "base model data"

FE_HEADER = """Final energy input to transport technologies.

Units: GWa
"""

FE_SHARE_HEADER = (
    """Portion of final energy input to transport by each c within (nl, ya) groups."""
)


SCALE_1_HEADER = """Ratio of MESSAGEix-Transport output to IEA EWEB data.

- `t` (technology) codes correspond to IEA `FLOW` codes or equivalent aggregates
  across groups of MESSAGEix-Transport technologies.
- `c` (commodity) codes correspond to MESSAGEix-GLOBIOM commodities or equivalent
  aggregates across groups of IEA `PRODUCT` codes.
"""

SCALE_2_HEADER = """Ratio of scaled MESSAGEix-Transport output to IEA EWEB data.

The numerator used to compute this scaling factor is the one corrected by the values in
scale-1.csv.
"""

UE_SHARE_HEADER = (
    """Portion of useful energy output by each t within (nl, ya) groups."""
)


def align_and_fill(
    qty: "TQuantity", ref: "TQuantity", value: float = 1.0
) -> "TQuantity":
    """Align `qty` with `ref`, and fill with `value`.

    The result is guaranteed to have a value for every key in `ref`.
    """
    return type(qty)(
        pd.DataFrame.from_dict(
            {"ref": ref.to_series(), "data": qty.to_series()}
        ).fillna(value)["data"]
    )


def fixed_scale_1(
    qty: "TQuantity", *, commodity: str, technology: str, value: float = 1.0
) -> "TQuantity":
    """Fix certain values for scale-1."""
    from genno.operator import concat, select

    # Coords of `qty`
    dims = {d: v.data for d, v in qty.coords.items()}

    # Coords excepting `commodity` and `technology`
    c_other: "Coords" = dict(c=sorted(set(dims["c"]) - {commodity}))
    t_other: "Coords" = dict(t=sorted(set(dims["t"]) - {technology}))
    # Dimensions for the fixed values
    dims.update(c=[commodity], t=[technology])

    # Concatenate:
    # 1. Values for all technologies other than `technology`.
    # 2. Values for `technology` and all commodities other than `commodity`.
    # 3. Fixed values for (t=technology, c=commodity).
    return concat(
        select(qty, t_other),
        select(qty, dict(t=["technology"]) | c_other),
        type(qty)(value).expand_dims(dims),
    )


@minimum_version("pandas 2; python 3.10")
def smooth(c: Computer, key: "genno.Key", *, dim: str = "ya") -> "genno.Key":
    """Implement ‘smoothing’ for `key` along the dimension `dim`.

    1. Identify values which do not meet a certain criterion. Currently the criterion
       is: the first contiguous sequence of values that are lower than the corresponding
       value in :math:`y_A = y_0` (e.g. in 2020).
    2. Remove those values.
    3. Fill by linear interpolation.
    """
    from itertools import pairwise

    assert key.tag != "2"
    ks = Key(key.remove_tag(key.tag or ""))

    def first_block_false(column: pd.Series) -> pd.Series:
        """Modify `column` to contain at most one contiguous block of :data:`.False`."""
        # Iterate over values pairwise to identify start and end indices of a block
        i_start = i_end = 0
        for i, pair in enumerate(pairwise(column.values), start=1):
            if pair == (True, False):  # Start of a block of False values
                i_start = i
            elif pair == (False, True):  # End of a block
                i_end = i
                break  # Don't examine further values

        if i_start > 0 and i_end > 0:  # Block of False values with a start and end
            column.iloc[i_end:] = True  # Fill with True after the end of the block
        elif i_start > 0:  # Block of False values *without* end
            column.iloc[i_start:] = True  # Erase the entire block by overwriting

        return column

    def clip_nan(qty: "TQuantity", coord: Any) -> "TQuantity":
        """Clip values below the value for `ya`, replacing with :any:`numpy.nan`.

        Only the first contiguous block of values below the value for `ya` are clipped.
        """
        # Dimensions other than `dim`
        others = list(qty.dims)
        others.remove(dim)

        # - Select the threshold values for `dim`=`coord`; broadcast to all `dim`.
        # - Merge with `qty` values.
        # - Reorder and sort index.
        # - Compute condition for clipping.
        # - Return clipped values.
        return type(qty)(
            qty.sel({dim: coord})
            .expand_dims({dim: qty.coords[dim].data})
            .to_series()
            .rename("threshold")
            .to_frame()
            .merge(qty.to_series().rename("value"), left_index=True, right_index=True)
            .reorder_levels(list(qty.dims))
            .sort_index()
            .where(
                lambda df: (df.value >= df.threshold)
                .groupby(others, group_keys=False)
                .apply(first_block_false)
            )["value"]
        )

    # Clip the data, removing values below the value for dim=y0
    c.add(ks["_clip"], clip_nan, key, "y0")

    # Identify the coordinates for interpolation on `dim`
    c.add(ks["_ya coords"], lambda qty: {dim: sorted(qty.coords[dim].data)}, key)

    # Interpolate to fill clipped data
    c.add(
        ks[2], "interpolate", ks["_clip"], ks["_ya coords"], method="slinear", sums=True
    )

    return ks[2]


def prepare_reporter(rep: "message_ix.Reporter") -> str:
    """Add tasks that produce data to parametrize transport in MESSAGEix-GLOBIOM.

    Returns :data:`.TARGET`. Retrieving the key results in the creation of files in
    the reporting output directory for the :class:`.Scenario` being reported (see
    :func:`.make_output_path`):

    1. :file:`demand.csv`: This contains MESSAGEix-Transport model solution data
       transformed into ``demand`` parameter data for a base MESSAGEix-GLOBIOM model—
       that is, one without MESSAGEix-Transport. :file:`input-base.csv` is used.
    2. :file:`bound_activity_lo.csv`: Same data transformed into ``bound_activity_lo``
       parameter data for the transport technologies ("coal_trp", etc.) appearing in the
       base model.

       .. todo:: Drop ``bound_activity_lo`` values that are equal to zero.
    3. :file:`bound_activity_up.csv`: Same values as (2), multiplied by 1.005.

    Two files are for diagnosis:

    4. :file:`scale-1.csv`: First stage scaling factor used to bring MESSAGEix-Transport
       (c, t) totals in correspondence with IEA World Energy Balance (WEB) values.
    5. :file:`scale-2.csv`: Second stage scaling factor used to bring overall totals.
    """

    # Add an empty list; invoking this key will trigger calculation of all the keys
    # below added to the list
    rep.add(TARGET, [])
    # Add this result key to the list of all reporting keys
    rep.graph[k_report.all].append(TARGET)

    # Create output subdirectory for base model files
    rep.graph["config"]["output_dir"].joinpath("base").mkdir(
        parents=True, exist_ok=True
    )

    # Keys for starting quantities
    e_iea = Key("energy:n-y-product-flow:iea")
    e_fnp = Key(e_iea.drop("y"))
    e_cnlt = Key("energy:c-nl-t:iea+0")
    k = Key("in:nl-t-ya-c-l-h:transport+units")  # MESSAGE solution values

    # First period
    y0 = rep.get("y0")

    # Transform IEA EWEB data for comparison
    # - The specific edition of the data used is set in .build.add_exogenous_data()
    # - Data for 2019 is used to proxy for `y0` = 2020, to avoid incorporating COVID
    #   impacts.
    assert y0 == 2020, f"IEA Extended World Energy Balances: no data for y={y0}"
    rep.add(e_fnp[0], "select", e_iea, indexers=dict(y=2019), drop=True)
    # rep.apply(to_csv, e_fnp[0], name="debug-e-fnp-0")  # DEBUG
    rep.add(e_fnp[1], "aggregate", e_fnp[0], "groups::iea to transport", keep=False)
    # rep.apply(to_csv, e_fnp[1], name="debug-e-fnp-1")  # DEBUG
    rep.add(
        e_cnlt,
        "rename_dims",
        e_fnp[1],
        quote(dict(flow="t", n="nl", product="c")),
        sums=True,
    )

    # Transport outputs for comparison
    rep.add(k[0], "aggregate", k, "groups::transport to iea", keep=False, sums=True)
    rep.add(k[1] / "ya", "select", k[0], indexers=dict(ya=y0), drop=True)

    # Scaling factor 1: ratio of MESSAGEix-Transport outputs to IEA data
    tmp = rep.add("scale 1", "div", k[1] / "ya", e_cnlt)
    s1 = Key(single_key(tmp))
    rep.add(s1[1], "convert_units", s1, units="1 / a")
    rep.add(s1[2], "mul", s1[1], genno.Quantity(1.0, units="a"))
    # Replace ~0 and ∞ values with 1.0; this avoids x / 0 = inf
    rep.add(s1[3], "where", s1[2], cond=lambda v: (v > 1e-3) & (v != np.inf), other=1.0)
    # Ensure no values are dropped versus the numerator (= MESSAGE outputs)
    rep.add(s1[4], align_and_fill, s1[3], k[1] / "ya")

    rep.apply(to_csv, s1[4], name=s1.name, header_comment=SCALE_1_HEADER)

    # Restore original "t" labels to scale-1
    rep.add(s1[5], "select", s1[4], "indexers::iea to transport")
    rep.add(s1[6], "rename_dims", s1[5], quote(dict(t_new="t")))

    # Force scale-1 factor to 1.0 for (t=F ROAD, c=gas)
    rep.add(s1[7], fixed_scale_1, s1[6], commodity="gas", technology="F ROAD")

    # Interpolate the scaling factor from computed value in ya=y₀ to 1.0 in ya ≥ 2050
    rep.add(s1[8], lambda q: q.expand_dims(ya=[y0]), s1[7])
    rep.add(s1[9], lambda q: q.expand_dims(ya=[2050]).clip(1.0, 1.0), s1[7])
    rep.add(s1[10], lambda q: q.expand_dims(ya=[2110]).clip(1.0, 1.0), s1[7])
    rep.add(s1[11], "concat", s1[8], s1[9], s1[10])
    rep.add("ya::coord", lambda v: {"ya": v}, "y::model")
    rep.add(s1[12], "interpolate", s1[11], "ya::coord", **EXTRAPOLATE)

    rep.apply(to_csv, s1[12], name=f"{s1.name} blend", header_comment=SCALE_1_HEADER)

    # Correct MESSAGEix-Transport outputs for the MESSAGEix-base model using the high-
    # resolution scaling factor
    rep.add(k["s1"], "div", k, s1[12])

    # Scaling factor 2: ratio of total of scaled data to IEA total
    rep.add(k[2] / "ya", "select", k["s1"], indexers=dict(ya=y0), drop=True, sums=True)
    rep.add(
        "energy:nl:iea+transport",
        "select",
        e_cnlt / "c",
        indexers=dict(t="transport"),
        drop=True,
    )
    tmp = rep.add("scale 2", "div", k[2] / ("c", "t", "ya"), "energy:nl:iea+transport")
    s2 = Key(single_key(tmp))
    rep.add(s2[1], "convert_units", s2, units="1 / a")
    rep.add(s2[2], "mul", s2[1], genno.Quantity(1.0, units="a"))

    rep.apply(to_csv, s2[2], name=s2.name, header_comment=SCALE_2_HEADER)

    # Correct MESSAGEix-Transport outputs using the low-resolution scaling factor
    rep.add(k["s2"], "div", k["s1"], s2[2])

    # Output "final energy csv"
    rep.add(k["s2+GWa"], "convert_units", k["s2"], units="GWa / a", sums=True)
    rep.apply(
        to_csv,
        k["s2+GWa"] / tuple("hlt"),
        name="final energy",
        header_comment="Final energy input to transport",
    )

    # Compute for file and plot: transport final energy intensity of GDP PPP
    k_gdp = rep.add("gdp:nl-ya", "rename_dims", gdp_exo, quote({"n": "nl", "y": "ya"}))
    k_fei = single_key(rep.add("fe intensity", "div", k["s2"] / tuple("chlt"), k_gdp))
    rep.add(k_fei + "units", "convert_units", k_fei, units="MJ / USD")
    rep.apply(to_csv, k_fei + "units", name="fe intensity")

    # Convert "final" energy inputs to transport to "useful energy" outputs, using
    # efficiency data from input-base.csv (in turn, from the base model). This data
    # will be used for `demand`.
    # - Sum across the "t" dimension of `k` to avoid conflict with "t" labels introduced
    #   by the data from file.
    tmp = rep.add("ue", "div", k["s2"] / "t", "input:t-c-h:base")
    ue = Key(single_key(tmp))

    rep.apply(share_constraints, k["s2"], ue)

    # Ensure units: in::transport+units [=] GWa/a and input::base [=] GWa; their ratio
    # gives units 1/a. The base model expects "GWa" for all 3 parameters.
    rep.add(ue[1], "mul", ue, genno.Quantity(1.0, units="GWa * a"))
    rep.apply(to_csv, ue[1] / ("c", "t"), name="demand no fill")

    # 'Smooth' ue[1] data by interpolating any dip below the base year value
    assert rep.apply(smooth, ue[1] / ("c", "t")) == ue[2] / ("c", "t")

    # Select only ya=y₀ data for use in `bound_activity_*`
    b_a_l = rep.add(Key("b_a_l", ue[2].dims), "select", ue[1], quote(dict(ya=[y0])))

    # `bound_activity_up` values are 1.005 * `bound_activity_lo` values
    b_a_u = rep.add("b_a_u", "mul", b_a_l, genno.Quantity(1.005))

    # Keyword arguments for as_message_df()
    args_demand = dict(
        dims=dict(node="nl", year="ya", time="h"),
        common=dict(commodity="transport", level="useful"),
    )
    args_bound_activity = dict(
        dims=dict(node_loc="nl", technology="t", year_act="ya", time="h"),
        common=dict(mode="M1"),
    )

    # Add similar steps for each parameter
    for name, base_key, args in (
        ("demand", ue[2] / ("c", "t"), args_demand),
        ("bound_activity_lo", b_a_l, args_bound_activity),
        ("bound_activity_lo-projected", ue[1], args_bound_activity),
        ("bound_activity_up", b_a_u, args_bound_activity),
    ):
        # More identifiers
        s = f"base model transport {name}"
        key, k_header = Key(f"{s}::ixmp"), f"{s} header"

        # Convert to MESSAGE data structure
        rep.add(
            key, "as_message_df", base_key, name=name.split("-")[0], wrap=False, **args
        )

        # Sort values
        # TODO Move upstream as a feature of as_message_df()
        dims = list(args["dims"])
        rep.add(key + "1", partial(pd.DataFrame.sort_values, by=dims), key)

        # Header for this file
        rep.add(k_header, "base_model_data_header", "scenario", name=name)

        rep.apply(to_csv, key + "1", name=name, header_key=k_header)

    return TARGET


def share_constraints(c: Computer, k_fe: "genno.Key", k_ue: "genno.Key") -> None:
    """ """
    from genno import Key

    for label, k, dim in ("fe", k_fe, "c"), ("ue", k_ue, "t"):
        # Dimensions to partial sum for the numerator of the share: omit `dim`
        dims_numerator = tuple(sorted(set("chlt") - {dim}))

        # Dimensions within which to compute max/min
        dims_maxmin = ("nl", dim)

        # Check dimensionality
        assert set(k.dims) == set(dims_numerator) | set(dims_maxmin) | {"ya"}

        # Compute shares of [...] energy by input
        k_share = c.add(f"{label}::share", "div", k / dims_numerator, k / tuple("chlt"))
        assert isinstance(k_share, Key)

        # Minimum and maximum shares occurring over the model horizon in each region
        c.add(k_share + "max", "max", k_share, dim=dims_maxmin)
        c.add(k_share + "min", "min", k_share, dim=dims_maxmin)

        c.apply(to_csv, k_share, name=f"{label} share", header_comment=FE_SHARE_HEADER)
        c.apply(
            to_csv,
            k_share + "max",
            name=f"{label} share max",
            header_comment=FE_SHARE_HEADER + "\n\nMaximum across ya.",
        )
        c.apply(
            to_csv,
            k_share + "min",
            name=f"{label} share min",
            header_comment=FE_SHARE_HEADER + "\n\nMinimum across ya.",
        )

    # Transform ue-share values to the expected format
    base = Key(c.full_key("ue::share"))
    for kind, (label, groupby) in product(
        ("lo", "up"),
        (
            ("A", []),  # Reduced resolution
            ("B", ["node"]),  # Keep distinct nodes
            ("C", ["year"]),  # Keep distinct years
            ("D", ["node", "year"]),  # Full resolution / distinct by node, year
        ),
    ):
        k = base + kind + label
        c.add(k, format_share_constraints, base, "config", kind=kind, groupby=groupby)

        agg = {"lo": "min", "up": "max"}[kind]
        dims = {"node", "year"} - set(groupby)
        c.apply(
            to_csv,
            k,
            name=f"share constraint {kind} {label}",
            float_format="{0:.3f}".format,
            header_comment=f"""Candidate MESSAGEix-GLOBIOM share constraint values

This set shows the {agg}imum values appearing across the {dims} dimension(s).""",
        )


def to_csv(
    c: Computer,
    base: "genno.Key",
    *,
    name: str,
    header_key: Optional["KeyLike"] = None,
    **write_kw,
):
    """Helper to add computations to output data to CSV."""
    # Some strings
    csv, path, fn = f"{name} csv", f"{name} path", f"{name.replace(' ', '-')}.csv"

    # Output path for this parameter
    c.add(path, "make_output_path", "config", name=Path("base", fn))

    # Write to file
    if header_key:
        # write_report() kwargs supplied via reference to another key
        c.add(csv, "write_report", base, path, header_key)
    else:
        # kwargs supplied as keyword arguments to to_csv()/Computer.apply()
        c.add(csv, "write_report", base, path, kwargs=write_kw)

    c.graph[TARGET].append(csv)


def format_share_constraints(
    qty: "AnyQuantity", config: dict, *, kind: str, groupby: list[str] = []
) -> pd.DataFrame:
    """Produce values for :file:`ue_share_constraints.xlsx`.

    This file is used by some code in :mod:`message_data` (unclear where) to produce
    values for the sets ``shares``, ``map_shares_commodity_*``, and
    ``share_commodity_{lo,up}``, but has a different structure from any of these. In
    particular, it has the columns:

    - share_name e.g. "UE_transport_electric"
    - share_tec e.g. "elec_trp" or a comma-separated list of ``technology`` codes.
    - commodity e.g. "transport"
    - level e.g. "useful"
    - node: either "all" or values like "CPA", "RCPA" which are taken to correspond to
      "R11_CPA", "R12_CPA", etc.
    - SSP: either "all", "LED", or possibly other values.
    - share_type: either "lower" or "upper" corresponding respectively to
      ``share_commodity_lo`` or ``share_commodity_up``.
    - target_value: either a value, "baseline", or "TS".
    - 2025 through 2110 (following the "B" list of periods). If "target_value" is "TS",
      these are filled, otherwise empty.
    """
    columns = (
        "share_name share_tec commodity level node SSP share_type target_value"
    ).split()

    if "year" in groupby:

        def maybe_pivot(df: pd.DataFrame) -> pd.DataFrame:
            return (
                df.pivot(columns="year", values="target_value", index=columns[:-1])
                .reset_index()
                .assign(target_value="TS")
                .reindex(columns=columns + sorted(df["year"].unique()))
            )
    else:

        def maybe_pivot(df: pd.DataFrame) -> pd.DataFrame:
            return df.reindex(columns=columns)

    return (
        qty.to_series()
        .rename("target_value")
        .reset_index()
        .rename(columns={"nl": "node", "t": "share_tec", "ya": "year"})
        .query("year >= 2020")
        .groupby(groupby + ["share_tec"])
        .agg(target_value=("target_value", {"lo": "min", "up": "max"}[kind]))
        .reset_index()
        .assign(
            # Transform share_tec values to produce a share_name
            share_name=lambda df: "UE_transport_" + df["share_tec"].str.rstrip("_trp"),
            # Fixed values
            SSP=f"SSP{str(config['transport'].ssp.value)}",
            commodity="transport",
            level="useful",
            node="all"
            if "node" not in groupby
            else lambda df: df["node"].str.lstrip("R12_"),
            share_type={"lo": "lower", "up": "upper"}[kind],
        )
        .pipe(maybe_pivot)
    )
