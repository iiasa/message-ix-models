"""Data preparation for the MESSAGEix-GLOBIOM base model."""

from functools import partial
from itertools import pairwise
from typing import TYPE_CHECKING, Any, Union

import numpy as np
import pandas as pd
from genno import Computer, KeySeq
from genno.core.key import single_key

from message_ix_models.util import minimum_version

from .key import gdp_exo

if TYPE_CHECKING:
    import genno
    import message_ix
    from genno.core.key import KeyLike
    from genno.types import AnyQuantity


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


@minimum_version("pandas 2")
def smooth(c: Computer, key: "genno.Key", *, dim: str = "ya") -> "genno.Key":
    """Implement ‘smoothing’ for `key` along the dimension `dim`.

    1. Identify values which do not meet a certain criterion. Currently the criterion
       is: the first contiguous sequence of values that are lower than the corresponding
       value in :math:`y_A = y_0` (e.g. in 2020).
    2. Remove those values.
    3. Fill by linear interpolation.
    """
    from genno import Quantity

    assert key.tag != "2"
    ks = KeySeq(key.remove_tag(key.tag or ""))

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

    def clip_nan(qty: "AnyQuantity", coord: Any) -> "AnyQuantity":
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
        return Quantity(
            qty.sel({dim: coord})
            .expand_dims({dim: qty.coords[dim]})
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

    Returns a key, "base model data". Retrieving the key results in the creation of 5
    files in the reporting output directory for the :class:`.Scenario` being reported
    (see :func:`.make_output_path`):

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
    from genno import Key, KeySeq, Quantity, quote

    # Final key
    targets = []

    def _to_csv(base: "Key", name: str, write_kw: Union[dict, "KeyLike", None] = None):
        """Helper to add computations to output data to CSV."""
        # Some strings
        csv, path, fn = f"{name} csv", f"{name} path", f"{name.replace(' ', '-')}.csv"
        # Output path for this parameter
        rep.add(path, "make_output_path", "config", name=fn)
        # Write to file
        rep.add(csv, "write_report", base, path, write_kw or {})
        targets.append(csv)

    # Keys for starting quantities
    e_iea = Key("energy:n-y-product-flow:iea")
    e_fnp = KeySeq(e_iea.drop("y"))
    e_cnlt = Key("energy:c-nl-t:iea+0")
    k = KeySeq("in:nl-t-ya-c-l-h:transport+units")

    # First period
    y0 = rep.get("y0")

    # Transform IEA EWEB data for comparison
    assert y0 == 2020, f"IEA Extended World Energy Balances: no data for y={y0}"
    rep.add(e_fnp[0], "select", e_iea, indexers=dict(y=y0), drop=True)
    rep.add(e_fnp[1], "aggregate", e_fnp[0], "groups::iea to transport", keep=False)
    rep.add(
        e_cnlt,
        "rename_dims",
        e_fnp[1],
        quote(dict(flow="t", n="nl", product="c")),
        sums=True,
    )

    # Transport outputs for comparison
    rep.add(k[0], "select", k.base, indexers=dict(ya=y0), drop=True)
    rep.add(k[1], "aggregate", k[0], "groups::transport to iea", keep=False)

    # Scaling factor 1: ratio of MESSAGEix-Transport outputs to IEA data
    tmp = rep.add("scale 1", "div", k[1], e_cnlt)
    s1 = KeySeq(tmp)
    rep.add(s1[1], "convert_units", s1.base, units="1 / a")
    rep.add(s1[2], "mul", s1[1], Quantity(1.0, units="a"))

    _to_csv(s1[2], s1.name, dict(header_comment=SCALE_1_HEADER))

    # Replace ~0 and ∞ values with 1.0; this avoids x / 0 = inf
    rep.add(s1[3], "where", s1[2], cond=lambda v: (v > 1e-3) & (v != np.inf), other=1.0)
    # Restore original "t" labels to scale-1
    rep.add(s1[4], "select", s1[3], "indexers::iea to transport")
    rep.add(s1[5], "rename_dims", s1[4], quote(dict(t_new="t")))

    # Interpolate the scaling factor from computed value in ya=y₀ to 1.0 in ya ≥ 2050
    rep.add(s1[6], lambda q: q.expand_dims(ya=[y0]), s1[5])
    rep.add(s1[7], lambda q: q.expand_dims(ya=[2050]).clip(1.0, 1.0), s1[5])
    rep.add(s1[8], lambda q: q.expand_dims(ya=[2110]).clip(1.0, 1.0), s1[5])
    rep.add(s1[9], "concat", s1[6], s1[7], s1[8])
    rep.add("ya::coord", lambda v: {"ya": v}, "y::model")
    rep.add(
        s1[10], "interpolate", s1[9], "ya::coord", kwargs=dict(fill_value="extrapolate")
    )
    _to_csv(s1[10], f"{s1.name}-blend", dict(header_comment=SCALE_1_HEADER))

    # Correct MESSAGEix-Transport outputs for the MESSAGEix-base model using the high-
    # resolution scaling factor
    rep.add(k["s1"], "div", k.base, s1[10])

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
    s2 = KeySeq(tmp)

    rep.add(s2[1], "convert_units", s2.base, units="1 / a")
    rep.add(s2[2], "mul", s2[1], Quantity(1.0, units="a"))

    _to_csv(s2[2], s2.name, dict(header_comment=SCALE_2_HEADER))

    # Correct MESSAGEix-Transport outputs using the low-resolution scaling factor
    rep.add(k["s2"], "div", k["s1"], s2[2])

    # Compute for file and plot: transport final energy intensity of GDP PPP
    k_gdp = rep.add("gdp:nl-ya", "rename_dims", gdp_exo, quote({"n": "nl", "y": "ya"}))
    k_fei = single_key(rep.add("fe intensity", "div", k["s2"] / tuple("chlt"), k_gdp))
    rep.add(k_fei + "units", "convert_units", k_fei, units="MJ / USD")
    _to_csv(k_fei + "units", "fe intensity")

    # Convert "final" energy inputs to transport to "useful energy" outputs, using
    # efficiency data from input-base.csv (in turn, from the base model). This data
    # will be used for `demand`.
    # - Sum across the "t" dimension of `k` to avoid conflict with "t" labels introduced
    #   by the data from file.
    tmp = rep.add("ue", "div", k["s2"] / "t", "input:t-c-h:base")
    ue = KeySeq(tmp)

    # Compute shares of useful energy by input
    ue_share = rep.add(
        "ue::share", "div", ue.base / tuple("chl"), ue.base / tuple("chlt")
    )
    assert isinstance(ue_share, Key)

    # Minimum and maximum shares occurring over the model horizon in each region
    rep.add(ue_share + "max", "max", ue_share, dim=("nl", "t"))
    rep.add(ue_share + "min", "min", ue_share, dim=("nl", "t"))

    _to_csv(ue_share, "ue share", dict(header_comment=UE_SHARE_HEADER))
    _to_csv(
        ue_share + "max",
        "ue share max",
        dict(header_comment=UE_SHARE_HEADER + "\n\nMaximum across ya."),
    )
    _to_csv(
        ue_share + "min",
        "ue share min",
        dict(header_comment=UE_SHARE_HEADER + "\n\nMinimum across ya."),
    )

    # Ensure units: in::transport+units [=] GWa/a and input::base [=] GWa; their ratio
    # gives units 1/a. The base model expects "GWa" for all 3 parameters.
    rep.add(ue[1], "mul", ue.base, Quantity(1.0, units="GWa * a"))
    _to_csv(ue[1] / ("c", "t"), "demand no fill", {})

    # 'Smooth' ue[1] data by interpolating any dip below the base year value
    assert rep.apply(smooth, ue[1] / ("c", "t")) == ue[2] / ("c", "t")

    # Select only ya=y₀ data for use in `bound_activity_*`
    b_a_l = rep.add(Key("b_a_l", ue[2].dims), "select", ue[1], quote(dict(ya=[y0])))

    # `bound_activity_up` values are 1.005 * `bound_activity_lo` values
    b_a_u = rep.add("b_a_u", "mul", b_a_l, Quantity(1.005))

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
        key, header = Key(f"{s}::ixmp"), f"{s} header"

        # Convert to MESSAGE data structure
        rep.add(
            key, "as_message_df", base_key, name=name.split("-")[0], wrap=False, **args
        )

        # Sort values
        # TODO Move upstream as a feature of as_message_df()
        dims = list(args["dims"])
        rep.add(key + "1", partial(pd.DataFrame.sort_values, by=dims), key)

        # Header for this file
        rep.add(header, "base_model_data_header", "scenario", name=name)

        _to_csv(key + "1", name, header)

    # Key to trigger all the above
    result = "base model data"
    rep.add(result, targets)

    return result
