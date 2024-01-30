"""Data preparation for the MESSAGEix-GLOBIOM base model."""
from functools import partial
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import message_ix


SCALE_1_HEADER = """Ratio of MESSAGEix-Transport output to IEA EWEB data.

- `t` (technology) codes correspond to IEA `FLOW` codes or equivalent aggregates
  across groups of MESSAGEix-Transport technologies.
- `c` (commodity) codes correspond to MESSAGEix-GLOBIOM commodities or equivalent
  aggregates across groups of IEA `PRODUCT` codes.
"""


def prepare_reporter(rep: "message_ix.Reporter") -> str:
    """Add tasks that produce data to parametrize transport in MESSAGEix-GLOBIOM.

    Returns a key, "base model data". Retrieving the key results in the creation of 3
    files in the reporting output directory for the :class:`.Scenario` being reported
    (see :func:`.make_output_path`):

    - :file:`demand.csv`: This contains MESSAGEix-Transport model solution data
      transformed into ``demand`` parameter data for a base MESSAGEix-GLOBIOM model—that
      is, one without MESSAGEix-Transport. :file:`input-base.csv` is used.
    - :file:`bound_activity_lo.csv`: Same data transformed into ``bound_activity_lo``
      parameter data for the transport technologies ("coal_trp", etc.) appearing in the
      base model.
    - :file:`bound_activity_up.csv`: Same values as ``bound_activity_lo``, multiplied by
      1.005.

    .. todo:: Drop ``bound_activity_lo`` values that are equal to zero.
    """
    from genno import Key, Quantity, quote

    # Final key
    targets = []

    e_iea = Key("energy:n-y-product-flow:iea")
    e_fnp = e_iea.drop("y")
    e_cnlt = e_fnp.drop("flow", "product") * "nl" * "c"
    in_ = Key("in:nl-t-ya-c:transport+units")

    # Transform IEA EWEB data for comparison
    rep.add(e_fnp + "0", "select", e_iea, indexers=dict(y=2020), drop=True)
    rep.add(
        e_fnp + "1", "aggregate", e_fnp + "0", "groups::iea to transport", keep=False
    )
    rep.add(
        e_cnlt, "rename_dims", e_fnp + "1", quote(dict(flow="t", n="nl", product="c"))
    )

    # Transport outputs for comparison
    rep.add(in_ + "0", "select", in_, indexers=dict(ya=2020), drop=True)
    rep.add(in_ + "1", "aggregate", in_ + "0", "groups::transport to iea", keep=False)

    # Scaling factor 1: ratio of MESSAGEix-Transport outputs to IEA data
    k = rep.add("scale 1", "div", in_ + "1", e_cnlt)
    assert isinstance(k, Key)
    rep.add(k + "1", "convert_units", k, units="1 / a")

    # Output path for this parameter
    rep.add(f"{k.name} path", "make_output_path", "config", "scenario", "scale-1.csv")
    # Write to file
    rep.add(
        f"{k.name} csv",
        "write_report",
        k + "1",
        f"{k.name} path",
        dict(header_comment=SCALE_1_HEADER),
    )
    targets.append(f"{k.name} csv")

    # TODO Correct MESSAGEix-Transport outputs for MESSAGEix-GLOBIOM base model (below)
    #      using the high-resolution scaling factor.
    # TODO Compute a scaling factor: overall totals/low resolution.
    # TODO Correct MESSAGEix-Transport outputs using the low-resolution scaling factor.

    # Common calculations

    # Convert "final" energy inputs to transport to "useful energy" outputs, using
    # efficiency data from input-base.csv (in turn, from the base model). This data
    # will be used for `demand`.
    ue = rep.add("ue", "div", "in:nl-ya-c-l-h:transport+units", "input:t-c-h:base")
    assert isinstance(ue, Key)

    # Ensure units: in::transport+units [=] GWa/a and input::base [=] GWa; their ratio
    # gives units 1/a. The base model expects "GWa" for all 3 parameters.
    rep.add(ue + "1", "mul", ue, Quantity(1.0, units="GWa * a"))

    # Select only ya=2020 data for use in `bound_activity_*`
    b_a_l = rep.add(Key("b_a_l", ue.dims), "select", ue + "1", quote(dict(ya=[2020])))

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
    for name, short, base_key, args in [
        ("demand", "demand", (ue + "1").drop("c", "t"), args_demand),
        ("bound_activity_lo", "b_a_l", b_a_l, args_bound_activity),
        ("bound_activity_up", "b_a_u", b_a_u, args_bound_activity),
    ]:
        # More identifiers
        s = f"base model transport {short}"
        key = Key(f"{s}::ixmp")
        targets.append(f"{s} csv")

        # Convert to MESSAGE data structure
        rep.add(key, "as_message_df", base_key, name=name, wrap=False, **args)

        # Sort values
        # TODO Move upstream as a feature of as_message_df()
        dims = list(args["dims"])
        rep.add(key + "1", partial(pd.DataFrame.sort_values, by=dims), key)

        # Output path for this parameter
        rep.add(f"{s} path", "make_output_path", "config", "scenario", f"{name}.csv")

        # Header for this file
        rep.add(f"{s} header", "base_model_data_header", "scenario", name=name)

        # Write to file
        rep.add(targets[-1], "write_report", key + "1", f"{s} path", f"{s} header")

    # Key to trigger all the above
    result = "base model data"
    rep.add(result, targets)

    return result
