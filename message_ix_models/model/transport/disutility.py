from typing import TYPE_CHECKING

from genno import Key, Quantity, quote

from . import files as exo

if TYPE_CHECKING:
    from genno import Computer


def prepare_computer(c: "Computer") -> None:
    """Prepare `c` for calculating disutility inputs to LDV usage technologies."""
    key = Key("disutility:n-cg-t-y")

    # Interpolate to ensure all y::model are covered
    k1 = c.add(
        key + "1",
        "interpolate",
        key + "per vehicle",
        # NB "y::coords" is not equivalent here; includes all y, not just y::model
        quote(dict(y=c.get("y::model"))),
        kwargs=dict(fill_value="extrapolate"),
    )
    # Divide disutility per vehicle by annual driving distance per vehicle → disutility
    # per vehicle-km; convert to preferred units
    # TODO add "cg" dimension to ldv activity
    k2 = c.add(key + "2", "div", k1, exo.activity_ldv)
    k3 = c.add(key + "3", "mul", k2, Quantity(1.0, units="vehicle / year"))
    k4 = c.add(key + "4", "convert_units", k3, units="USD / km")

    # Map (t, cg) to (t)
    c.add("indexers::usage", "indexers_usage", "t::transport")
    k5 = c.add(key + "5", "select", k4, "indexers::usage")
    c.add(key, "rename_dims", k5, quote({"t_new": "t"}))

    # Convert to message_ix-ready data
    # - Use y for both year_vtg and year_act. This is because the usage pseudo-
    #   technologies are ephemeral: only existing for year_vtg == year_act.
    common = dict(
        commodity="disutility",
        level="useful",  # TODO Read this from the spec or template
        mode="all",
        time="year",
        time_origin="year",
    )
    dims = dict(
        node_loc="n", node_origin="n", technology="t", year_vtg="y", year_act="y"
    )
    c.add(
        "disutility::ixmp", "as_message_df", key, name="input", dims=dims, common=common
    )

    # Add to the scenario
    c.add("transport_data", __name__, key="disutility::ixmp")
