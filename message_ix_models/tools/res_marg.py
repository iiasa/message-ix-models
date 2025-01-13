"""Update the reserve margin.

:func:`main` can also be invoked using the CLI command
:program:`mix-models --url=… res-marg`.
"""

from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context


def main(scen: "Scenario", contin: float = 0.2) -> None:
    """Update the reserve margin.

    For a given scenario, regional reserve margin (=peak load factor) values are updated
    based on the electricity demand in the industry and res/comm sector.

    This is based on the approach described in Johnsonn et al. (2017):
    DOI: https://doi.org/10.1016/j.eneco.2016.07.010 (see section 2.2.1. Firm capacity
    requirement)

    Parameters
    ----------
    scen :
        Scenario to which changes should be applied.
    contin :
        Backup capacity for contingency reasons as percentage of peak capacity (default
        20%).
    """
    demands = scen.par("demand")
    demands = (
        demands[demands.commodity.isin(["i_spec", "rc_spec"])]
        .set_index(["node", "commodity", "year", "level", "time", "unit"])
        .sort_index()
    )
    input_eff = (
        scen.par("input", {"technology": ["elec_t_d"]})
        .set_index(
            [
                "node_loc",
                "year_act",
                "year_vtg",
                "commodity",
                "level",
                "mode",
                "node_origin",
                "technology",
                "time",
                "time_origin",
                "unit",
            ]
        )
        .sort_index()
    )

    with scen.transact("Update reserve-margin constraint"):
        for reg in demands.index.get_level_values("node").unique():
            if "_GLB" in reg:
                continue
            for year in demands.index.get_level_values("year").unique():
                rc_spec = float(
                    demands.loc[reg, "rc_spec", year, "useful", "year"].iloc[0].value
                )
                i_spec = float(
                    demands.loc[reg, "i_spec", year, "useful", "year"].iloc[0].value
                )
                inp = float(
                    input_eff.loc[
                        reg,
                        year,
                        year,
                        "electr",
                        "secondary",
                        "M1",
                        reg,
                        "elec_t_d",
                        "year",
                        "year",
                    ]
                    .iloc[0]
                    .value
                )
                val = (
                    ((i_spec * 1.0 + rc_spec * 2.0) / (i_spec + rc_spec))
                    * (1.0 + contin)
                    * inp
                    * -1.0
                )
                scen.add_par(
                    "relation_activity",
                    {
                        "relation": ["res_marg"],
                        "node_rel": [reg],
                        "year_rel": [year],
                        "node_loc": [reg],
                        "technology": ["elec_t_d"],
                        "year_act": [year],
                        "mode": ["M1"],
                        "value": [val],
                        "unit": ["GWa"],
                    },
                )


@click.command("res-marg")
@click.pass_obj
def cli(ctx: "Context"):
    """Reserve margin calculation."""
    main(ctx.get_scenario())
