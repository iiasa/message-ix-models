"""Update the reserve margin."""

import argparse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from message_ix import Scenario


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


if __name__ == "__main__":
    descr = """
    Reserve margin calculation

    Example usage:
    python res_marg.py --version [X] [model name] [scenario name]

    """
    parser = argparse.ArgumentParser(
        description=descr, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    version = "--version : string\n    ix-scenario name"
    parser.add_argument("--version", help=version)
    model = "model : string\n    ix-model name"
    parser.add_argument("model", help=model)
    scenario = "scenario : string\n    ix-scenario name"
    parser.add_argument("scenario", help=scenario)

    # parse cli
    args = parser.parse_args()
    model = args.model
    scenario = args.scenario
    version = int(args.version) if args.version else None

    import ixmp
    import message_ix

    mp = ixmp.Platform()
    scen = message_ix.Scenario(mp, model, scenario, version=version, cache=True)

    main(scen)
