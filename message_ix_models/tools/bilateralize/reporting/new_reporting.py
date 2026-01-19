"""Reporting/postprocessing for LED China."""

import logging
from copy import deepcopy

import pandas as pd
import ixmp
import pyam

from message_ix import Reporter
import message_ix
import genno

from message_ix_models.report import Key
from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)

BILATERAL_FLOWS = dict(
        Biomass = ["biomass_shipped"],
        Coal = ["coal_shipped"],
        Oil = ["crudeoil_shipped", "crudeoil_piped"],
        Gas = ["gas_piped", "LNG_shipped"],
    )

def convert_iamc(c: "genno.Computer", CONVERT_IAMC: dict,
) -> None:
    """Add tasks from :data:`.CONVERT_IAMC`."""
    from message_ix_models.report import iamc as handle_iamc
    from message_ix_models.report import util

    keys = []
    for info in CONVERT_IAMC:
        handle_iamc(c, deepcopy(info))
        keys.append(f"{info['variable']}::iamc")

    # Concatenate IAMC-format tables
    k = Key("trade", tag="iamc")
    c.add(k, "concat", *keys)
    #c.add(k+"units", "apply_units", k, units="GWa/yr")


# Call reporter
def trade_reporting(scenario: message_ix.Scenario = None,
                    model_name: str = None,
                    scenario_name: str = None,
                    out_dir: str = package_data_path('bilateralize', 'reporting', 'trade')):

    # Set up
    key_base_trade = Key("out:nl-nd-t-ya-c")

    CONVERT_IAMC = (
    dict(
        variable="trade gross imports",
        base= key_base_trade + "tradeunits",
        rename={"nd": "region", "ya": "year"},
        var=["Gross Imports", "nl", "c"],
        ),
    dict(
        variable="trade gross exports",
        base=key_base_trade + "tradeunits",
        var=["Gross Exports", "nd","c"],
    ),)

    # Load scenario
    mp = ixmp.Platform()

    if scenario is None:
        scenario = message_ix.Scenario(mp, model = model_name, scenario = scenario_name)

    rep = Reporter.from_scenario(scenario)

    # Gross bilateral exports and imports
    trade_tecs = [t for t in scenario.set("technology") if "_exp_" in t]
    rep.add("t::trade filters", genno.quote(dict(t = trade_tecs)))
    rep.add(key_base_trade + "trade", "select", key_base_trade, "t::trade filters", sums = True)
    rep.add(key_base_trade + "tradeunits", "apply_units", key_base_trade + "trade", units="GWa/yr")

    convert_iamc(rep, CONVERT_IAMC) # bring in exports as IAMC format

    ge = rep.get("trade gross exports::iamc")
    ge = ge.convert_unit("GWa / a", to = "EJ/yr")

    ge.to_csv(out_dir / f'{scenario.model}_{scenario.scenario}_gross_exports.csv')

    gi = rep.get("trade gross imports::iamc")
    gi = gi.convert_unit("GWa / a", to = "EJ/yr")

    gi.to_csv(out_dir / f'{scenario.model}_{scenario.scenario}_gross_imports.csv')

    gt = pyam.concat([ge, gi])
    gt.to_csv(out_dir / f'{scenario.model}_{scenario.scenario}_trade.csv')