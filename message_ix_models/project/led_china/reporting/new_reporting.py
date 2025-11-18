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
from message_ix_models.project.led_china.reporting.config import Config
from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)

def load_config(name: str) -> "Config":
    """Load a config for a given reporting variable category from the YAML files.

    This is a thin wrapper around :meth:`.Config.from_files`.
    """
    return Config.from_files(name)

key_base_trade = Key("out:nl-nd-t-ya-c") # TODO: remote t

BILATERAL_FLOWS = 
dict(
    Biomass = ["biomass_shipped"],
    Coal = ["coal_shipped"],
    Oil = ["crudeoil_shipped", "crudeoil_piped"],
    Gas = ["gas_piped", "LNG_shipped"],
)
CONVERT_IAMC = (
    # These are variables that represent trade flows in model outputs
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
    ))

def convert_iamc(c: "genno.Computer"
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
mp = ixmp.Platform()
scenario = message_ix.Scenario(mp, model = 'china_security', scenario = 'SSP2_Baseline')

rep = Reporter.from_scenario(scenario)

# Gross bilateral exports and imports
trade_tecs = [t for t in scenario.set("technology") if "_exp_" in t]
rep.add("t::trade filters", genno.quote(dict(t = trade_tecs)))
rep.add(key_base_trade + "trade", "select", key_base_trade, "t::trade filters", sums = True)
rep.add(key_base_trade + "tradeunits", "apply_units", key_base_trade + "trade", units="GWa/yr")

convert_iamc(rep) # bring in exports as IAMC format

#base_df = rep.get("message::default")
#base_df.to_csv(package_data_path('led_china', 'reporting', 'base_message.csv'))

ge = rep.get("trade gross exports::iamc")
ge = ge.convert_unit("GWa / a", to = "EJ/yr")

ge.to_csv(package_data_path('led_china', 'reporting', 'gross_exports', f'{scenario.model}_{scenario.scenario}.csv'))

gi = rep.get("trade gross imports::iamc")
gi.to_csv(package_data_path('led_china', 'reporting', 'gross_imports', f'{scenario.model}_{scenario.scenario}.csv'))
