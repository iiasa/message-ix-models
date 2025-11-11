"""Reporting/postprocessing for LED China."""

import logging
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any

import genno
import pandas as pd
from genno import Computer, Key, KeySeq, MissingKeyError
from genno.core.key import single_key
import ixmp
import pyam

from message_ix import Reporter
import message_ix

from types import SimpleNamespace

from message_ix_models import Context, ScenarioInfo
from message_ix_models.report.util import add_replacements
from message_ix_models.report import prepare_reporter

from message_ix_models.project.led_china.reporting.config import Config

if TYPE_CHECKING:
    import ixmp
    from genno import Computer

    from message_ix_models import Spec

log = logging.getLogger(__name__)

CONVERT_IAMC = (
    # These are variables that represent trade flows in model outputs
    dict(
        variable="trade gross imports",
        base="out:nd-nl-t-ya-c",
        var=["Gross Imports", "region", "c"],
    ),
    dict(
        variable="trade gross exports",
        base="out:nl-nd-t-ya-c",
        var=["Gross Exports", "nd","c"],
    ))


def convert_iamc(c: "Computer") -> None:
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

# Call reporter
mp = ixmp.Platform()
scenario = message_ix.Scenario(mp, model = 'china_security', scenario = 'SSP2_Baseline')
rep = Reporter.from_scenario(scenario)

trade_tecs = [t for t in scenario.set("technology") if "exp" in t]
rep.set_filters(t=trade_tecs)

convert_iamc(rep)

ge = rep.get("trade gross exports::iamc")
gi = rep.get("trade gross imports::iamc")

out = pyam.concat([ge, gi])

ge.to_csv("trade_gross_exports.csv")

