"""Reporting for NAVIGATE."""
import logging
import re
from datetime import date
from itertools import count
from pathlib import Path

import pandas as pd
from message_ix.reporting import Reporter
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import nodes_ex_world, private_data_path
from sdmx.model import Code

from message_data.tools.prep_submission import Config, ScenarioConfig

log = logging.getLogger(__name__)


# Functions that perform mapping of certain codes/labels


def _model_name(value: str) -> str:
    # Discard the internal " (NAVIGATE)" suffix
    return value.split(" (NAVIGATE)")[0]


def _scenario_name(value: str) -> str:
    return {
        # NB "baseline" does not appear in the NAVIGATE codelist; choose another value
        "baseline": "NAV_Dem-NPi-ref",
    }.get(value, value)


def _region(value: str) -> str:
    # Discard the "R12_" prefix
    return value.split("R12_")[-1]


#: Regular expression patterns and replacements for variable names
VARIABLE_SUB = (
    (re.compile(r"^Carbon Sequestration\|CCS(.*)$"), r"Carbon Capture|Storage\g<1>"),
    (re.compile(r"^Carbon Sequestration(\|Land Use.*)$"), r"Carbon Removal\g<1>"),
    (
        re.compile(
            r"\|Industry excl Non-Energy Use\|(Chemicals|Non-Ferrous Metals|"
            "Non-Metallic Minerals|Steel)"
        ),
        r"|Industry|\g<1>",
    ),
    # NB this does *not* apply to Final Energy|Solids|Coal, only names with additional
    #    parts
    (re.compile(r"^(Final Energy\|.*\|Solids\|)Coal"), r"\g<1>Fossil"),
    (
        re.compile(
            r"^((Final Energy\|Transportation|Price\|Secondary Energy)\|Liquids\|)Oil"
        ),
        r"\g<1>Fossil",
    ),
    (
        re.compile(
            r"^((Price\|Final Energy\|Residential|Secondary Energy)\|Gases\|)"
            "Natural Gas"
        ),
        r"\g<1>Fossil",
    ),
    (re.compile(r"^(Secondary Energy\|Solids\|)Coal"), r"\g<1>Fossil"),
    (re.compile(r"^(Production\|)Cement"), r"\g<1>Non-Metallic Minerals|Cement|Volume"),
    (re.compile(r"^(Production\|)Chemicals"), r"\g<1>Chemicals|Volume"),
    (
        re.compile(r"^(Production\|)Non-ferrous metals"),
        r"\g<1>Non-Ferrous Metals|Volume",
    ),
    (re.compile(r"\|Steel"), r"|Iron and Steel"),
    (re.compile(r"^(Production\|Iron and Steel)$"), r"\g<1>|Volume"),
    # For NGFS, apparently not needed for NAVIGATE
    # ("Commercial", "Residential and Commercial|Commercial"),
    # ("Residential", "Residential and Commercial|Residential"),
    # ("High Value Chemicals", "High value chemicals"),
    # (r"Non-Ferrous Metals\|Aluminium", "Non-ferrous metals"),
    # (r"Non-Metallic Minerals\|Cement", "Cement"),
    # (r"Liquids\|Biomass$", "Liquids|Bioenergy"),
    # (
    #     re.compile(
    #         "^(?:(Investment\|Infrastructure|Price\|Carbon\|Demand)\|)Transport"
    #     ),
    #     "Transportation",
    # ),
    # (re.compile(r"(?:Price\|)Non-Metallic Minerals\|Cement"), "Industry|Cement"),
    # (re.compile(r"(?:Production\|)Primary\|Chemicals"), "Chemicals"),
)


def _variable(value: str) -> str:
    # Apply each of the replacements
    result = value
    for pattern, repl in VARIABLE_SUB:
        result = re.sub(pattern, repl, result)
    return result


UNIT_MAP = {
    ("GW", "Capacity Additions|Electricity|Storage Capacity"): "GWh/yr",
    ("GW", "Capacity Additions|"): "GW/yr",
    ("Mt NOx/yr", None): "Mt NO2/yr",
    ("US$2010/t CO2 or local currency/t CO2", None): "US$2010/t CO2",
    ("US$2010/GJ or local currency/GJ", None): "US$2010/GJ",
    #
    # The following based on error output from the NAVIGATE scenario Explorer
    # TODO fix these in the reporting per se
    ("My/yr", "Collected Scrap|Non-Ferrous Metals"): "Mt/yr",
    ("My/yr", "Total Scrap|Non-Ferrous Metals"): "Mt/yr",
    ("Mt / a", "Emissions|BC"): "Mt BC/yr",
    ("Mt / a", "Emissions|CF4"): "kt CF4/yr",  # TODO check which prefix is correct
    ("Mt / a", "Emissions|CH4"): "Mt CH4/yr",
    # FIXME this is fragile; correct behaviour depends on CO2 appearing first in the
    #       list because prep_submission.map_units() uses str.startswith. Probably use
    #       regular expressions instead
    ("Mt / a", "Emissions|CO2"): "Mt CO2/yr",
    ("Mt / a", "Emissions|CO"): "Mt CO/yr",
    ("Mt / a", "Emissions|N2O"): "kt N2O/yr",  # TODO check which prefix is correct
    ("Mt / a", "Emissions|NH3"): "Mt NH3/yr",
    ("Mt / a", "Emissions|NOx"): "Mt NO2/yr",
    ("Mt / a", "Emissions|OC"): "Mt OC/yr",
    ("million m3/yr", "Forestry Production|Forest Residues"): "million t DM/yr",
    (
        "Index (2005 = 1)",
        "Price|Agriculture|Non-Energy Crops and Livestock|Index",
    ): "Index (2020 = 1)",
    (
        "Index (2005 = 1)",
        "Price|Agriculture|Non-Energy Crops|Index",
    ): "Index (2020 = 1)",
    ("EJ/yr", re.compile("^Trade$")): "billion US$2010/yr",
    ("Mt CO2-equiv/yr", re.compile("^Trade$")): "billion US$2010/yr",
}


def gen_config(context: Context, fn_ref_1: Path, fn_ref_2: Path) -> Config:
    """Generate configuration for :mod:`.prep_submission`."""
    # Identify the file path for output
    today = date.today().strftime("%Y-%m-%d")
    for index in count():
        out_file = context.get_local_path("report", f"{today}_{index}.xlsx")
        if not out_file.exists():
            break

    cfg = Config(
        source_dir=fn_ref_1.parent,
        out_fil=out_file,
    )

    # Read the variable list to keep from the NAVIGATE repository
    cfg.read_nomenclature(fn_ref_2)

    for (m_source, s_source) in (
        ("MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)", "baseline"),
    ):
        cfg.scenario[(m_source, s_source)] = ScenarioConfig(
            model=_model_name(m_source),
            scenario=_scenario_name(s_source),
            reference_scenario="baseline",
            final=True,
        )

    # Region name mapping
    nodes = get_codes(f"node/{context.regions}")
    nodes = nodes[nodes.index(Code(id="World"))].child
    cfg.name_map["Region"] = {
        _region(node): node for node in map(str, nodes_ex_world(nodes))
    }

    # Unit mapping
    cfg.unit_map = UNIT_MAP.copy()

    # Variable name mapping
    names_1 = set(pd.read_excel(fn_ref_1, usecols=["Variable"])["Variable"])
    names_2 = set(
        pd.read_csv(
            private_data_path("report", "default_variable_definitions.csv"),
            usecols=["Variable"],
        )["Variable"]
    )
    names_3 = cfg.variable_keep

    # Display diagnostic information
    log.info(
        f"""Number of variable names
in reporting output                 {len(names_1) = }
in default_variable_definitions.csv {len(names_2) = }
in NAVIGATE variables.yaml          {len(names_3) = }

{len(names_1 - names_2) = }
{len(names_2 - names_1) = }
{len(names_1 | names_2) = }
{len(names_3 - (names_1 | names_2)) = }"""
    )

    cfg.name_map["Variable"] = dict()
    for var in sorted(names_1 | names_2):
        target = _variable(var)
        if target != var:
            cfg.name_map["Variable"][var] = target

    # More diagnostic info
    names_4 = set(cfg.name_map["Variable"].values())
    log.info(
        f"""Variable mappings constructed
for      {len(names_4)} names
of which {len(names_3 & names_4)} are accepted by NAVIGATE"""
    )

    return cfg


def callback(rep: Reporter, context: Context) -> None:
    """:meth:`.prepare_reporter` callback for NAVIGATE.

    Adds a key "navigate bmt" that invokes buildings, materials, and transport
    reporting.
    """
    from message_data.reporting import register

    # Set up reporting for each of the model variants
    register("model.buildings")
    register("model.material")
    register("model.transport")

    rep.add("remove_all_ts", "remove all ts data", "scenario", "config")
    rep.add(
        "navigate all",
        [
            "buildings all",
            "materials all",
            "transport iamc all",
        ],
    )
