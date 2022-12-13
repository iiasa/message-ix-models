"""Reporting for NAVIGATE."""
import logging
import re
from datetime import date
from itertools import count
from pathlib import Path
from typing import Collection, Optional

import pandas as pd
from message_ix import Reporter, Scenario
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import identify_nodes, nodes_ex_world, private_data_path
from sdmx.model import Code

from message_data.tools.prep_submission import Config, ScenarioConfig

from . import iter_scenario_codes

log = logging.getLogger(__name__)


# Functions that perform mapping of certain codes/labels


def _model_name(value: str) -> str:
    # Discard the internal " (NAVIGATE)" suffix
    return value.split(" (NAVIGATE)")[0]


def _scenario_name(context: Context, value: str) -> Optional[str]:
    """Return a valid ID from the NAVIGATE scenarios codelist.

    NB "baseline" does not appear in the NAVIGATE codelist.
    """
    if value == "baseline":
        return value if context.get("navigate_dsd") == "iiasa-ece" else None

    candidate = f"NAV_Dem-{value}"
    for code in iter_scenario_codes(context):
        if code.id == candidate:
            return candidate

    return None


def _region(codelist_id: str, value: str) -> str:
    # Discard the prefix
    return value.split(f"{codelist_id}_")[-1]


#: Regular expression patterns and replacements for variable names
VARIABLE_SUB = (
    (re.compile(r"^Carbon Sequestration\|CCS(.*)$"), r"Carbon Capture|Storage\g<1>"),
    (re.compile(r"^Carbon Sequestration(\|Land Use.*)$"), r"Carbon Removal\g<1>"),
    (re.compile(r"(.*)\|Industry excl Non-Energy Use\|(.*)"), r"\g<1>|Industry|\g<2>"),
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
        re.compile(r"^(Production\|Chemicals\|)High Value Chemicals"),
        r"\g<1>High Value Chemicals|Volume",
    ),
    (
        re.compile(r"^(Production\|Non-Ferrous Metals\|)Aluminium"),
        r"\g<1>Aluminium\|Volume",
    ),
    (re.compile(r"\|Steel"), r"|Iron and Steel"),
    (re.compile(r"^(Production\|Iron and Steel)$"), r"\g<1>|Volume"),
    (
        re.compile(
            r"^(Emissions\|CO2\|Energy\|Demand\|Industry\|Non-Metallic Minerals)\|"
            "Cement"
        ),
        r"\g<1>",
    ),
    (
        re.compile(
            r"^(Emissions\|CO2\|Energy\|Demand\|Industry\|Non-Ferrous Metals)\|"
            "Aluminium"
        ),
        r"\g<1>",
    ),
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


def gen_config(
    context: Context, workflow_dir: Path, scenarios: Collection[Scenario]
) -> Config:
    """Generate configuration for :mod:`.prep_submission`.

    Parameters
    ----------
    workflow_dir
        The base path (directory) for the NAVIGATE workflow repository.
    scenarios
        Collection of scenarios.
    """
    # Identify the file path for output
    today = date.today().strftime("%Y-%m-%d")
    _dsd = "" if context.navigate_dsd == "navigate" else f"_{context.navigate_dsd}"
    for index in count():
        out_file = context.get_local_path("report", f"{today}_{index}{_dsd}.xlsx")
        if not out_file.exists():
            break

    # Create base configuration for prep_submission
    cfg = Config(
        out_fil=out_file, source_dir=context.get_local_path("report", "legacy")
    )

    # Read the variable list to keep from the NAVIGATE repository
    cfg.read_nomenclature(workflow_dir)

    # Iterate over scenarios to include
    regions = set()
    for s in scenarios:
        _name = _scenario_name(context, s.scenario)
        if _name is None:
            log.info(f"No target scenario name for {s.url}; skip")
            continue

        cfg.scenario[(s.model, s.scenario)] = ScenarioConfig(
            model=_model_name(s.model),
            scenario=_name,
            reference_scenario="baseline",
            final=True,
        )

        # Identify the node code list for region mapping, below
        regions.add(identify_nodes(s))
        # Construct a filename to read the variable names reported, below
        filename = legacy_output_path(cfg.source_dir, s)

    assert 1 == len(
        regions
    ), f"{len(scenarios)} scenarios have {len(regions)} distinct regions: {regions}"
    node_cl = list(regions)[0]

    # Region name mapping
    nodes = get_codes(f"node/{node_cl}")
    nodes = nodes[nodes.index(Code(id="World"))].child
    if context.navigate_dsd == "navigate":
        # navigate: map e.g. "R12_AFR" to "AFR". This is currently redundant, because
        # the legacy reporting (or its interaction with ixmp's region-alias feature and
        # the particular metadata in the ixmp-dev database) appears to perform this
        # transformation before this point.
        cfg.name_map["Region"] = {
            n: _region(node_cl, n) for n in map(str, nodes_ex_world(nodes))
        }
    else:
        # iiasa-ece: restore e.g. "AFR" produced by legacy reporting to "R12_AFR"
        cfg.name_map["Region"] = {
            _region(node_cl, n): n for n in map(str, nodes_ex_world(nodes))
        }

    log.debug(
        f"Region code mapping for target DSD {context.navigate_dsd!r}:\n"
        + repr(cfg.name_map["Region"])
    )

    # Unit mapping
    cfg.unit_map.update(UNIT_MAP)

    # Variable name mapping

    # Names from the legacy reporting output. Arbitrarily used the filename for the last
    # scenario handled in the above loop; this assumes that the set of variable names in
    # each file is the same (as they should be).
    names_1 = set(pd.read_excel(filename, usecols=["Variable"])["Variable"])
    # Names from the legacy reporting configuration
    names_2 = set(
        pd.read_csv(
            private_data_path("report", "default_variable_definitions.csv"),
            usecols=["Variable"],
        )["Variable"]
    )
    # Names from configuration
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

    # Iterate over names_1 and names_2
    cfg.name_map["Variable"] = dict()
    for var in sorted(names_1 | names_2):
        # Attempt to transform the variable name
        target = _variable(var)
        # Name is different; record it as one to be mapped
        if target != var:
            cfg.name_map["Variable"][var] = target

    # Log more diagnostic info
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

    rep.add("remove_ts", "remove ts data", "scenario", "config", "y0")
    rep.add(
        "navigate all",
        [
            "buildings all",
            "materials all",
            "transport iamc all",
        ],
    )


def legacy_output_path(base_path: Path, scenario: Scenario) -> Path:
    """Return the path where the legacy reporting writes output for `scenario`.

    .. todo:: provide this from a function within the legacy reporting submodule; call
       that function both here and in :func:`.pp_utils.write_xlsx`.
    """
    return base_path.joinpath(f"{scenario.model}_{scenario.scenario}.xlsx")
