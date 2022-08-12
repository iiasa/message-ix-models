"""Reporting for NAVIGATE."""
import logging
import re
from pathlib import Path

import pandas as pd
import yaml
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import nodes_ex_world, private_data_path

log = logging.getLogger(__name__)

#: Format of the prep_submission configuration file. `sheet name` → expected columns.
SHEET_COL = dict(
    scenario_config=(
        "source_model_name",
        "source_scenario_name",
        "target_model_name",
        "target_scenario_name",
        "final_scenario",
        "reference_scenario",
        "sheet_name",
    ),
    region_mapping=("source_name", "target_name"),
    variable_mapping=("source_name", "target_name"),
    unit_mapping=("source_name", "target_name"),
)


# Functions that perform mapping of certain codes/labels


def _model_name(value: str) -> str:
    # Discard the internal " (NAVIGATE)" suffix
    return value.split(" (NAVIGATE)")[0]


def _scenario_name(value: str) -> str:
    # No change
    return value


def _region(value: str) -> str:
    # Discard the "R12_" prefix
    return value.split("R12_")[-1]


#: Regular expression patterns and replacements for variable names
VARIABLE_SUB = (
    (re.compile(r"^Carbon Sequestration\|CCS(.*)$"), r"Carbon Capture|Storage\g<1>"),
    (re.compile(r"^Carbon Sequestration(\|Land Use.*)$"), r"Carbon Removal\g<1>"),
    (re.compile(r"\|Industry excl Non-Energy Use\|"), "|Industry|"),
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
    (re.compile(r"^(Production\|)Steel"), r"\g<1>Iron and Steel|Volume"),
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


def gen_config(context: Context, fn_ref_1: Path, fn_ref_2: Path) -> None:
    """Generate a configuration file for :mod:`.prep_submission`."""
    path = private_data_path("navigate", "prep-submission.xlsx")
    path.parent.mkdir(exist_ok=True)
    ew = pd.ExcelWriter(path)

    sheet = "scenario_config"
    data = pd.DataFrame(
        dict(
            source_model_name=["MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)"],
            source_scenario_name=["baseline"],
            final_scenario=["TRUE"],
            reference_scenario=["baseline"],
        ),
        columns=SHEET_COL[sheet],
        index=[0],
    )

    data = data.assign(
        target_model_name=lambda df: df["source_model_name"].apply(_model_name),
        target_scenario_name=lambda df: df["source_scenario_name"].apply(
            _scenario_name
        ),
    )
    data.to_excel(ew, sheet_name=sheet, index=False)

    sheet = "region_mapping"
    nodes = get_codes(f"node/{context.regions}")
    nodes = nodes[nodes.index("World")].child

    # Note that "source" and "target" have the opposite of the intuitive meanings
    data = pd.DataFrame(
        map(str, nodes_ex_world(nodes)), columns=["target_name"]
    ).assign(source_name=lambda df: df["target_name"].apply(_region))
    assert set(SHEET_COL[sheet]) == set(data.columns)
    data.to_excel(ew, sheet_name=sheet, index=False)

    sheet = "variable_mapping"

    names_1 = set(pd.read_excel(fn_ref_1, usecols=["Variable"])["Variable"])
    names_2 = set(
        pd.read_csv(
            private_data_path("report", "default_variable_definitions.csv"),
            usecols=["Variable"],
        )["Variable"]
    )
    with open(fn_ref_2) as f:
        names_3 = set(map(lambda entry: entry.popitem()[0], yaml.safe_load(f)))

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

    data = (
        pd.DataFrame(sorted(names_1 | names_2), columns=["source_name"])
        .assign(target_name=lambda df: df["source_name"].apply(_variable))
        .query("target_name != source_name")
    )

    # More diagnostic info
    log.info(
        f"""Variable mappings constructed
for      {len(data)} names
of which {len(names_3 & set(data['target_name']))} are accepted by NAVIGATE"""
    )

    assert set(SHEET_COL[sheet]) == set(data.columns)
    data.to_excel(ew, sheet_name=sheet, index=False)

    sheet = "unit_mapping"
    data = pd.DataFrame(columns=SHEET_COL[sheet])
    assert set(SHEET_COL[sheet]) == set(data.columns)
    data.to_excel(ew, sheet_name=sheet, index=False)

    ew.close()
