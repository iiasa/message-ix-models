"""Reporting for MESSAGEix-Materials.

Created on Mon Mar  8 12:58:21 2021
@author: unlu

This code produces the following outputs:

- message_ix_reporting.xlsx: message_ix level reporting
- check.xlsx: can be used for checking the filtered variables
- New_Reporting_Model_Scenario.xlsx: Reporting including the material variables
- Merged_Model_Scenario.xlsx: Includes all IAMC variables
- Material_global_graphs.pdf
"""
# NB(PNK) as of 2022-09-21, the following appears to be outdated; this code runs with
#         pyam 1.5.0 within the NAVIGATE workflow. Check and maybe remove.
# NOTE: Works with pyam-iamc version 0.9.0
# Problems with the most recent versions:
# 0.11.0 --> Filtering with asterisk * does not work, dataframes are empty.
# Problems with emission and region filtering.
# https://github.com/IAMconsortium/pyam/issues/517
# 0.10.0 --> Plotting gives the follwoing error:
# ValueError: Can not plot data that does not extend for the entire year range.
# There are various tech.s that only have data starting from 2025 or 2030.
# foil_imp AFR, frunace_h2_aluminum, h2_i...

# PACKAGES
import logging
import re
from itertools import product
from typing import List

import matplotlib
import message_ix
import numpy as np
import pandas as pd
import pyam
import xlsxwriter
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from message_ix_models import Context

log = logging.getLogger(__name__)

matplotlib.use("Agg")


def print_full(x):
    pd.set_option("display.max_rows", len(x))
    print(x)
    pd.reset_option("display.max_rows")


#: Replacements applied during processing of specific quantities.
NAME_MAP = {
    "aluminum": "Non-Ferrous Metals|Aluminium",
    "steel": "Steel",
    "cement": "Non-Metallic Minerals|Cement",
    "petro": "Chemicals|High Value Chemicals",
    "ammonia": "Chemicals|Ammonia",
    "BCA": "BC",
    "OCA": "OC",
    "CO2_industry": "CO2",
}

#: Replacements applied to the final results.
NAME_MAP1 = {
    "CO2_industry": "CO2",
    "R11_AFR|R11_AFR": "R11_AFR",
    "R11_CPA|R11_CPA": "R11_CPA",
    "R11_MEA|R11_MEA": "R11_MEA",
    "R11_FSU|R11_FSU": "R11_FSU",
    "R11_PAS|R11_PAS": "R11_PAS",
    "R11_SAS|R11_SAS": "R11_SAS",
    "R11_LAM|R11_LAM": "R11_LAM",
    "R11_NAM|R11_NAM": "R11_NAM",
    "R11_PAO|R11_PAO": "R11_PAO",
    "R11_EEU|R11_EEU": "R11_EEU",
    "R11_WEU|R11_WEU": "R11_WEU",
    "R11_AFR": "R11_AFR",
    "R11_CPA": "R11_CPA",
    "R11_MEA": "R11_MEA",
    "R11_FSU": "R11_FSU",
    "R11_PAS": "R11_PAS",
    "R11_SAS": "R11_SAS",
    "R11_LAM": "R11_LAM",
    "R11_NAM": "R11_NAM",
    "R11_PAO": "R11_PAO",
    "R11_EEU": "R11_EEU",
    "R11_WEU": "R11_WEU",
    "R12_AFR|R12_AFR": "R12_AFR",
    "R12_RCPA|R12_RCPA": "R12_RCPA",
    "R12_MEA|R12_MEA": "R12_MEA",
    "R12_FSU|R12_FSU": "R12_FSU",
    "R12_PAS|R12_PAS": "R12_PAS",
    "R12_SAS|R12_SAS": "R12_SAS",
    "R12_LAM|R12_LAM": "R12_LAM",
    "R12_NAM|R12_NAM": "R12_NAM",
    "R12_PAO|R12_PAO": "R12_PAO",
    "R12_EEU|R12_EEU": "R12_EEU",
    "R12_WEU|R12_WEU": "R12_WEU",
    "R12_CHN|R12_CHN": "R12_CHN",
    "R12_AFR": "R12_AFR",
    "R12_RCPA": "R12_RCPA",
    "R12_MEA": "R12_MEA",
    "R12_FSU": "R12_FSU",
    "R12_PAS": "R12_PAS",
    "R12_SAS": "R12_SAS",
    "R12_LAM": "R12_LAM",
    "R12_NAM": "R12_NAM",
    "R12_PAO": "R12_PAO",
    "R12_EEU": "R12_EEU",
    "R12_WEU": "R12_WEU",
    "R12_CHN": "R12_CHN",
    "World": "R12_GLB",
    "model": "Model",
    "scenario": "Scenario",
    "variable": "Variable",
    "region": "Region",
    "unit": "Unit",
    "BCA": "BC",
    "OCA": "OC",
    "Final Energy|Non-Energy Use|Chemicals|Ammonia": (
        "Final Energy|Industry|Chemicals|Ammonia"
    ),
    "Final Energy|Non-Energy Use|Chemicals|Ammonia|Gases": (
        "Final Energy|Industry|Chemicals|Ammonia|Gases"
    ),
    "Final Energy|Non-Energy Use|Chemicals|Ammonia|Liquids": (
        "Final Energy|Industry|Chemicals|Ammonia|Liquids"
    ),
    "Final Energy|Non-Energy Use|Chemicals|Ammonia|Liquids|Oil": (
        "Final Energy|Industry|Chemicals|Ammonia|Liquids|Oil"
    ),
    "Final Energy|Non-Energy Use|Chemicals|Ammonia|Solids": (
        "Final Energy|Industry|Chemicals|Ammonia|Solids"
    ),
}


def plot_production_al(df: pyam.IamDataFrame, ax, r: str) -> None:
    df = df.copy()

    df.filter(
        variable=[
            "out|useful_material|aluminum|import_aluminum|*",
            "out|final_material|aluminum|prebake_aluminum|*",
            "out|final_material|aluminum|soderberg_aluminum|*",
            "out|new_scrap|aluminum|*",
        ],
        inplace=True,
    )

    if r == "World":
        df.filter(
            variable=[
                "out|final_material|aluminum|prebake_aluminum|*",
                "out|final_material|aluminum|soderberg_aluminum|*",
                "out|new_scrap|aluminum|*",
            ],
            inplace=True,
        )

    df.plot.stack(ax=ax)
    ax.legend(
        [
            "Prebake",
            "Soderberg",
            "Newscrap",
            "Oldscrap_min",
            "Oldscrap_av",
            "Oldscrap_max",
            "import",
        ],
        bbox_to_anchor=(-0.4, 1),
        loc="upper left",
    )
    ax.set_title(f"Aluminium Production_{r}")
    ax.set_xlabel("Year")
    ax.set_ylabel("Mt")


def plot_production_steel(df: pyam.IamDataFrame, ax, r: str) -> None:
    df = df.copy()
    df.filter(
        variable=[
            "out|final_material|steel|*",
            "out|useful_material|steel|import_steel|*",
        ],
        inplace=True,
    )

    if r == "World":
        df.filter(variable=["out|final_material|steel|*"], inplace=True)

    df.plot.stack(ax=ax)
    ax.legend(
        ["Bof steel", "Eaf steel M1", "Eaf steel M2", "Import"],
        bbox_to_anchor=(-0.4, 1),
        loc="upper left",
    )
    ax.set_title(f"Steel Production_{r}")
    ax.set_ylabel("Mt")


def plot_petro(df: pyam.IamDataFrame, ax, r: str) -> None:
    df.plot.stack(ax=ax)
    ax.legend(
        ["atm_gasoil", "naphtha", "vacuum_gasoil", "bioethanol", "ethane", "propane"],
        bbox_to_anchor=(-0.4, 1),
        loc="upper left",
    )
    ax.set_title(f"HVC feedstock {r}")
    ax.set_xlabel("Years")
    ax.set_ylabel("GWa")


def plot_production_cement(df: pyam.IamDataFrame, ax, r: str) -> None:
    df.plot.stack(ax=ax)
    ax.legend(
        ["Ballmill Grinding", "Vertical Mill Grinding"],
        bbox_to_anchor=(-0.6, 1),
        loc="upper left",
    )
    ax.set_title(f"Final Cement Production_{r}")
    ax.set_xlabel("Year")
    ax.set_ylabel("Mt")


def plot_production_cement_clinker(df: pyam.IamDataFrame, ax, r: str) -> None:
    df.plot.stack(ax=ax)
    ax.legend(
        ["Dry Clinker", "Wet Clinker"], bbox_to_anchor=(-0.5, 1), loc="upper left"
    )
    ax.set_title(f"Clinker Cement Production_{r}")
    ax.set_xlabel("Year")
    ax.set_ylabel("Mt")


def plot_emi_aggregates(df: pyam.IamDataFrame, pp, r: str, e: str) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))

    df.plot.stack(ax=ax)
    ax.set_title(f"Emissions_{r}_{e}")
    ax.set_ylabel("Mt")
    ax.legend(bbox_to_anchor=(0.3, 1))

    plt.close()
    pp.savefig(fig)


def report(
    scenario: message_ix.Scenario,
    message_df: pd.DataFrame,
    years: List[int],
    nodes: List[str],
    config: dict,
) -> None:
    """Produces the material related variables.

    .. todo:: Expand docstring.
    """
    # In order to avoid confusion in the second reporting stage there should
    # no existing timeseries uploaded in the scenairo. Clear these except the
    # residential and commercial ones since they should be always included.

    # Activate this part to keep the residential and commercial variables
    # when the model is run with the buildigns linkage.
    # df_rem = df_rem[~(df_rem["variable"].str.contains("Residential") | \
    # df_rem["variable"].str.contains("Commercial"))]

    # Temporary to add the transport variables
    # transport_path = os.path.join('C:\\', 'Users','unlu','Documents',
    # 'MyDocuments_IIASA','Material_Flow', 'jupyter_notebooks', '2022-06-02_0.xlsx')
    # df_transport = pd.read_excel(transport_path)
    # scenario.check_out(timeseries_only=True)
    # scenario.add_timeseries(df_transport)
    # scenario.commit('Added transport timeseries')

    # Path for materials reporting output
    directory = config["output_path"].expanduser().joinpath("materials")
    directory.mkdir(exist_ok=True)

    # Replace region labels like R12_AFR|R12_AFR with simply R12_AFR
    # TODO locate the cause of this upstream and fix
    df = message_df.rename({"region": {f"{n}|{n}": n for n in nodes}})

    # Dump output of message_ix built-in reporting to an Excel file
    name = directory.joinpath("message_ix_reporting.xlsx")
    df.to_excel(name)
    log.info(f"'message::default' report written to {name}")
    log.info(f"{len(df)} rows")
    log.info(f"{len(df.variable)} unique variable names")

    # Obtain a pyam dataframe
    # FIXME(PNK) this re-reads the file above. This seems unnecessary, and can be slow.
    df = pyam.IamDataFrame(pd.read_excel(name).fillna(dict(Unit="")))

    # Subsequent code expects that the nodes set contains "World", but not "_GLB"
    # NB cannot use message_ix_models.util.nodes_ex_world(), which excludes both
    nodes = list(filter(lambda n: not n.endswith("_GLB"), nodes))

    # Filter variables necessary for materials reporting
    df.filter(
        region=nodes,
        year=years,
        variable=[
            "out|new_scrap|aluminum|*",
            "out|final_material|aluminum|prebake_aluminum|M1",
            "out|final_material|aluminum|secondary_aluminum|M1",
            "out|final_material|aluminum|soderberg_aluminum|M1",
            "out|new_scrap|steel|*",
            "in|new_scrap|steel|*",
            "out|final_material|steel|*",
            "out|useful_material|steel|import_steel|*",
            "out|secondary_material|NH3|*",
            "in|useful_material|steel|export_steel|*",
            "out|useful_material|aluminum|import_aluminum|*",
            "in|useful_material|aluminum|export_aluminum|*",
            "out|final_material|*|import_petro|*",
            "in|final_material|*|export_petro|*",
            "out|secondary|lightoil|loil_imp|*",
            "out|secondary|fueloil|foil_imp|*",
            "out|tertiary_material|clinker_cement|*",
            "out|demand|cement|*",
            "out|final_material|BTX|*",
            "out|final_material|ethylene|*",
            "out|final_material|propylene|*",
            "out|secondary|fueloil|agg_ref|*",
            "out|secondary|lightoil|agg_ref|*",
            "out|useful|i_therm|solar_i|M1",
            "in|final|*",
            "in|secondary|coal|coal_NH3|M1",
            "in|secondary|electr|NH3_to_N_fertil|M1",
            "in|secondary|electr|coal_NH3|M1",
            "in|secondary|electr|electr_NH3|M1",
            "in|secondary|electr|gas_NH3|M1",
            "in|secondary|fueloil|fueloil_NH3|M1",
            "in|secondary|gas|gas_NH3|M1",
            "in|secondary|coal|coal_NH3_ccs|M1",
            "in|secondary|electr|coal_NH3_ccs|M1",
            "in|secondary|electr|gas_NH3_ccs|M1",
            "in|secondary|fueloil|fueloil_NH3_ccs|M1",
            "in|secondary|gas|gas_NH3_ccs|M1",
            "in|primary|biomass|biomass_NH3|M1",
            "in|seconday|electr|biomass_NH3|M1",
            "in|primary|biomass|biomass_NH3_ccs|M1",
            "in|secondary|electr|biomass_NH3_ccs|M1",
            "in|desulfurized|*|steam_cracker_petro|*",
            "in|secondary_material|*|steam_cracker_petro|*",
            "in|product|aluminum|scrap_recovery|M1",
            "in|dummy_end_of_life|aluminum|scrap_recovery_aluminum|M1",
            "in|dummy_end_of_life|steel|scrap_recovery_steel|M1",
            "out|dummy_end_of_life|aluminum|total_EOL_aluminum|M1",
            "out|dummy_end_of_life|steel|total_EOL_steel|M1",
            "out|dummy_end_of_life|cement|total_EOL_cement|M1",
            "in|product|steel|scrap_recovery_steel|M1",
            "out|end_of_life|*",
            "emis|CO2_industry|*",
            "emis|CF4|*",
            "emis|SO2|*",
            "emis|NOx|*",
            "emis|CH4|*",
            "emis|N2O|*",
            "emis|BCA|*",
            "emis|CO|*",
            "emis|NH3|*",
            "emis|NOx|*",
            "emis|OCA|*",
            "emis|CO2|*",
        ],
        inplace=True,
    )
    log.info(f"{df = }\n{len(df.variable)} unique variable names")

    # Compute global totals
    df.aggregate_region(df.variable, region="World", method=sum, append=True)

    df.to_excel(directory.joinpath("check.xlsx"))
    log.info(f"Necessary variables are filtered; {len(df)} in total")

    # Obtain the model and scenario name
    model_name = scenario.model
    scenario_name = scenario.scenario

    # Create an empty pyam dataframe to store the new variables

    empty_template_path = directory.joinpath("empty_template.xlsx")
    workbook = xlsxwriter.Workbook(empty_template_path)
    worksheet = workbook.add_worksheet()
    worksheet.write("A1", "Model")
    worksheet.write("B1", "Scenario")
    worksheet.write("C1", "Region")
    worksheet.write("D1", "Variable")
    worksheet.write("E1", "Unit")
    columns = "F1 G1 H1 I1 J1 K1 L1 M1 N1 O1 P1 Q1 R1".split()

    for yr, col in zip(years, columns):
        worksheet.write(col, yr)
    workbook.close()

    df_final = pyam.IamDataFrame(empty_template_path)
    print("Empty template for new variables created")

    # Create a pdf file with figures
    path = directory.joinpath("Material_global_graphs.pdf")
    pp = PdfPages(path)
    # pp = PdfPages("Material_global_graphs.pdf")

    # Reporting and Plotting

    print("Production plots and variables are being generated")
    for r in nodes:
        # PRODUCTION - PLOTS
        # Needs to be checked again to see whether the graphs are correct

        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 10))
        fig.tight_layout(pad=10.0)

        # ALUMINUM
        df_al = df.filter(
            region=r, year=years, variable=["out|*|aluminum|*", "in|*|aluminum|*"]
        )
        df_al.convert_unit("", to="Mt/yr", factor=1, inplace=True)

        plot_production_al(df_al.copy(), ax1, r)

        # STEEL

        df_steel = df.copy()
        df_steel.filter(region=r, year=years, inplace=True)
        df_steel.filter(variable=["out|*|steel|*", "in|*|steel|*"], inplace=True)
        df_steel.convert_unit("", to="Mt/yr", factor=1, inplace=True)

        plot_production_steel(df_steel.copy(), ax2, r)

        # PETRO

        df_petro = df.copy()
        df_petro.filter(region=r, year=years, inplace=True)
        df_petro.filter(
            variable=[
                "in|final|ethanol|ethanol_to_ethylene_petro|M1",
                "in|desulfurized|*|steam_cracker_petro|*",
                "in|secondary_material|*|steam_cracker_petro|*",
            ],
            inplace=True,
        )
        df_petro.convert_unit("", to="Mt/yr", factor=1, inplace=True)

        if r == "World":
            df_petro.filter(
                variable=[
                    "in|final|ethanol|ethanol_to_ethylene_petro|M1",
                    "in|desulfurized|*|steam_cracker_petro|*",
                    "in|secondary_material|*|steam_cracker_petro|*",
                ],
                inplace=True,
            )

        plot_petro(df_petro.copy(), ax3, r)

        plt.close()
        pp.savefig(fig)

        # PRODUCTION - IAMC Variables

        # ALUMINUM

        # Primary Production
        primary_al_vars = [
            "out|final_material|aluminum|prebake_aluminum|M1",
            "out|final_material|aluminum|soderberg_aluminum|M1",
        ]

        # Secondary Production
        secondary_al_vars = ["out|final_material|aluminum|secondary_aluminum|M1"]

        # Collected Scrap
        collected_scrap_al_vars = [
            "out|new_scrap|aluminum|manuf_aluminum|M1",
            "in|dummy_end_of_life|aluminum|scrap_recovery_aluminum|M1",
        ]

        # Total Available Scrap:
        #  New scrap + The end of life products (exegenous assumption)
        # + from power and buildings sector

        total_scrap_al_vars = [
            "out|new_scrap|aluminum|manuf_aluminum|M1",
            "out|dummy_end_of_life|aluminum|total_EOL_aluminum|M1",
        ]

        new_scrap_al_vars = ["out|new_scrap|aluminum|manuf_aluminum|M1"]
        old_scrap_al_vars = ["out|dummy_end_of_life|aluminum|total_EOL_aluminum|M1"]

        df_al.aggregate(
            "Production|Primary|Non-Ferrous Metals|Aluminium",
            components=primary_al_vars,
            append=True,
        )
        df_al.aggregate(
            "Production|Secondary|Non-Ferrous Metals|Aluminium",
            components=secondary_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Production|Non-Ferrous Metals|Aluminium",
            components=secondary_al_vars + primary_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Collected Scrap|Non-Ferrous Metals|Aluminium",
            components=collected_scrap_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Collected Scrap|Non-Ferrous Metals",
            components=collected_scrap_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Total Scrap|Non-Ferrous Metals|Aluminium",
            components=total_scrap_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Total Scrap|Non-Ferrous Metals",
            components=total_scrap_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Total Scrap|Non-Ferrous Metals|Aluminium|New Scrap",
            components=new_scrap_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Total Scrap|Non-Ferrous Metals|Aluminium|Old Scrap",
            components=old_scrap_al_vars,
            append=True,
        )

        df_al.aggregate(
            "Production|Non-Ferrous Metals",
            components=primary_al_vars + secondary_al_vars,
            append=True,
        )

        df_al.filter(
            variable=[
                "Production|Primary|Non-Ferrous Metals|Aluminium",
                "Production|Secondary|Non-Ferrous Metals|Aluminium",
                "Production|Non-Ferrous Metals|Aluminium",
                "Production|Non-Ferrous Metals",
                "Collected Scrap|Non-Ferrous Metals|Aluminium",
                "Collected Scrap|Non-Ferrous Metals",
                "Total Scrap|Non-Ferrous Metals",
                "Total Scrap|Non-Ferrous Metals|Aluminium",
                "Total Scrap|Non-Ferrous Metals|Aluminium|New Scrap",
                "Total Scrap|Non-Ferrous Metals|Aluminium|Old Scrap",
            ],
            inplace=True,
        )

        # STEEL

        # bof_steel also uses a certain share of scrap but this is neglegcted in
        # the reporting.

        # eaf_steel has three modes M1,M2,M3: Only M2 has scrap input.

        primary_steel_vars = [
            "out|final_material|steel|bof_steel|M1",
            "out|final_material|steel|eaf_steel|M1",
            "out|final_material|steel|eaf_steel|M3",
        ]

        secondary_steel_vars = [
            "out|final_material|steel|eaf_steel|M2",
            "in|new_scrap|steel|bof_steel|M1",
        ]

        collected_scrap_steel_vars = [
            "in|dummy_end_of_life|steel|scrap_recovery_steel|M1"
        ]
        total_scrap_steel_vars = ["out|dummy_end_of_life|steel|total_EOL_steel|M1"]

        new_scrap_steel_vars = ["out|new_scrap|steel|manuf_steel|M1"]
        old_scrap_steel_vars = ["out|dummy_end_of_life|steel|total_EOL_steel|M1"]

        df_steel.aggregate(
            "Production|Primary|Steel (before sub.)",
            components=primary_steel_vars,
            append=True,
        )

        df_steel.subtract(
            "Production|Primary|Steel (before sub.)",
            "in|new_scrap|steel|bof_steel|M1",
            "Production|Primary|Steel",
            append=True,
        )

        df_steel.aggregate(
            "Production|Secondary|Steel",
            components=secondary_steel_vars,
            append=True,
        )

        df_steel.aggregate(
            "Production|Steel",
            components=["Production|Primary|Steel", "Production|Secondary|Steel"],
            append=True,
        )

        df_steel.aggregate(
            "Collected Scrap|Steel",
            components=collected_scrap_steel_vars,
            append=True,
        )
        df_steel.aggregate(
            "Total Scrap|Steel", components=total_scrap_steel_vars, append=True
        )

        df_steel.aggregate(
            "Total Scrap|Steel|Old Scrap", components=old_scrap_steel_vars, append=True
        )

        df_steel.aggregate(
            "Total Scrap|Steel|New Scrap", components=new_scrap_steel_vars, append=True
        )

        df_steel.filter(
            variable=[
                "Production|Primary|Steel",
                "Production|Secondary|Steel",
                "Production|Steel",
                "Collected Scrap|Steel",
                "Total Scrap|Steel",
                "Total Scrap|Steel|Old Scrap",
                "Total Scrap|Steel|New Scrap",
            ],
            inplace=True,
        )

        # CHEMICALS

        df_chemicals = df.copy()
        df_chemicals.filter(region=r, year=years, inplace=True)
        df_chemicals.filter(
            variable=[
                "out|secondary_material|NH3|*",
                "out|final_material|ethylene|*",
                "out|final_material|propylene|*",
                "out|final_material|BTX|*",
            ],
            inplace=True,
        )
        df_chemicals.convert_unit("", to="Mt/yr", factor=1, inplace=True)

        # AMMONIA

        primary_ammonia_vars = [
            "out|secondary_material|NH3|gas_NH3|M1",
            "out|secondary_material|NH3|coal_NH3|M1",
            "out|secondary_material|NH3|biomass_NH3|M1",
            "out|secondary_material|NH3|fueloil_NH3|M1",
            "out|secondary_material|NH3|electr_NH3|M1",
        ]

        df_chemicals.aggregate(
            "Production|Primary|Chemicals|Ammonia",
            components=primary_ammonia_vars,
            append=True,
        )

        df_chemicals.aggregate(
            "Production|Chemicals|Ammonia",
            components=primary_ammonia_vars,
            append=True,
        )

        # High Value Chemicals

        intermediate_petro_vars = [
            "out|final_material|ethylene|ethanol_to_ethylene_petro|M1",
            "out|final_material|ethylene|steam_cracker_petro|atm_gasoil",
            "out|final_material|ethylene|steam_cracker_petro|naphtha",
            "out|final_material|ethylene|steam_cracker_petro|vacuum_gasoil",
            "out|final_material|ethylene|steam_cracker_petro|ethane",
            "out|final_material|ethylene|steam_cracker_petro|propane",
            "out|final_material|propylene|steam_cracker_petro|atm_gasoil",
            "out|final_material|propylene|steam_cracker_petro|naphtha",
            "out|final_material|propylene|steam_cracker_petro|vacuum_gasoil",
            "out|final_material|propylene|steam_cracker_petro|propane",
            "out|final_material|BTX|steam_cracker_petro|atm_gasoil",
            "out|final_material|BTX|steam_cracker_petro|naphtha",
            "out|final_material|BTX|steam_cracker_petro|vacuum_gasoil",
        ]

        df_chemicals.aggregate(
            "Production|Primary|Chemicals|High Value Chemicals",
            components=intermediate_petro_vars,
            append=True,
        )

        df_chemicals.aggregate(
            "Production|Chemicals|High Value Chemicals",
            components=intermediate_petro_vars,
            append=True,
        )

        # Totals

        chemicals_vars = intermediate_petro_vars + primary_ammonia_vars
        df_chemicals.aggregate(
            "Production|Primary|Chemicals",
            components=chemicals_vars,
            append=True,
        )

        df_chemicals.aggregate(
            "Production|Chemicals",
            components=chemicals_vars,
            append=True,
        )

        df_chemicals.filter(
            variable=[
                "Production|Primary|Chemicals|High Value Chemicals",
                "Production|Chemicals|High Value Chemicals",
                "Production|Primary|Chemicals",
                "Production|Chemicals",
                "Production|Primary|Chemicals|Ammonia",
                "Production|Chemicals|Ammonia",
            ],
            inplace=True,
        )

        # Add to final data_frame
        df_final.append(df_al, inplace=True)
        df_final.append(df_steel, inplace=True)
        df_final.append(df_chemicals, inplace=True)

    # CEMENT
    for r in nodes:
        # PRODUCTION - PLOT

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 10))
        fig.tight_layout(pad=15.0)

        # Clinker cement

        df_cement_clinker = df.copy()
        df_cement_clinker.filter(region=r, year=years, inplace=True)
        df_cement_clinker.filter(
            variable=["out|tertiary_material|clinker_cement|*"], inplace=True
        )

        plot_production_cement_clinker(df_cement_clinker, ax1, r)

        # Final prodcut cement

        df_cement = df.copy()
        df_cement.filter(region=r, year=years, inplace=True)
        df_cement.filter(variable=["out|demand|cement|*"], inplace=True)

        plot_production_cement(df_cement, ax2, r)

        plt.close()
        pp.savefig(fig)

        # PRODUCTION - IAMC format

        primary_cement_vars = [
            "out|demand|cement|grinding_ballmill_cement|M1",
            "out|demand|cement|grinding_vertmill_cement|M1",
        ]

        total_scrap_cement_vars = ["out|dummy_end_of_life|cement|total_EOL_cement|M1"]

        df_cement.convert_unit("", to="Mt/yr", factor=1, inplace=True)
        df_cement.aggregate(
            "Production|Primary|Cement",
            components=primary_cement_vars,
            append=True,
        )

        df_cement.aggregate(
            "Production|Non-Metallic Minerals",
            components=primary_cement_vars,
            append=True,
        )

        df_cement.aggregate(
            "Production|Cement",
            components=primary_cement_vars,
            append=True,
        )

        df_cement.aggregate(
            "Total Scrap|Non-Metallic Minerals",
            components=total_scrap_cement_vars,
            append=True,
        )

        df_cement.aggregate(
            "Total Scrap|Non-Metallic Minerals|Cement",
            components=total_scrap_cement_vars,
            append=True,
        )
        df_cement.filter(
            variable=[
                "Production|Primary|Cement",
                "Production|Cement",
                "Total Scrap|Non-Metallic Minerals"
                "Total Scrap|Non-Metallic Minerals|Cement",
            ],
            inplace=True,
        )
        df_final.append(df_cement, inplace=True)

    # FINAL ENERGY BY FUELS (Only Non-Energy Use)
    # Only inclides High Value Chemicals
    # Ammonia is not seperately included as the model input valus are combined for
    # feedstock and energy.

    # The ammonia related variables are not included in the main filter but left in the
    # remaining code to be reference for the future changes.

    print("Final Energy by fuels only non-energy use is being printed.")
    commodities = ["gas", "liquids", "solids", "all"]

    for c, r in product(commodities, nodes):
        df_final_energy = df.copy()

        # GWa to EJ/yr
        df_final_energy.convert_unit("", to="GWa", factor=1, inplace=True)
        df_final_energy.convert_unit("GWa", to="EJ/yr", factor=0.03154, inplace=True)
        df_final_energy.filter(region=r, year=years, inplace=True)
        df_final_energy.filter(
            variable=["in|final|*|cokeoven_steel|*"], keep=False, inplace=True
        )
        df_final_energy.filter(
            variable=[
                "in|final|atm_gasoil|*",
                "in|final|vacuum_gasoil|*",
                "in|final|naphtha|*",
                "in|final|*|coal_fs|*",
                "in|final|*|methanol_fs|*",
                "in|final|*|ethanol_fs|*",
                "in|final|ethanol|ethanol_to_ethylene_petro|*",
                "in|final|*|foil_fs|*",
                "in|final|gas|gas_processing_petro|*",
                "in|final|*|loil_fs|*",
                "in|final|*|gas_fs|*",
            ],
            inplace=True,
        )

        if c == "gas":
            df_final_energy.filter(
                variable=[
                    "in|final|gas|*",
                    "in|secondary|gas|gas_NH3|M1",
                    "in|secondary|gas|gas_NH3_ccs|M1",
                ],
                inplace=True,
            )
        # Do not include gasoil and naphtha feedstock
        if c == "liquids":
            df_final_energy.filter(
                variable=[
                    "in|final|ethanol|ethanol_to_ethylene_petro|*",
                    "in|final|*|foil_fs|*",
                    "in|final|*|loil_fs|*",
                    "in|final|*|methanol_fs|*",
                    "in|final|*|ethanol_fs|*",
                    "in|final|atm_gasoil|*",
                    "in|final|vacuum_gasoil|*",
                    "in|final|naphtha|*",
                    "in|secondary|fueloil|fueloil_NH3|M1",
                    "in|secondary|fueloil|fueloil_NH3_ccs|M1",
                ],
                inplace=True,
            )
        if c == "solids":
            df_final_energy.filter(
                variable=[
                    "in|final|biomass|*",
                    "in|final|coal|*",
                    "in|secondary|coal|coal_NH3|M1",
                    "in|secondary|coal|coal_NH3_ccs|M1",
                    "in|primary|biomass|biomass_NH3_ccs|M1",
                    "in|primary|biomass|biomass_NH3|M1",
                ],
                inplace=True,
            )

        all_flows = df_final_energy.timeseries().reset_index()
        splitted_vars = [v.split("|") for v in all_flows.variable]
        aux1_df = pd.DataFrame(
            splitted_vars,
            columns=["flow_type", "level", "commodity", "technology", "mode"],
        )
        aux2_df = pd.concat(
            [all_flows.reset_index(drop=True), aux1_df.reset_index(drop=True)],
            axis=1,
        )

        # Include only the related industry sector variables
        # Aluminum, cement and steel do not have feedstock use

        var_sectors = [v for v in aux2_df["variable"].values]

        aux2_df = aux2_df[aux2_df["variable"].isin(var_sectors)]

        df_final_energy.filter(variable=var_sectors, inplace=True)

        # Aggregate

        if c == "all":
            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Non-Energy Use"], inplace=True
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "gas":

            # Can not distinguish by type Gases (natural gas, biomass, synthetic fossil,
            # efuel) (coal_gas), from biomass (gas_bio), natural gas (gas_bal): All go
            # into secondary level.
            # Can not be distinguished in the final level.
            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Gases",
                components=var_sectors,
                append=True,
            )

            df_final_energy.filter(
                variable=["Final Energy|Non-Energy Use|Gases"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

        elif c == "liquids":

            # All liquids
            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Liquids",
                components=var_sectors,
                append=True,
            )

            # Only bios

            filter_vars = [
                v
                for v in aux2_df["variable"].values
                if (("ethanol" in v) & ("methanol" not in v))
            ]
            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Liquids|Biomass",
                components=filter_vars,
                append=True,
            )
            # Fossils

            filter_vars = [
                v
                for v in aux2_df["variable"].values
                if (
                    ("lightoil" in v)
                    | ("fueloil" in v)
                    | ("atm_gasoil" in v)
                    | ("vacuum_gasoil" in v)
                    | ("naphtha" in v)
                )
            ]

            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Liquids|Oil",
                components=filter_vars,
                append=True,
            )

            # Other

            filter_vars = [v for v in aux2_df["variable"].values if (("methanol" in v))]

            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Liquids|Coal",
                components=filter_vars,
                append=True,
            )

            df_final_energy.filter(
                variable=[
                    "Final Energy|Non-Energy Use|Liquids",
                    "Final Energy|Non-Energy Use|Liquids|Oil",
                    "Final Energy|Non-Energy Use|Liquids|Biomass",
                    "Final Energy|Non-Energy Use|Liquids|Coal",
                ],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "solids":

            # All
            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Solids",
                components=var_sectors,
                append=True,
            )
            # Bio
            filter_vars = [v for v in aux2_df["variable"].values if ("biomass" in v)]
            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Solids|Biomass",
                components=filter_vars,
                append=True,
            )

            # Fossil
            filter_vars = [v for v in aux2_df["variable"].values if ("coal" in v)]
            df_final_energy.aggregate(
                "Final Energy|Non-Energy Use|Solids|Coal",
                components=filter_vars,
                append=True,
            )
            df_final_energy.filter(
                variable=[
                    "Final Energy|Non-Energy Use|Solids",
                    "Final Energy|Non-Energy Use|Solids|Biomass",
                    "Final Energy|Non-Energy Use|Solids|Coal",
                ],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

    # FINAL ENERGY BY FUELS (Excluding Non-Energy Use)
    # For ammonia only electricity use is included since only this has seperate
    # input values in the model.

    print("Final Energy by fuels excluding non-energy use is being printed.")
    commodities = [
        "electr",
        "gas",
        "hydrogen",
        "liquids",
        "solids",
        "heat",
        "other",
        "all",
    ]
    for c, r in product(commodities, nodes):
        df_final_energy = df.copy()
        df_final_energy.convert_unit("", to="GWa", factor=1, inplace=True)
        df_final_energy.filter(region=r, year=years, inplace=True)
        df_final_energy.filter(
            variable=[
                "in|final|*|cokeoven_steel|*",
                "in|final|co_gas|*",
                "in|final|bf_gas|*",
            ],
            keep=False,
            inplace=True,
        )

        if c == "electr":
            df_final_energy.filter(
                variable=[
                    "in|final|electr|*",
                    "in|secondary|electr|NH3_to_N_fertil|M1",
                    "in|secondary|electr|coal_NH3|M1",
                    "in|secondary|electr|electr_NH3|M1",
                    "in|secondary|electr|fueloil_NH3|M1",
                    "in|secondary|electr|gas_NH3|M1",
                    "in|secondary|electr|coal_NH3_ccs|M1",
                    "in|secondary|electr|fueloil_NH3_ccs|M1",
                    "in|secondary|electr|gas_NH3_ccs|M1",
                    "in|secondary|electr|biomass_NH3_ccs|M1",
                    "in|secondary|electr|biomass_NH3|M1",
                ],
                inplace=True,
            )
        elif c == "gas":
            df_final_energy.filter(variable=["in|final|gas|*"], inplace=True)
        elif c == "liquids":
            # Do not include gasoil and naphtha feedstock
            df_final_energy.filter(
                variable=[
                    "in|final|ethanol|*",
                    "in|final|fueloil|*",
                    "in|final|lightoil|*",
                    "in|final|methanol|*",
                ],
                inplace=True,
            )
        elif c == "solids":
            df_final_energy.filter(
                variable=[
                    "in|final|biomass|*",
                    "in|final|coal|*",
                    "in|final|coke_iron|*",
                ],
                inplace=True,
            )
        elif c == "hydrogen":
            df_final_energy.filter(variable=["in|final|hydrogen|*"], inplace=True)
        elif c == "heat":
            df_final_energy.filter(variable=["in|final|d_heat|*"], inplace=True)
        elif c == "other":
            df_final_energy.filter(variable=["out|useful|i_therm|*"], inplace=True)
        elif c == "all":
            df_final_energy.filter(
                variable=[
                    "in|final|*",
                    "in|secondary|electr|NH3_to_N_fertil|M1",
                    "in|secondary|electr|coal_NH3|M1",
                    "in|secondary|electr|electr_NH3|M1",
                    "in|secondary|electr|fueloil_NH3|M1",
                    "in|secondary|electr|gas_NH3|M1",
                    "in|secondary|electr|coal_NH3_ccs|M1",
                    "in|secondary|electr|fueloil_NH3_ccs|M1",
                    "in|secondary|electr|gas_NH3_ccs|M1",
                    "in|secondary|electr|biomass_NH3_ccs|M1",
                    "in|secondary|electr|biomass_NH3|M1",
                    "out|useful|i_therm|solar_i|M1",
                ],
                inplace=True,
            )

        all_flows = df_final_energy.timeseries().reset_index()
        splitted_vars = [v.split("|") for v in all_flows.variable]
        aux1_df = pd.DataFrame(
            splitted_vars,
            columns=["flow_type", "level", "commodity", "technology", "mode"],
        )
        aux2_df = pd.concat(
            [all_flows.reset_index(drop=True), aux1_df.reset_index(drop=True)],
            axis=1,
        )

        # Include only the related industry sector variables and state some exceptions
        var_sectors = list(
            filter(
                # 4th element (technology) ends with one of the following
                lambda v: re.match(
                    ".*_(cement|steel|aluminum|petro|i|I|NH3)$", v.split("|")[3]
                )
                # Exclude specific technologies
                and not re.match("(ethanol_to_ethylene|gas_processing)_petro", v)
                # Exclude inputs of 3 specific commodities to steam_cracker_petro
                and not re.match(
                    r"^in.final.((atm|vacuum)_gasoil|naphtha).steam_cracker_petro.\1", v
                ),
                aux2_df["variable"].values,
            )
        )
        aux2_df = aux2_df[aux2_df["variable"].isin(var_sectors)]

        df_final_energy.filter(variable=var_sectors, inplace=True)

        # Aggregate

        if c == "all":
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry excl Non-Energy Use"], inplace=True
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "electr":

            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Electricity",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry excl Non-Energy Use|Electricity"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "gas":

            # Can not distinguish by type Gases (natural gas, biomass, synthetic
            # fossil, efuel) (coal_gas), from biomass (gas_bio), natural gas
            # (gas_bal): All go into secondary level
            # Can not be distinguished in the final level.

            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Gases",
                components=var_sectors,
                append=True,
            )

            df_final_energy.filter(
                variable=["Final Energy|Industry excl Non-Energy Use|Gases"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

        elif c == "hydrogen":
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Hydrogen",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry excl Non-Energy Use|Hydrogen"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

        elif c == "liquids":

            # All liquids
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Liquids",
                components=var_sectors,
                append=True,
            )

            # Only bios (ethanol, methanol ?)

            filter_vars = [
                v
                for v in aux2_df["variable"].values
                if (("ethanol" in v) & ("methanol" not in v))
            ]
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Liquids|Biomass",
                components=filter_vars,
                append=True,
            )

            # Fossils

            filter_vars = [
                v
                for v in aux2_df["variable"].values
                if (("fueloil" in v) | ("lightoil" in v))
            ]

            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Liquids|Oil",
                components=filter_vars,
                append=True,
            )

            # Other

            filter_vars = [v for v in aux2_df["variable"].values if (("methanol" in v))]

            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Liquids|Coal",
                components=filter_vars,
                append=True,
            )

            df_final_energy.filter(
                variable=[
                    "Final Energy|Industry excl Non-Energy Use|Liquids",
                    "Final Energy|Industry excl Non-Energy Use|Liquids|Oil",
                    "Final Energy|Industry excl Non-Energy Use|Liquids|Biomass",
                    "Final Energy|Industry excl Non-Energy Use|Liquids|Coal",
                ],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "solids":

            # All
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Solids",
                components=var_sectors,
                append=True,
            )

            # Bio
            filter_vars = [v for v in aux2_df["variable"].values if ("biomass" in v)]
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Solids|Biomass",
                components=filter_vars,
                append=True,
            )

            # Fossil
            filter_vars = [
                v for v in aux2_df["variable"].values if ("biomass" not in v)
            ]
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Solids|Coal",
                components=filter_vars,
                append=True,
            )
            df_final_energy.filter(
                variable=[
                    "Final Energy|Industry excl Non-Energy Use|Solids",
                    "Final Energy|Industry excl Non-Energy Use|Solids|Biomass",
                    "Final Energy|Industry excl Non-Energy Use|Solids|Coal",
                ],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "heat":
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Heat",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry excl Non-Energy Use|Heat"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "other":
            df_final_energy.aggregate(
                "Final Energy|Industry excl Non-Energy Use|Other",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry excl Non-Energy Use|Other"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

    # FINAL ENERGY BY FUELS (Including Non-Energy Use)
    print("Final Energy by fuels including non-energy use is being printed.")
    commodities = [
        "electr",
        "gas",
        "hydrogen",
        "liquids",
        "solids",
        "heat",
        "all",
        "other",
    ]
    for c, r in product(commodities, nodes):
        df_final_energy = df.copy()
        df_final_energy.convert_unit("", to="GWa", factor=1, inplace=True)
        df_final_energy.filter(region=r, year=years, inplace=True)
        df_final_energy.filter(
            variable=[
                "in|final|*|cokeoven_steel|*",
                "in|final|bf_gas|*",
                "in|final|co_gas|*",
            ],
            keep=False,
            inplace=True,
        )

        if c == "other":
            df_final_energy.filter(
                variable=["out|useful|i_therm|solar_i|M1"], inplace=True
            )
        elif c == "electr":
            df_final_energy.filter(
                variable=[
                    "in|final|electr|*",
                    "in|secondary|electr|NH3_to_N_fertil|M1",
                    "in|secondary|electr|coal_NH3|M1",
                    "in|secondary|electr|electr_NH3|M1",
                    "in|secondary|electr|fueloil_NH3|M1",
                    "in|secondary|electr|gas_NH3|M1",
                    "in|secondary|electr|coal_NH3_ccs|M1",
                    "in|secondary|electr|fueloil_NH3_ccs|M1",
                    "in|secondary|electr|gas_NH3_ccs|M1",
                    "in|secondary|electr|biomass_NH3_ccs|M1",
                    "in|secondary|electr|biomass_NH3|M1",
                ],
                inplace=True,
            )

        elif c == "gas":
            df_final_energy.filter(
                variable=[
                    "in|final|gas|*",
                    "in|secondary|gas|gas_NH3|M1",
                    "in|secondary|gas|gas_NH3_ccs|M1",
                ],
                inplace=True,
            )

        elif c == "liquids":
            # Include gasoil and naphtha feedstock
            df_final_energy.filter(
                variable=[
                    "in|final|ethanol|*",
                    "in|final|fueloil|*",
                    "in|final|lightoil|*",
                    "in|final|methanol|*",
                    "in|final|vacuum_gasoil|*",
                    "in|final|naphtha|*",
                    "in|final|atm_gasoil|*",
                    "in|secondary|fueloil|fueloil_NH3|M1",
                    "in|secondary|fueloil|fueloil_NH3_ccs|M1",
                ],
                inplace=True,
            )
        elif c == "solids":
            df_final_energy.filter(
                variable=[
                    "in|final|biomass|*",
                    "in|final|coal|*",
                    "in|final|coke_iron|*",
                    "in|secondary|coal|coal_NH3|M1",
                    "in|secondary|coal|coal_NH3_ccs|M1",
                    "in|primary|biomass|biomass_NH3_ccs|M1",
                    "in|primary|biomass|biomass_NH3|M1",
                ],
                inplace=True,
            )

        elif c == "hydrogen":
            df_final_energy.filter(variable=["in|final|hydrogen|*"], inplace=True)
        elif c == "heat":
            df_final_energy.filter(variable=["in|final|d_heat|*"], inplace=True)
        elif c == "all":
            df_final_energy.filter(
                variable=[
                    "in|final|*",
                    "in|secondary|coal|coal_NH3|M1",
                    "in|secondary|electr|NH3_to_N_fertil|M1",
                    "in|secondary|electr|coal_NH3|M1",
                    "in|secondary|electr|electr_NH3|M1",
                    "in|secondary|electr|gas_NH3|M1",
                    "in|secondary|fueloil|fueloil_NH3|M1",
                    "in|secondary|gas|gas_NH3|M1",
                    "in|secondary|coal|coal_NH3_ccs|M1",
                    "in|secondary|electr|coal_NH3_ccs|M1",
                    "in|secondary|electr|gas_NH3_ccs|M1",
                    "in|secondary|fueloil|fueloil_NH3_ccs|M1",
                    "in|secondary|gas|gas_NH3_ccs|M1",
                    "in|primary|biomass|biomass_NH3|M1",
                    "in|seconday|electr|biomass_NH3|M1",
                    "in|primary|biomass|biomass_NH3_ccs|M1",
                    "in|secondary|electr|biomass_NH3_ccs|M1",
                    "out|useful|i_therm|solar_i|M1",
                ],
                inplace=True,
            )

        all_flows = df_final_energy.timeseries().reset_index()
        splitted_vars = [v.split("|") for v in all_flows.variable]
        aux1_df = pd.DataFrame(
            splitted_vars,
            columns=["flow_type", "level", "commodity", "technology", "mode"],
        )
        aux2_df = pd.concat(
            [all_flows.reset_index(drop=True), aux1_df.reset_index(drop=True)],
            axis=1,
        )

        # Include only the related industry sector variables

        var_sectors = list(
            filter(
                lambda v: re.match(
                    "_(aluminum|cement|fs|i|I|NH3|petro|steel)", v.split("|")[3]
                ),
                aux2_df["variable"].values,
            )
        )
        aux2_df = aux2_df[aux2_df["variable"].isin(var_sectors)]

        df_final_energy.filter(variable=var_sectors, inplace=True)

        # Aggregate

        if c == "other":
            df_final_energy.aggregate(
                "Final Energy|Industry|Other",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry|Other"], inplace=True
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "all":
            df_final_energy.aggregate(
                "Final Energy|Industry",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(variable=["Final Energy|Industry"], inplace=True)
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "electr":

            df_final_energy.aggregate(
                "Final Energy|Industry|Electricity",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry|Electricity"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "gas":

            # Can not distinguish by type Gases (natural gas, biomass, synthetic
            # fossil, efuel) (coal_gas), from biomass (gas_bio), natural gas
            # (gas_bal): All go into secondary level
            # Can not be distinguished in the final level.

            df_final_energy.aggregate(
                "Final Energy|Industry|Gases",
                components=var_sectors,
                append=True,
            )

            df_final_energy.filter(
                variable=["Final Energy|Industry|Gases"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

        elif c == "hydrogen":
            df_final_energy.aggregate(
                "Final Energy|Industry|Hydrogen",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry|Hydrogen"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

        elif c == "liquids":

            # All liquids
            df_final_energy.aggregate(
                "Final Energy|Industry|Liquids",
                components=var_sectors,
                append=True,
            )
            # Only bios (ethanol)

            filter_vars = [
                v
                for v in aux2_df["variable"].values
                if (("ethanol" in v) & ("methanol" not in v))
            ]
            df_final_energy.aggregate(
                "Final Energy|Industry|Liquids|Biomass",
                components=filter_vars,
                append=True,
            )

            # Oils

            filter_vars = [
                v
                for v in aux2_df["variable"].values
                if (
                    ("naphtha" in v)
                    | ("atm_gasoil" in v)
                    | ("vacuum_gasoil" in v)
                    | ("fueloil" in v)
                    | ("lightoil" in v)
                )
            ]

            df_final_energy.aggregate(
                "Final Energy|Industry|Liquids|Oil",
                components=filter_vars,
                append=True,
            )

            # Methanol

            filter_vars = [v for v in aux2_df["variable"].values if (("methanol" in v))]

            df_final_energy.aggregate(
                "Final Energy|Industry|Liquids|Coal",
                components=filter_vars,
                append=True,
            )

            df_final_energy.filter(
                variable=[
                    "Final Energy|Industry|Liquids",
                    "Final Energy|Industry|Liquids|Oil",
                    "Final Energy|Industry|Liquids|Biomass",
                    "Final Energy|Industry|Liquids|Coal",
                ],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "solids":

            # All
            df_final_energy.aggregate(
                "Final Energy|Industry|Solids",
                components=var_sectors,
                append=True,
            )

            # Bio
            filter_vars = [v for v in aux2_df["variable"].values if ("biomass" in v)]

            df_final_energy.aggregate(
                "Final Energy|Industry|Solids|Biomass",
                components=filter_vars,
                append=True,
            )

            # Fossil
            filter_vars = [
                v for v in aux2_df["variable"].values if ("biomass" not in v)
            ]

            df_final_energy.aggregate(
                "Final Energy|Industry|Solids|Coal",
                components=filter_vars,
                append=True,
            )
            df_final_energy.filter(
                variable=[
                    "Final Energy|Industry|Solids",
                    "Final Energy|Industry|Solids|Biomass",
                    "Final Energy|Industry|Solids|Coal",
                ],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)
        elif c == "heat":
            df_final_energy.aggregate(
                "Final Energy|Industry|Heat",
                components=var_sectors,
                append=True,
            )
            df_final_energy.filter(
                variable=["Final Energy|Industry|Heat"],
                inplace=True,
            )
            df_final_energy.convert_unit(
                "GWa", to="EJ/yr", factor=0.03154, inplace=True
            )
            df_final.append(df_final_energy, inplace=True)

    # FINAL ENERGY BY SECTOR AND FUEL
    # Feedstock not included

    sectors = [
        "aluminum",
        "steel",
        "cement",
        "petro",
        "Non-Ferrous Metals",
        "Non-Metallic Minerals",
        "Chemicals",
        "Other Sector",
    ]
    print("Final Energy by sector and fuel is being printed")
    for r, s in product(nodes, sectors):
        df_final_energy = df.copy()
        df_final_energy.convert_unit("", to="GWa", factor=1, inplace=True)
        exclude = [
            "in|final|atm_gasoil|steam_cracker_petro|*",
            "in|final|ethanol|ethanol_to_ethylene_petro|M1",
            "in|final|gas|gas_processing_petro|M1",
            "in|final|naphtha|steam_cracker_petro|*",
            "in|final|vacuum_gasoil|steam_cracker_petro|*",
            "in|final|*|cokeoven_steel|*",
            "in|final|bf_gas|*",
            "in|final|co_gas|*",
        ]

        df_final_energy.filter(region=r, year=years, inplace=True)
        df_final_energy.filter(
            variable=["in|final|*", "out|useful|i_therm|solar_i|M1"], inplace=True
        )
        df_final_energy.filter(variable=exclude, keep=False, inplace=True)

        # Decompose the pyam table into pandas data frame

        all_flows = df_final_energy.timeseries().reset_index()

        # Split the strings in the identified variables for further processing
        splitted_vars = [v.split("|") for v in all_flows.variable]

        # Create auxilary dataframes for processing
        aux1_df = pd.DataFrame(
            splitted_vars,
            columns=["flow_type", "level", "commodity", "technology", "mode"],
        )
        aux2_df = pd.concat(
            [all_flows.reset_index(drop=True), aux1_df.reset_index(drop=True)],
            axis=1,
        )

        # To be able to report the higher level sectors.
        if s == "Non-Ferrous Metals":
            tec = [t for t in aux2_df["technology"].values if "aluminum" in t]
            aux2_df = aux2_df[aux2_df["technology"].isin(tec)]
        elif s == "Non-Metallic Minerals":
            tec = [t for t in aux2_df["technology"].values if "cement" in t]
            aux2_df = aux2_df[aux2_df["technology"].isin(tec)]
        elif s == "Chemicals":
            tec = [
                t
                for t in aux2_df["technology"].values
                if (("petro" in t) | ("NH3" in t))
            ]
            aux2_df = aux2_df[aux2_df["technology"].isin(tec)]
        elif s == "Other Sector":
            tec = list(
                filter(lambda t: re.match("_[iI]$", t), aux2_df["technology"].values)
            )
            aux2_df = aux2_df[aux2_df["technology"].isin(tec)]
        else:
            # Filter the technologies only for the certain industry sector
            tec = [t for t in aux2_df["technology"].values if s in t]
            aux2_df = aux2_df[aux2_df["technology"].isin(tec)]

        s = NAME_MAP.get(s, s)

        # Lists to keep commodity, aggregate and variable names.

        commodity_list = []
        aggregate_list = []
        var_list = []

        # For the categoris below filter the required variable names,
        # create a new aggregate name

        commodity_list = [
            "electr",
            "gas",
            "hydrogen",
            "liquids",
            "liquid_bio",
            "liquid_fossil",
            "liquid_other",
            "solids",
            "solids_bio",
            "solids_fossil",
            "heat",
            "all",
        ]

        for c in commodity_list:
            if c == "electr":
                var = np.unique(
                    aux2_df.loc[aux2_df["commodity"] == "electr", "variable"].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Electricity"
                )
            elif c == "gas":
                var = np.unique(
                    aux2_df.loc[aux2_df["commodity"] == "gas", "variable"].values
                ).tolist()
                aggregate_name = f"Final Energy|Industry excl Non-Energy Use|{s}|Gases"
            elif c == "hydrogen":
                var = np.unique(
                    aux2_df.loc[aux2_df["commodity"] == "hydrogen", "variable"].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Hydrogen"
                )
            elif c == "liquids":
                var = np.unique(
                    aux2_df.loc[
                        (
                            (aux2_df["commodity"] == "fueloil")
                            | (aux2_df["commodity"] == "lightoil")
                            | (aux2_df["commodity"] == "methanol")
                            | (aux2_df["commodity"] == "ethanol")
                        ),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Liquids"
                )
            elif c == "liquid_bio":
                var = np.unique(
                    aux2_df.loc[
                        ((aux2_df["commodity"] == "ethanol")),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Liquids|Biomass"
                )
            elif c == "liquid_fossil":
                var = np.unique(
                    aux2_df.loc[
                        (
                            (aux2_df["commodity"] == "fueloil")
                            | (aux2_df["commodity"] == "lightoil")
                        ),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Liquids|Oil"
                )
            elif c == "liquid_other":
                var = np.unique(
                    aux2_df.loc[
                        ((aux2_df["commodity"] == "methanol")),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Liquids|Coal"
                )
            elif c == "solids":
                var = np.unique(
                    aux2_df.loc[
                        (
                            (aux2_df["commodity"] == "coal")
                            | (aux2_df["commodity"] == "biomass")
                            | (aux2_df["commodity"] == "coke_iron")
                        ),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = f"Final Energy|Industry excl Non-Energy Use|{s}|Solids"
            elif c == "solids_bio":
                var = np.unique(
                    aux2_df.loc[(aux2_df["commodity"] == "biomass"), "variable"].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Solids|Biomass"
                )
            elif c == "solids_fossil":
                var = np.unique(
                    aux2_df.loc[
                        (
                            (aux2_df["commodity"] == "coal")
                            | (aux2_df["commodity"] == "coke_iron")
                        ),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = (
                    f"Final Energy|Industry excl Non-Energy Use|{s}|Solids|Coals"
                )
            elif c == "heat":
                var = np.unique(
                    aux2_df.loc[(aux2_df["commodity"] == "d_heat"), "variable"].values
                ).tolist()
                aggregate_name = f"Final Energy|Industry excl Non-Energy Use|{s}|Heat"
            elif c == "all":
                var = aux2_df["variable"].tolist()
                aggregate_name = f"Final Energy|Industry excl Non-Energy Use|{s}"

            aggregate_list.append(aggregate_name)
            var_list.append(var)

        # Obtain the iamc format dataframe again

        aux2_df.drop(
            ["flow_type", "level", "commodity", "technology", "mode"],
            axis=1,
            inplace=True,
        )
        df_final_energy = pyam.IamDataFrame(data=aux2_df)

        # Aggregate the commodities in iamc object
        i = 0
        for c in commodity_list:
            df_final_energy.aggregate(
                aggregate_list[i], components=var_list[i], append=True
            )
            i = i + 1

        df_final_energy.filter(variable=aggregate_list, inplace=True)
        df_final_energy.convert_unit("GWa", to="EJ/yr", factor=0.03154, inplace=True)
        df_final.append(df_final_energy, inplace=True)

    # FINAL ENERGY NON-ENERGY USE BY SECTOR AND FUEL
    # Only in chemcials sector there is non-energy use
    # not in aluminum, steel, cement

    # For high value chemicals non-energy use is reported.

    # For ammonia, there is no seperation for non-energy vs. energy. Everything
    # is included here and the name of the variable is later changed.

    sectors = ["petro", "ammonia"]
    print("Final Energy non-energy use by sector and fuel is being printed")
    for r, s in product(nodes, sectors):
        df_final_energy = df.copy()
        df_final_energy.convert_unit("", to="GWa", factor=1, inplace=True)
        include = [
            "in|final|atm_gasoil|steam_cracker_petro|*",
            "in|final|ethanol|ethanol_to_ethylene_petro|M1",
            "in|final|gas|gas_processing_petro|M1",
            "in|final|naphtha|steam_cracker_petro|*",
            "in|final|vacuum_gasoil|steam_cracker_petro|*",
            "in|secondary|coal|coal_NH3|M1",
            "in|secondary|coal|coal_NH3_ccs|M1",
            "in|secondary|fueloil|fueloil_NH3|M1",
            "in|secondary|fueloil|fueloil_NH3_ccs|M1",
            "in|secondary|gas|gas_NH3|M1",
            "in|secondary|gas|gas_NH3_ccs|M1",
            "in|primary|biomass|biomass_NH3|M1",
            "in|primary|biomass|biomass_NH3_ccs|M1",
            "in|secondary|electr|NH3_to_N_fertil|M1",
            "in|secondary|electr|coal_NH3|M1",
            "in|secondary|electr|electr_NH3|M1",
            "in|secondary|electr|gas_NH3|M1",
            "in|secondary|electr|coal_NH3|M1",
            "in|secondary|electr|coal_NH3_ccs|M1",
            "in|secondary|electr|gas_NH3_ccs|M1",
            "in|seconday|electr|biomass_NH3|M1",
            "in|secondary|electr|biomass_NH3_ccs|M1",
            "in|secondary|electr|fueloil_NH3|M1",
            "in|secondary|electr|fueloil_NH3_ccs|M1",
        ]

        df_final_energy.filter(region=r, year=years, inplace=True)
        df_final_energy.filter(variable=include, inplace=True)

        # Decompose the pyam table into pandas data frame

        all_flows = df_final_energy.timeseries().reset_index()

        # Split the strings in the identified variables for further processing
        splitted_vars = [v.split("|") for v in all_flows.variable]

        # Create auxilary dataframes for processing
        aux1_df = pd.DataFrame(
            splitted_vars,
            columns=["flow_type", "level", "commodity", "technology", "mode"],
        )
        aux2_df = pd.concat(
            [all_flows.reset_index(drop=True), aux1_df.reset_index(drop=True)],
            axis=1,
        )

        # Filter the technologies only for the certain industry sector
        if s == "petro":
            tec = [t for t in aux2_df["technology"].values if (s in t)]
        elif s == "ammonia":
            tec = [t for t in aux2_df["technology"].values if ("NH3" in t)]

        aux2_df = aux2_df[aux2_df["technology"].isin(tec)]

        s = NAME_MAP.get(s, s)

        # Lists to keep commodity, aggregate and variable names.

        aggregate_list = []
        commodity_list = []
        var_list = []

        # For the categoris below filter the required variable names,
        # create a new aggregate name

        commodity_list = [
            "gas",
            "liquids",
            "liquid_bio",
            "liquid_oil",
            "all",
            "solids",
        ]

        for c in commodity_list:
            if c == "gas":
                var = np.unique(
                    aux2_df.loc[aux2_df["commodity"] == "gas", "variable"].values
                ).tolist()
                aggregate_name = f"Final Energy|Non-Energy Use|{s}|Gases"

            elif c == "liquids":
                var = np.unique(
                    aux2_df.loc[
                        (
                            (aux2_df["commodity"] == "naphtha")
                            | (aux2_df["commodity"] == "atm_gasoil")
                            | (aux2_df["commodity"] == "vacuum_gasoil")
                            | (aux2_df["commodity"] == "ethanol")
                            | (aux2_df["commodity"] == "fueloil")
                        ),
                        "variable",
                    ].values
                ).tolist()

                aggregate_name = f"Final Energy|Non-Energy Use|{s}|Liquids"
            elif c == "liquid_bio":
                var = np.unique(
                    aux2_df.loc[
                        ((aux2_df["commodity"] == "ethanol")),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = f"Final Energy|Non-Energy Use|{s}|Liquids|Biomass"
            elif c == "liquid_oil":
                var = np.unique(
                    aux2_df.loc[
                        (
                            (aux2_df["commodity"] == "atm_gasoil")
                            | (aux2_df["commodity"] == "naphtha")
                            | (aux2_df["commodity"] == "vacuum_gasoil")
                            | (aux2_df["commodity"] == "fueloil")
                        ),
                        "variable",
                    ].values
                ).tolist()

                aggregate_name = f"Final Energy|Non-Energy Use|{s}|Liquids|Oil"

            elif c == "solids":
                var = np.unique(
                    aux2_df.loc[
                        (
                            (aux2_df["commodity"] == "coal")
                            | (aux2_df["commodity"] == "biomass")
                        ),
                        "variable",
                    ].values
                ).tolist()
                aggregate_name = f"Final Energy|Non-Energy Use|{s}|Solids"
            elif c == "all":
                var = aux2_df["variable"].tolist()
                aggregate_name = f"Final Energy|Non-Energy Use|{s}"

            aggregate_list.append(aggregate_name)
            var_list.append(var)

        # Obtain the iamc format dataframe again

        aux2_df.drop(
            ["flow_type", "level", "commodity", "technology", "mode"],
            axis=1,
            inplace=True,
        )
        df_final_energy = pyam.IamDataFrame(data=aux2_df)

        # Aggregate the commodities in iamc object

        i = 0
        for c in commodity_list:
            if var_list[i]:
                df_final_energy.aggregate(
                    aggregate_list[i], components=var_list[i], append=True
                )

            i = i + 1

        df_final_energy.filter(variable=aggregate_list, inplace=True)
        df_final_energy.convert_unit("GWa", to="EJ/yr", factor=0.03154, inplace=True)
        df_final.append(df_final_energy, inplace=True)

    # EMISSIONS
    # If ammonia is used as feedstock the emissions are accounted under 'CO2_industry',
    # so as 'demand'. If used as fuel, under 'CO2_transformation'.
    # The CCS technologies deduct negative emissions from the overall CO2.
    # If CCS technologies are used,

    sectors = [
        "aluminum",
        "steel",
        "petro",
        "cement",
        "ammonia",
        "all",
        "Chemicals",
        "Other Sector",
    ]
    emission_type = [
        "CO2_industry",
        "CH4",
        "CO2",
        "NH3",
        "NOx",
        "CF4",
        "N2O",
        "BCA",
        "CO",
        "OCA",
    ]

    print("Emissions are being printed.")
    for typ, r, e in product(["demand", "process"], nodes, emission_type):
        df_emi = df.copy()
        df_emi.filter(region=r, year=years, inplace=True)
        #  CCS technologies for ammonia has both CO2 and CO2_industry
        #  at the same time.
        if e == "CO2_industry":
            emi_filter = [
                "emis|CO2|biomass_NH3_ccs|*",
                "emis|CO2|gas_NH3_ccs|*",
                "emis|CO2|coal_NH3_ccs|*",
                "emis|CO2|fueloil_NH3_ccs|*",
                "emis|CO2_industry|biomass_NH3_ccs|*",
                "emis|CO2_industry|gas_NH3_ccs|*",
                "emis|CO2_industry|coal_NH3_ccs|*",
                "emis|CO2_industry|fueloil_NH3_ccs|*",
                "emis|CO2_industry|biomass_NH3|*",
                "emis|CO2_industry|gas_NH3|*",
                "emis|CO2_industry|coal_NH3|*",
                "emis|CO2_industry|fueloil_NH3|*",
                "emis|CO2_industry|electr_NH3|*",
                "emis|CO2_industry|*",
            ]
            df_emi.filter(variable=emi_filter, inplace=True)
        else:
            emi_filter = ["emis|" + e + "|*"]
            exclude = [
                "emis|CO2|biomass_NH3_ccs|*",
                "emis|CO2|gas_NH3_ccs|*",
                "emis|CO2|coal_NH3_ccs|*",
                "emis|CO2|fueloil_NH3_ccs|*",
            ]
            df_emi.filter(variable=exclude, keep=False, inplace=True)
            df_emi.filter(variable=emi_filter, inplace=True)
        if (e == "CO2") | (e == "CO2_industry"):
            # From MtC to Mt CO2/yr
            df_emi.convert_unit("", to="Mt CO2/yr", factor=44 / 12, inplace=True)
        elif (e == "N2O") | (e == "CF4"):
            unit = "kt " + e + "/yr"
            df_emi.convert_unit("", to=unit, factor=1, inplace=True)
        else:
            e = NAME_MAP.get(e, e)
            # From kt/yr to Mt/yr
            unit = "Mt " + e + "/yr"
            df_emi.convert_unit("", to=unit, factor=0.001, inplace=True)

        all_emissions = df_emi.timeseries().reset_index()

        # Split the strings in the identified variables for further processing
        splitted_vars = [v.split("|") for v in all_emissions.variable]
        # Lists to later keep the variables and names to aggregate
        var_list = []
        aggregate_list = []

        # Collect the same emission type for each sector
        for s in sectors:
            # Create auxilary dataframes for processing
            aux1_df = pd.DataFrame(
                splitted_vars,
                columns=["emission", "type", "technology", "mode"],
            )
            aux2_df = pd.concat(
                [
                    all_emissions.reset_index(drop=True),
                    aux1_df.reset_index(drop=True),
                ],
                axis=1,
            )
            # Filter the technologies only for the sector

            if (typ == "process") & (s == "all") & (e != "CO2_industry"):
                tec = [
                    t
                    for t in aux2_df["technology"].values
                    if (
                        (
                            (
                                ("cement" in t)
                                | ("steel" in t)
                                | ("aluminum" in t)
                                | ("petro" in t)
                            )
                            & ("furnace" not in t)
                        )
                    )
                ]
            if (typ == "process") & (s != "all"):
                tec = [
                    t
                    for t in aux2_df["technology"].values
                    if ((s in t) & ("furnace" not in t) & ("NH3" not in t))
                ]

            if (typ == "demand") & (s == "Chemicals"):
                tec = [
                    t
                    for t in aux2_df["technology"].values
                    if (("NH3" in t) | (("petro" in t) & ("furnace" in t)))
                ]

            if (typ == "demand") & (s == "Other Sector") & (e != "CO2"):
                tec = [
                    t
                    for t in aux2_df["technology"].values
                    if (
                        (
                            ("biomass_i" in t)
                            | ("coal_i" in t)
                            | ("elec_i" in t)
                            | ("eth_i" in t)
                            | ("foil_i" in t)
                            | ("gas_i" in t)
                            | ("h2_i" in t)
                            | ("heat_i" in t)
                            | ("hp_el_i" in t)
                            | ("hp_gas_i" in t)
                            | ("loil_i" in t)
                            | ("meth_i" in t)
                            | ("sp_coal_I" in t)
                            | ("sp_el_I" in t)
                            | ("sp_eth_I" in t)
                            | ("sp_liq_I" in t)
                            | ("sp_meth_I" in t)
                            | ("h2_fc_I" in t)
                        )
                        & (
                            ("eth_ic_trp" not in t)
                            & ("meth_ic_trp" not in t)
                            & ("coal_imp" not in t)
                            & ("foil_imp" not in t)
                            & ("gas_imp" not in t)
                            & ("elec_imp" not in t)
                            & ("eth_imp" not in t)
                            & ("meth_imp" not in t)
                            & ("loil_imp" not in t)
                        )
                    )
                ]

            if (typ == "demand") & (s == "all") & (e != "CO2"):
                tec = [
                    t
                    for t in aux2_df["technology"].values
                    if (
                        (
                            (
                                ("cement" in t)
                                | ("steel" in t)
                                | ("aluminum" in t)
                                | ("petro" in t)
                            )
                            & ("furnace" in t)
                        )
                        | (
                            ("biomass_i" in t)
                            | ("coal_i" in t)
                            | ("elec_i" in t)
                            | ("eth_i" in t)
                            | ("foil_i" in t)
                            | ("gas_i" in t)
                            | ("h2_i" in t)
                            | ("heat_i" in t)
                            | ("hp_el_i" in t)
                            | ("hp_gas_i" in t)
                            | ("loil_i" in t)
                            | ("meth_i" in t)
                            | ("sp_coal_I" in t)
                            | ("sp_el_I" in t)
                            | ("sp_eth_I" in t)
                            | ("sp_liq_I" in t)
                            | ("sp_meth_I" in t)
                            | ("h2_fc_I" in t)
                            | ("DUMMY_limestone_supply_cement" in t)
                            | ("DUMMY_limestone_supply_steel" in t)
                            | ("eaf_steel" in t)
                            | ("DUMMY_coal_supply" in t)
                            | ("DUMMY_gas_supply" in t)
                            | ("NH3" in t)
                        )
                        & (
                            ("eth_ic_trp" not in t)
                            & ("meth_ic_trp" not in t)
                            & ("coal_imp" not in t)
                            & ("foil_imp" not in t)
                            & ("gas_imp" not in t)
                            & ("elec_imp" not in t)
                            & ("eth_imp" not in t)
                            & ("meth_imp" not in t)
                            & ("loil_imp" not in t)
                        )
                    )
                ]

            if (
                (typ == "demand")
                & (s != "all")
                & (s != "Other Sector")
                & (s != "Chemicals")
            ):

                if s == "steel":
                    # Furnaces are not used as heat source for iron&steel
                    # Dummy supply technologies help accounting the emissions
                    # from cokeoven_steel, bf_steel, dri_steel, eaf_steel,
                    # sinter_steel.

                    tec = [
                        t
                        for t in aux2_df["technology"].values
                        if (
                            ("DUMMY_coal_supply" in t)
                            | ("DUMMY_gas_supply" in t)
                            | ("DUMMY_limestone_supply_steel" in t)
                        )
                    ]

                elif s == "cement":
                    tec = [
                        t
                        for t in aux2_df["technology"].values
                        if (
                            ((s in t) & ("furnace" in t))
                            | ("DUMMY_limestone_supply_cement" in t)
                        )
                    ]
                elif s == "ammonia":
                    tec = [t for t in aux2_df["technology"].values if (("NH3" in t))]
                else:
                    tec = [
                        t
                        for t in aux2_df["technology"].values
                        if ((s in t) & ("furnace" in t))
                    ]
            # Adjust the sector names
            s = NAME_MAP.get(s, s)

            aux2_df = aux2_df[aux2_df["technology"].isin(tec)]
            # If there are no emission types for that setor skip
            if aux2_df.empty:
                continue

            # Add elements to lists for aggregation over emission type
            # for each sector
            var = aux2_df["variable"].values.tolist()
            var_list.append(var)

            # Aggregate names:
            if s == "all":
                if (typ == "demand") & (e != "CO2"):
                    if e != "CO2_industry":
                        aggregate_name = "Emissions|" + e + "|Energy|Demand|Industry"
                        aggregate_list.append(aggregate_name)
                    else:
                        aggregate_name = (
                            "Emissions|" + "CO2" + "|Energy|Demand|Industry"
                        )
                        aggregate_list.append(aggregate_name)
                if (typ == "process") & (e != "CO2_industry"):
                    aggregate_name = "Emissions|" + e + "|Industrial Processes"
                    aggregate_list.append(aggregate_name)
            else:
                if (typ == "demand") & (e != "CO2"):
                    if e != "CO2_industry":
                        aggregate_name = f"Emissions|{e}|Energy|Demand|Industry|{s}"
                        aggregate_list.append(aggregate_name)
                    else:
                        aggregate_name = f"Emissions|CO2|Energy|Demand|Industry|{s}"
                        aggregate_list.append(aggregate_name)
                if (typ == "process") & (e != "CO2_industry"):
                    aggregate_name = f"Emissions|{e}|Industrial Processes|{s}"
                    aggregate_list.append(aggregate_name)

        # To plot:   Obtain the iamc format dataframe again

        aux2_df = pd.concat(
            [
                all_emissions.reset_index(drop=True),
                aux1_df.reset_index(drop=True),
            ],
            axis=1,
        )
        aux2_df.drop(["emission", "type", "technology", "mode"], axis=1, inplace=True)
        df_emi = pyam.IamDataFrame(data=aux2_df)

        # Aggregation over emission type for each sector if there are elements
        # to aggregate
        for i in range(len(aggregate_list)):
            df_emi.aggregate(aggregate_list[i], components=var_list[i], append=True)

        if len(aggregate_list):
            df_emi.filter(variable=aggregate_list, inplace=True)
            df_final.append(df_emi, inplace=True)

            plot_emi_aggregates(df_emi, pp, r, e)

    # PLOTS
    #
    # # HVC Demand: See if this is correct ....
    #
    # for r in nodes:
    #
    #     fig, ax1 = plt.subplots(1, 1, figsize=(10, 10))
    #
    #     if r != "China*":
    #
    #         df_petro = df.copy()
    #         df_petro.filter(region=r, year=years, inplace=True)
    #         df_petro.filter(
    #             variable=[
    #                 "out|final_material|BTX|*",
    #                 "out|final_material|ethylene|*",
    #                 "out|final_material|propylene|*",
    #             ],
    #             inplace=True,
    #         )
    #
    #         # BTX production
    #         BTX_vars = [
    #             "out|final_material|BTX|steam_cracker_petro|atm_gasoil",
    #             "out|final_material|BTX|steam_cracker_petro|naphtha",
    #             "out|final_material|BTX|steam_cracker_petro|vacuum_gasoil",
    #         ]
    #         df_petro.aggregate("BTX production", components=BTX_vars, append=True)
    #
    #         # Propylene production
    #
    #         propylene_vars = [
    #             # "out|final_material|propylene|catalytic_cracking_ref|atm_gasoil",
    #             # "out|final_material|propylene|catalytic_cracking_ref|vacuum_gasoil",
    #             "out|final_material|propylene|steam_cracker_petro|atm_gasoil",
    #             "out|final_material|propylene|steam_cracker_petro|naphtha",
    #             "out|final_material|propylene|steam_cracker_petro|propane",
    #             "out|final_material|propylene|steam_cracker_petro|vacuum_gasoil",
    #         ]
    #
    #         df_petro.aggregate(
    #             "Propylene production", components=propylene_vars, append=True
    #         )
    #
    #         ethylene_vars = [
    #             "out|final_material|ethylene|ethanol_to_ethylene_petro|M1",
    #             "out|final_material|ethylene|steam_cracker_petro|atm_gasoil",
    #             "out|final_material|ethylene|steam_cracker_petro|ethane",
    #             "out|final_material|ethylene|steam_cracker_petro|naphtha",
    #             "out|final_material|ethylene|steam_cracker_petro|propane",
    #             "out|final_material|ethylene|steam_cracker_petro|vacuum_gasoil",
    #         ]
    #
    #         df_petro.aggregate(
    #             "Ethylene production", components=ethylene_vars, append=True
    #         )
    #
    #         if r != "World":
    #
    #             df_petro.filter(
    #                 variable=[
    #                     "BTX production",
    #                     "Propylene production",
    #                     "Ethylene production",
    #                     "out|final_material|BTX|import_petro|*",
    #                     "out|final_material|propylene|import_petro|*",
    #                     "out|final_material|ethylene|import_petro|*",
    #                 ],
    #                 inplace=True,
    #             )
    #         else:
    #             df_petro.filter(
    #                 variable=[
    #                     "BTX production",
    #                     "Propylene production",
    #                     "Ethylene production",
    #                 ],
    #                 inplace=True,
    #             )
    #
    #         df_petro.plot.stack(ax=ax1)
    #         ax1.legend(
    #             [
    #                 "BTX production",
    #                 "Ethylene production",
    #                 "Propylene production",
    #                 "import_BTX",
    #                 "import_ethylene",
    #                 "import_propylene",
    #             ]
    #         )
    #         ax1.set_title("HVC Production_" + r)
    #         ax1.set_xlabel("Years")
    #         ax1.set_ylabel("Mt")
    #
    #         plt.close()
    #         pp.savefig(fig)

    # # Refinery Products - already commented out
    #
    # for r in nodes:
    #
    #     fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 8))
    #     # fig.tight_layout(pad=15.0)
    #
    #     if r != "China*":
    #
    #         # Fuel oil
    #
    #         df_ref_fueloil = df.copy()
    #         df_ref_fueloil.filter(region=r, year=years, inplace=True)
    #
    #         if r == "World":
    #             df_ref_fueloil.filter(
    #                 variable=["out|secondary|fueloil|agg_ref|*"], inplace=True
    #             )
    #
    #         else:
    #             df_ref_fueloil.filter(
    #                 variable=[
    #                     "out|secondary|fueloil|agg_ref|*",
    #                     "out|secondary|fueloil|foil_imp|*",
    #                 ],
    #                 inplace=True,
    #             )
    #
    #         df_ref_fueloil.stack_plot(ax=ax1)
    #
    #         ax1.legend(
    #             [
    #                 "Atm gas oil",
    #                 "Atm resiude",
    #                 "Heavy fuel oil",
    #                 "Petroleum coke",
    #                 "Vacuum residue",
    #                 "import",
    #             ],
    #             bbox_to_anchor=(0.3, 1),
    #         )
    #         ax1.set_title("Fuel oil mix_" + r)
    #         ax1.set_xlabel("Year")
    #         ax1.set_ylabel("GWa")
    #
    #         # Light oil
    #
    #         df_ref_lightoil = df.copy()
    #         df_ref_lightoil.filter(region=r, year=years, inplace=True)
    #
    #         if r == "World":
    #             df_ref_lightoil.filter(
    #                 variable=["out|secondary|lightoil|agg_ref|*"], inplace=True
    #             )
    #
    #         else:
    #             df_ref_lightoil.filter(
    #                 variable=[
    #                     "out|secondary|lightoil|agg_ref|*",
    #                     "out|secondary|lightoil|loil_imp|*",
    #                 ],
    #                 inplace=True,
    #             )
    #
    #         # ,"out|secondary|lightoil|loil_imp|*"
    #         df_ref_lightoil.stack_plot(ax=ax2)
    #         # df_final.append(df_ref_lightoil, inplace = True)
    #         ax2.legend(
    #             [
    #                 "Diesel",
    #                 "Ethane",
    #                 "Gasoline",
    #                 "Kerosene",
    #                 "Light fuel oil",
    #                 "Naphtha",
    #                 "Refinery gas",
    #                 "import",
    #             ],
    #             bbox_to_anchor=(1, 1),
    #         )
    #         ax2.set_title("Light oil mix_" + r)
    #         ax2.set_xlabel("Year")
    #         ax2.set_ylabel("GWa")
    #
    #         plt.close()
    #         pp.savefig(fig)
    #
    # # Oil production World - Already commented out
    #
    # fig, ax1 = plt.subplots(1, 1, figsize=(8, 8))
    #
    # df_all_oil = df.copy()
    # df_all_oil.filter(region="World", year=years, inplace=True)
    # df_all_oil.filter(
    #     variable=[
    #         "out|secondary|fueloil|agg_ref|*",
    #         "out|secondary|lightoil|agg_ref|*",
    #     ],
    #     inplace=True,
    # )
    # df_all_oil.stack_plot(ax=ax1)
    #
    # ax1.legend(
    #     [
    #         "Atmg_gasoil",
    #         "atm_residue",
    #         "heavy_foil",
    #         "pet_coke",
    #         "vaccum_residue",
    #         "diesel",
    #         "ethane",
    #         "gasoline",
    #         "kerosne",
    #         "light_foil",
    #         "naphtha",
    #         "refinery_gas_a",
    #         "refinery_gas_b",
    #     ],
    #     bbox_to_anchor=(1, 1),
    # )
    # ax1.set_title("Oil production" + r)
    # ax1.set_xlabel("Year")
    # ax1.set_ylabel("GWa")
    #
    # plt.close()
    # pp.savefig(fig)

    # Final Energy by all fuels: See if this plot is correct..

    # Select the sectors
    # sectors = ["aluminum", "steel", "cement", "petro"]
    #
    # for r in nodes:
    #
    #     # For each region create a figure
    #
    #     fig, axs = plt.subplots(nrows=2, ncols=2)
    #     fig.set_size_inches(20, 20)
    #     fig.subplots_adjust(wspace=0.2)
    #     fig.subplots_adjust(hspace=0.5)
    #     fig.tight_layout(pad=20.0)
    #
    #     if r != "China*":
    #
    #         # Specify the position of each sector in the graph
    #
    #         cnt = 1
    #
    #         for s in sectors:
    #
    #             if cnt == 1:
    #                 x_cor = 0
    #                 y_cor = 0
    #
    #             if cnt == 2:
    #                 x_cor = 0
    #                 y_cor = 1
    #
    #             if cnt == 3:
    #                 x_cor = 1
    #                 y_cor = 0
    #
    #             if cnt == 4:
    #                 x_cor = 1
    #                 y_cor = 1
    #
    #             df_final_energy = df.copy()
    #             df_final_energy.convert_unit("", to="GWa", factor=1, inplace=True)
    #             df_final_energy.filter(region=r, year=years, inplace=True)
    #             df_final_energy.filter(variable=["in|final|*"], inplace=True)
    #             df_final_energy.filter(
    #                 variable=["in|final|*|cokeoven_steel|*"], keep=False, inplace=True
    #             )
    #
    #             if s == "petro":
    #                 # Exclude the feedstock ethanol and natural gas
    #                 df_final_energy.filter(
    #                     variable=[
    #                         "in|final|ethanol|ethanol_to_ethylene_petro|M1",
    #                         "in|final|gas|gas_processing_petro|M1",
    #                         "in|final|atm_gasoil|steam_cracker_petro|atm_gasoil",
    #                         "in|final|vacuum_gasoil|steam_cracker_petro|vacuum_gasoil",
    #                         "in|final|naphtha|steam_cracker_petro|naphtha",
    #                     ],
    #                     keep=False,
    #                     inplace=True,
    #                 )
    #
    #             all_flows = df_final_energy.timeseries().reset_index()
    #
    #             # Split the strings in the identified variables for further processing
    #             splitted_vars = [v.split("|") for v in all_flows.variable]
    #
    #             # Create auxilary dataframes for processing
    #             aux1_df = pd.DataFrame(
    #                 splitted_vars,
    #                 columns=["flow_type", "level", "commodity", "technology", "mode"],
    #             )
    #             aux2_df = pd.concat(
    #                 [
    #                     all_flows.reset_index(drop=True),
    #                     aux1_df.reset_index(drop=True),
    #                 ],
    #                 axis=1,
    #             )
    #
    #             # Filter the technologies only for the certain material
    #             tec = [t for t in aux2_df["technology"].values if s in t]
    #             aux2_df = aux2_df[aux2_df["technology"].isin(tec)]
    #
    #             # Lists to keep commodity, aggregate and variable.
    #
    #             aggregate_list = []
    #             commodity_list = []
    #             var_list = []
    #
    #             # For each commodity collect the variable name, create an aggregate
    #             # name
    #             s = NAME_MAP.get(s, s)
    #             for c in np.unique(aux2_df["commodity"].values):
    #                 var = np.unique(
    #                     aux2_df.loc[aux2_df["commodity"] == c, "variable"].values
    #                 ).tolist()
    #                 aggregate_name = "Final Energy|" + s + "|" + c
    #
    #                 aggregate_list.append(aggregate_name)
    #                 commodity_list.append(c)
    #                 var_list.append(var)
    #
    #             # Obtain the iamc format dataframe again
    #
    #             aux2_df.drop(
    #                 ["flow_type", "level", "commodity", "technology", "mode"],
    #                 axis=1,
    #                 inplace=True,
    #             )
    #             df_final_energy = pyam.IamDataFrame(data=aux2_df)
    #
    #             # Aggregate the commodities in iamc object
    #
    #             i = 0
    #             for c in commodity_list:
    #                 df_final_energy.aggregate(
    #                     aggregate_list[i], components=var_list[i], append=True
    #                 )
    #                 i = i + 1
    #
    #             df_final_energy.convert_unit(
    #                 "GWa", to="EJ/yr", factor=0.03154, inplace=True
    #             )
    #             df_final_energy.filter(variable=aggregate_list).plot.stack(
    #                 ax=axs[x_cor, y_cor]
    #             )
    #             axs[x_cor, y_cor].set_ylabel("EJ/yr")
    #             axs[x_cor, y_cor].set_title("Final Energy_" + s + "_" + r)
    #             cnt = cnt + 1
    #
    #     plt.close()
    #     pp.savefig(fig)

    # # Scrap Release: Buildings, Other and Power Sector
    # # TODO: Make the code better
    # # NEEDS TO BE CHECKED IF IT IS WORKING........
    # print('Scrap generated by sector')
    # materials = ["aluminum","steel","cement"]
    #
    # for r in nodes:
    #     print(r)
    #
    #     for m in materials:
    #         print(m)
    #
    #         df_scrap_by_sector = df.copy()
    #         df_scrap_by_sector.filter(region=r, year=years, inplace=True)
    #
    #         if m != 'cement':
    #
    #             filt_buildings = 'out|end_of_life|' + m + '|demolition_build|M1'
    #             print(filt_buildings)
    #             filt_other = 'out|end_of_life|' + m + '|other_EOL_' + m + '|M1'
    #             print(filt_other)
    #             filt_power = ['out|end_of_life|' + m + '|other_EOL_' + m + '|M1',
    #                           'out|end_of_life|' + m + '|demolition_build|M1',
    #                           'in|end_of_life|' + m + '|total_EOL_' + m + '|M1']
    #             print(filt_power)
    #
    #             m = NAME_MAP.get(m, m)
    #             var_name_buildings = 'Total Scrap|Residential and Commercial|' + m
    #             var_name_other = 'Total Scrap|Other|' + m
    #             var_name_power = 'Total Scrap|Power Sector|' + m
    #
    #             df_scrap_by_sector.aggregate(var_name_other,\
    #             components=[filt_other],append=True)
    #
    #             df_scrap_by_sector.aggregate(var_name_buildings,\
    #             components=[filt_buildings],append=True)
    #
    #             df_scrap_by_sector.subtract('in|end_of_life|' + m + '|total_EOL_'/
    #             + m + '|M1', ['out|end_of_life|' + m + '|demolition_build|M1',
    #             'out|end_of_life|' + m + '|other_EOL_' + m + '|M1'],
    #             var_name_power,axis='variable', append = True)
    #
    #             df_scrap_by_sector.filter(variable=[var_name_buildings,
    #                                                 var_name_other, var_name_power],
    #                                                                 inplace=True)
    #
    #             df_scrap_by_sector["unit"] = "Mt/yr"
    #             df_final.append(df_scrap_by_sector, inplace=True)
    #
    #         else:
    #             filt_buildings = 'out|end_of_life|' + m + '|demolition_build|M1'
    #             print(filt_buildings)
    #             filt_power = ['out|end_of_life|' + m + '|other_EOL_' + m + '|M1',
    #                           'out|end_of_life|' + m + '|demolition_build|M1',
    #                           'in|end_of_life|' + m + '|total_EOL_' + m + '|M1']
    #             print(filt_power)
    #             m = NAME_MAP.get(m, m)
    #             var_name_buildings = 'Total Scrap|Residential and Commercial|' + m
    #             print(var_name_buildings)
    #             var_name_power = 'Total Scrap|Power Sector|' + m
    #             df_scrap_by_sector.aggregate(var_name_buildings,\
    #             components=[filt_buildings],append=True)
    #
    #             df_scrap_by_sector.subtract('in|end_of_life|' + m + '|total_EOL_'/
    #             + m + '|M1', 'out|end_of_life|' + m + '|demolition_build|M1',
    #             var_name_power, axis = 'variable', append = True)
    #
    #             df_scrap_by_sector.filter(variable=[var_name_buildings,
    #                                             var_name_power],inplace=True)
    #
    #             df_scrap_by_sector["unit"] = "Mt/yr"
    #             df_final.append(df_scrap_by_sector, inplace=True)

    # PRICE
    #
    # df_final = df_final.timeseries().reset_index()
    #
    # commodity_type = [
    #     "Non-Ferrous Metals|Aluminium",
    #     "Non-Ferrous Metals|Aluminium|New Scrap",
    #     "Non-Ferrous Metals|Aluminium|Old Scrap",
    #     "Non-Ferrous Metals|Bauxite",
    #     "Non-Ferrous Metals|Alumina",
    #     "Steel|Iron Ore",
    #     "Steel|Pig Iron",
    #     "Steel|New Scrap",
    #     "Steel|Old Scrap",
    #     "Steel",
    #     "Non-Metallic Minerals|Cement",
    #     "Non-Metallic Minerals|Limestone",
    #     "Non-Metallic Minerals|Clinker Cement",
    #     "Chemicals|High Value Chemicals",
    # ]
    #
    # for c in commodity_type:
    #     prices = scenario.var("PRICE_COMMODITY")
    #     # Used for calculation of average prices for scraps
    #     output = scenario.par(
    #         "output",
    #         filters={
    #             "technology": ["scrap_recovery_aluminum", "scrap_recovery_steel"],
    #         },
    #     )
    #     # Differs per sector what to report so more flexible with conditions.
    #     # Store the relevant variables in prices_all
    #
    #     # ALUMINUM
    #     if c == "Non-Ferrous Metals|Bauxite":
    #         continue
    #     if c == "Non-Ferrous Metals|Alumina":
    #         prices_all = prices[
    #             (prices["level"] == "secondary_material")
    #             & (prices["commodity"] == "aluminum")
    #         ]
    #     if c == "Non-Ferrous Metals|Aluminium":
    #         prices_all = prices[
    #             (prices["level"] == "final_material")
    #             & (prices["commodity"] == "aluminum")
    #         ]
    #     if c == "Non-Ferrous Metals|Aluminium|New Scrap":
    #         prices_all = prices[
    #             (prices["level"] == "new_scrap") & (prices["commodity"] == "aluminum")
    #         ]
    #
    #     # IRON AND STEEL
    #     if c == "Steel|Iron Ore":
    #         prices_all = prices[(prices["commodity"] == "ore_iron")]
    #     if c == "Steel|Pig Iron":
    #         prices_all = prices[(prices["commodity"] == "pig_iron")]
    #     if c == "Steel":
    #         prices_all = prices[
    #             (prices["commodity"] == "steel")
    #             & (prices["level"] == "final_material")
    #         ]
    #     if c == "Steel|New Scrap":
    #         prices_all = prices[
    #             (prices["commodity"] == "steel") & (prices["level"] == "new_scrap")
    #         ]
    #     # OLD SCRAP (For aluminum and steel)
    #
    #     if (c == "Steel|Old Scrap") | (c == "Non-Ferrous Metals|Aluminium|Old Scrap"):
    #
    #         prices = prices[
    #             (
    #                 (prices["level"] == "old_scrap_1")
    #                 | (prices["level"] == "old_scrap_2")
    #                 | (prices["level"] == "old_scrap_3")
    #             )
    #         ]
    #
    #         if c == "Non-Ferrous Metals|Aluminium|Old Scrap":
    #             output = output[output["technology"] == "scrap_recovery_aluminum"]
    #             prices = prices[(prices["commodity"] == "aluminum")]
    #         if c == "Steel|Old Scrap":
    #             output = output[output["technology"] == "scrap_recovery_steel"]
    #             prices = prices[(prices["commodity"] == "steel")]
    #
    #         prices.loc[prices["level"] == "old_scrap_1", "weight"] = output.loc[
    #             output["level"] == "old_scrap_1", "value"
    #         ].values[0]
    #
    #         prices.loc[prices["level"] == "old_scrap_2", "weight"] = output.loc[
    #             output["level"] == "old_scrap_2", "value"
    #         ].values[0]
    #
    #         prices.loc[prices["level"] == "old_scrap_3", "weight"] = output.loc[
    #             output["level"] == "old_scrap_3", "value"
    #         ].values[0]
    #
    #         prices_all = pd.DataFrame(columns=["node", "commodity", "year"])
    #         prices_new = pd.DataFrame(columns=["node", "commodity", "year"])
    #
    #         for reg in output["node_loc"].unique():
    #             for yr in output["year_act"].unique():
    #                 prices_temp = (
    #                      prices.groupby(["node", "year"]).get_group((reg, yr))
    #                 )
    #                 rate = prices_temp["weight"].values.tolist()
    #                 amount = prices_temp["lvl"].values.tolist()
    #                 weighted_avg = np.average(amount, weights=rate)
    #                 prices_new = pd.DataFrame(
    #                     {
    #                         "node": reg,
    #                         "year": yr,
    #                         "commodity": c,
    #                         "lvl": weighted_avg,
    #                      },
    #                     index=[0],
    #                 )
    #                 prices_all = pd.concat([prices_all, prices_new])
    #     # CEMENT
    #
    #     if c == "Non-Metallic Minerals|Limestone":
    #         prices_all = prices[(prices["commodity"] == "limestone_cement")]
    #     if c == "Non-Metallic Minerals|Clinker Cement":
    #         prices_all = prices[(prices["commodity"] == "clinker_cement")]
    #     if c == "Non-Metallic Minerals|Cement":
    #         prices_all = prices[
    #             (prices["commodity"] == "cement") & (prices["level"] == "demand")
    #         ]
    #     # Petro-Chemicals
    #
    #     if c == "Chemicals|High Value Chemicals":
    #         prices = prices[
    #             (prices["commodity"] == "ethylene")
    #             | (prices["commodity"] == "propylene")
    #             | (prices["commodity"] == "BTX")
    #         ]
    #         prices_all = prices.groupby(by=["year", "node"]).mean().reset_index()
    #
    #     # Convert all to IAMC format.
    #     for r in prices_all["node"].unique():
    #         if (r == "R11_GLB") | (r == "R12_GLB"):
    #             continue
    #         df_price = pd.DataFrame(
    #             {
    #                 "model": model_name,
    #                 "scenario": scenario_name,
    #                 "unit": "2010USD/Mt",
    #             },
    #             index=[0],
    #         )
    #
    #         for y in prices_all["year"].unique():
    #             df_price["region"] = r
    #             df_price["variable"] = "Price|" + c
    #             x = prices_all.loc[
    #                 ((prices_all["node"] == r) & (prices_all["year"] == y)), "lvl"
    #             ]
    #             if not x.empty:
    #                 value = x.values[0] * 1.10774
    #             else:
    #                 value = 0
    #             df_price[y] = value
    #
    #             df.price = df_price.columns.astype(str)
    #
    #         df_final = pd.concat([df_final, df_price])

    # Material Demand - comment out if no power sector
    # Power Sector

    # input_cap_new = scenario.par("input_cap_new")
    # input_cap_new.drop(
    #     ["node_origin", "level", "time_origin", "unit"], axis=1, inplace=True
    # )
    # input_cap_new.rename(
    #     columns={
    #         "value": "material_intensity",
    #         "node_loc": "region",
    #         "year_vtg": "year",
    #     },
    #     inplace=True,
    # )
    #
    # cap_new = scenario.var("CAP_NEW")
    # cap_new.drop(["mrg"], axis=1, inplace=True)
    # cap_new.rename(
    #     columns={
    #         "lvl": "installed_capacity",
    #         "node_loc": "region",
    #         "year_vtg": "year"
    #     },
    #     inplace=True,
    # )
    #
    # merged_df = pd.merge(cap_new, input_cap_new)
    # merged_df["Material Need"] = (
    #     merged_df["installed_capacity"] * merged_df["material_intensity"]
    # )
    # merged_df = merged_df[merged_df["year"] >= min(years)]
    #
    # final_material_needs = (
    #     merged_df.groupby(["commodity", "region", "year"])
    #     .sum()
    #     .drop(["installed_capacity", "material_intensity"], axis=1)
    # )
    #
    # final_material_needs = final_material_needs.reset_index(["year"])
    # final_material_needs = pd.pivot_table(
    #     final_material_needs,
    #     values="Material Need",
    #     columns="year",
    #     index=["commodity", "region"],
    # ).reset_index(["commodity", "region"])
    #
    # final_material_needs_global = (
    #     final_material_needs.groupby("commodity").sum().reset_index()
    # )
    # final_material_needs_global["region"] = "World"
    #
    # material_needs_all = pd.concat(
    #     [final_material_needs, final_material_needs_global], ignore_index=True
    # )
    #
    # material_needs_all["scenario"] = scenario_name
    # material_needs_all["model"] = model_name
    # material_needs_all["unit"] = "Mt/yr"
    # material_needs_all["commodity"] = material_needs_all.apply(
    #     lambda x: NAME_MAP.get(x["commodity"], x["commodity"]), axis=1
    # )
    # material_needs_all = material_needs_all.assign(
    #     variable=lambda x: "Material Demand|Power Sector|" + x["commodity"]
    # )
    #
    # material_needs_all = material_needs_all.drop(["commodity"], axis=1)
    # df.price = df_price.columns.astype(str)
    #
    # print("This is the final_material_needs")
    # print(material_needs_all)
    #
    # df_final = pd.concat([df_final, material_needs_all])

    # Plotting is complete
    pp.close()

    # - Convert from pyam.IamDataFrame to pandas.DataFrame.
    # - Apply the replacements in NAME_MAP1.
    df = df_final.as_pandas().replace(NAME_MAP1)

    # Store
    path_new = directory.joinpath(f"New_Reporting_{model_name}_{scenario_name}.xlsx")
    df.to_excel(path_new, sheet_name="data")
    log.info(f"Wrote output to {path_new}")

    scenario.check_out(timeseries_only=True)
    log.info(f"Store timeseries on scenario:\n\n{df.head()}")
    scenario.add_timeseries(df)

    # NB(PNK) Appears to be old code that handled buildings reporting output
    # df_resid = pd.read_csv(path_resid)
    # df_resid["Model"] = model_name
    # df_resid["Scenario"] = scenario_name
    # df_comm = pd.read_csv(path_comm)
    # df_comm["Model"] = model_name
    # df_comm["Scenario"] = scenario_name
    # scenario.add_timeseries(df_resid)
    # scenario.add_timeseries(df_comm)

    log.info("…finished.")
    scenario.commit("material.report.report()")


def callback(rep: message_ix.Reporter, context: Context) -> None:
    """:meth:`.prepare_reporter` callback for MESSAGEix-Materials.

    - "materials all": invokes :func:`report`.
    """
    rep.add(
        "materials all",
        report,
        "scenario",
        "message::default",
        "y::model",
        "n",
        "config",
    )
