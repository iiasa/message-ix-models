"""Investment Cost Generation with Cost of Capital Decomposition.

Authors: Shuting Fan, Yiyi Ju

This module generates investment cost files with cost of capital (CoC) decomposition
using WACC projections and original investment cost data.
"""

import logging
from pathlib import Path

import pandas as pd
from message_ix import Scenario

from message_ix_models.util import private_data_path

log = logging.getLogger(__name__)


def main(context, scenario: Scenario) -> Scenario:
    """Generate investment cost file with CoC decomposition.

    Parameters
    ----------
    context
        Workflow context
    scenario : Scenario
        MESSAGE scenario (not used in this function)

    Returns
    -------
    Scenario
        The input scenario (unchanged)
    """
    # === Config ===
    ssp = "SSP2"
    wacc_scenario = "ccf"
    baseline_year = 2020
    A_default = 0.10
    wacc_csv_path = "predicted_wacc.csv"
    inv_cost_filename = "inv_cost_ori.csv"  # TODO: read from parent scenario
    out_filename = "inv_cost.csv"
    category_map_override = None

    def gene_coc(
        ssp: str = "SSP2",
        wacc_scenario: str = "ccf",
        baseline_year: int = 2020,
        A_default: float = 0.10,
        wacc_csv_path: str = "Predicted_WACC.csv",
        inv_cost_filename: str = "inv_cost_ori.csv",
        out_filename: str = "inv_cost.csv",
        category_map_override: dict | None = None,
    ) -> None:
        """
        Generate investment cost file (inv_cost.csv) with CoC decomposition.

        Parameters
        ----------
        ssp : SSP scenario (e.g., 'SSP2')
        wacc_scenario : WACC scenario (e.g., 'Scen0')
        baseline_year : Only process investments from this year onwards
        A_default : Default financing cost share (A) when WACC is missing
        wacc_csv_path : Path to WACC projection file
        inv_cost_filename : Original investment cost file name
        out_filename : Output file name
        category_map_override : Optional mapping to override technology-to-category
            mapping
        """

        def map_category(tech: str) -> str:
            """Map technology name to category."""
            if tech.startswith("solar_") or tech.startswith("csp_"):
                return "solar"
            elif tech.startswith("wind_"):
                return "wind"
            elif tech.startswith("bio_"):
                return "bio"
            elif tech.startswith("hydro_"):
                return "hydro"
            # Apply user override mapping
            if category_map_override:
                for prefix, cat in category_map_override.items():
                    if tech.startswith(prefix):
                        return cat
            return tech  # fallback to original technology name

        # Load original investment cost data
        log.info("Loading original investment cost data...")
        inv_cost_ori = pd.read_csv(private_data_path("investment", inv_cost_filename))
        inv_cost = inv_cost_ori.loc[inv_cost_ori["year_vtg"] >= baseline_year].copy()

        # Load and filter WACC data
        log.info("Loading and filtering WACC data...")
        current_dir = Path(__file__).parent
        wacc_path = current_dir / wacc_csv_path
        wacc = pd.read_csv(wacc_path)
        wacc = wacc[(wacc["Scenario"] == wacc_scenario) & (wacc["SSP"] == ssp)].copy()

        # Rename WACC columns for merging
        wacc = wacc.rename(
            columns={
                "Region": "node_loc",
                "Year": "year_vtg",
                "Tech": "category",
                "WACC": "A",
            }
        )

        # Map technology to category
        log.info("Mapping technologies to categories...")
        inv_cost = inv_cost.rename(columns={"technology": "technology_ori"})
        inv_cost["category"] = inv_cost["technology_ori"].apply(map_category)

        # Merge investment cost data with WACC data
        log.info("Merging investment cost data with WACC data...")
        inv_cost = inv_cost.merge(
            wacc[["node_loc", "year_vtg", "category", "A"]],
            on=["node_loc", "year_vtg", "category"],
            how="left",
        )

        # Fill missing WACC values with default
        inv_cost["A"] = inv_cost["A"].fillna(A_default)

        # First split into CoC and non-CoC parts
        log.info("Calculating CoC and non-CoC components...")
        inv_cost["coc_base"] = inv_cost["value"] * inv_cost["A"]
        inv_cost["non_coc_base"] = inv_cost["value"] - inv_cost["coc_base"]

        # Restore original technology column
        inv_cost["technology"] = inv_cost["technology_ori"]
        inv_cost = inv_cost.drop(
            columns=["technology_ori", "category"], errors="ignore"
        )

        # Sort data and calculate year-on-year growth rate of original value
        inv_cost = inv_cost.sort_values(["node_loc", "technology", "year_vtg"]).copy()

        inv_cost["value_growth"] = (
            inv_cost.groupby(["node_loc", "technology"])["value"]
            .transform(lambda x: x.div(x.shift(1)))
            .fillna(1.0)
        )

        inv_cost["non_coc_base0"] = inv_cost.groupby(["node_loc", "technology"])[
            "non_coc_base"
        ].transform("first")

        # Accumulate the product of growth to obtain the cumulative growth factor
        inv_cost["cum_growth"] = inv_cost.groupby(["node_loc", "technology"])[
            "value_growth"
        ].cumprod()
        # Adjust non-CoC part using the growth rate
        inv_cost["non_coc_new"] = inv_cost["non_coc_base0"] * inv_cost["cum_growth"]

        # Recalculate total value and CoC part using updated non-CoC and A
        inv_cost["value"] = inv_cost["non_coc_new"] / (1.0 - inv_cost["A"])
        inv_cost["coc_base"] = inv_cost["value"] * inv_cost["A"]

        # Save final output
        log.info("Saving investment cost file...")
        cols = [
            "node_loc",
            "technology",
            "year_vtg",
            "value",
            "unit",
            "coc_base",
            "non_coc_base",
        ]
        out = inv_cost[cols]
        output_path = current_dir / out_filename
        out.to_csv(output_path, index=False)

    # Call the gene_coc function
    gene_coc(
        ssp=ssp,
        wacc_scenario=wacc_scenario,
        baseline_year=baseline_year,
        A_default=A_default,
        wacc_csv_path=wacc_csv_path,
        inv_cost_filename=inv_cost_filename,
        out_filename=out_filename,
        category_map_override=category_map_override,
    )

    log.info("Investment cost generation completed.")

    return scenario
