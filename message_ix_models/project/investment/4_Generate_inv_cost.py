"""Investment Cost Generation with Cost of Capital Decomposition.

Authors: Shuting Fan, Yiyi Ju

This module generates investment cost files with cost of capital (CoC) decomposition
using WACC projections and original investment cost data.
"""

import logging

import pandas as pd

from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


def main():  # noqa: C901
    """Generate investment cost files with CoC decomposition for all WACC scenarios.

    Returns
    -------
    None
    """
    # === Config ===
    baseline_year = 2020
    A_default = 0.10
    inv_cost_filename = "inv_cost_ori.csv"  # TODO: read from parent scenario
    category_map_override = None

    def gene_coc(
        wacc_csv_path: str,
        out_filename: str,
        baseline_year: int = 2020,
        A_default: float = 0.10,
        inv_cost_filename: str = "inv_cost_ori.csv",
        category_map_override: dict | None = None,
    ) -> None:
        """
        Generate investment cost file with CoC decomposition.

        Parameters
        ----------
        wacc_csv_path : Path to WACC projection file
        out_filename : Output file name
        baseline_year : Only process investments from this year onwards
        A_default : Default financing cost share (A) when WACC is missing
        inv_cost_filename : Original investment cost file name
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
        inv_cost_dir = package_data_path("investment")
        inv_cost_path = inv_cost_dir / inv_cost_filename
        inv_cost_ori = pd.read_csv(inv_cost_path)
        inv_cost = inv_cost_ori.loc[inv_cost_ori["year_vtg"] >= baseline_year].copy()

        # Load WACC data
        log.info("Loading WACC data...")
        wacc_dir = package_data_path("investment")
        wacc_path = wacc_dir / wacc_csv_path
        wacc = pd.read_csv(wacc_path)
        # Since we're using individual SSP+scenario files, no filtering needed

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
        output_dir = package_data_path("investment")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / out_filename
        out.to_csv(output_path, index=False)

    # Find all predicted_wacc_*.csv files in the package data directory
    wacc_dir = package_data_path("investment")
    wacc_files = list(wacc_dir.glob("predicted_wacc_*.csv"))

    log.info(f"Looking for WACC files in: {wacc_dir}")

    if not wacc_files:
        log.warning(f"No predicted_wacc_*.csv files found in {wacc_dir}")
        return None

    log.info(f"Found {len(wacc_files)} WACC files to process:")
    for wacc_file in wacc_files:
        log.info(f"  - {wacc_file.name}")

    # Process each WACC file
    for wacc_file in wacc_files:
        # Extract the base name
        base_name = wacc_file.stem
        # Create corresponding output filename
        out_filename = f"inv_cost_{base_name.replace('predicted_wacc_', '')}.csv"

        log.info(f"Processing {wacc_file.name} -> {out_filename}")

        try:
            gene_coc(
                wacc_csv_path=wacc_file.name,
                out_filename=out_filename,
                baseline_year=baseline_year,
                A_default=A_default,
                inv_cost_filename=inv_cost_filename,
                category_map_override=category_map_override,
            )
            log.info(f"Successfully generated {out_filename}")
        except Exception as e:
            log.error(f"Failed to process {wacc_file.name}: {e}")
            continue

    # Summary
    output_dir = package_data_path("investment")
    successful_files = list(output_dir.glob("inv_cost_*.csv"))
    log.info(f"Generated {len(successful_files)} investment cost files:")
    for inv_file in sorted(successful_files):
        log.info(f"  - {inv_file.name}")

    return None
