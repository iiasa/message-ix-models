"""Utility functions for MESSAGEix-BMT (Buildings, Materials, Transport) integration."""

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import ScenarioInfo

log = logging.getLogger(__name__)


def _generate_vetting_csv(
    original_demand: pd.DataFrame,
    modified_demand: pd.DataFrame,
    output_path: str,
) -> None:
    """Generate a CSV file showing material demand subtraction details.

    Parameters
    ----------
    original_demand : pd.DataFrame
        Original demand data before subtraction
    modified_demand : pd.DataFrame
        Modified demand data after subtraction
    output_path : str
        Path where to save the vetting CSV file
    """
    # Reset index to work with columns
    orig = original_demand.reset_index()
    mod = modified_demand.reset_index()

    # Merge original and modified data
    vetting_data = orig.merge(
        mod,
        on=["node", "year", "commodity"],
        suffixes=("_original", "_modified"),
        how="outer",
    ).fillna(0)

    # Calculate subtraction amounts and percentages
    vetting_data["subtracted_amount"] = (
        vetting_data["value_original"] - vetting_data["value_modified"]
    )

    # Calculate percentage subtracted (avoid division by zero)
    vetting_data["subtraction_percentage"] = (
        vetting_data["subtracted_amount"]
        / vetting_data["value_original"].replace(0, 1)
        * 100
    )

    # Replace infinite values with 0 (when original was 0)
    vetting_data["subtraction_percentage"] = vetting_data[
        "subtraction_percentage"
    ].replace([float("inf"), -float("inf")], 0)

    # Round to reasonable precision
    vetting_data["subtraction_percentage"] = vetting_data[
        "subtraction_percentage"
    ].round(2)

    # Select and rename columns for clarity
    output_columns = [
        "node",
        "year",
        "commodity",
        "value_original",
        "value_modified",
        "subtracted_amount",
        "subtraction_percentage",
    ]

    vetting_data = vetting_data[output_columns].copy()
    vetting_data.columns = [
        "node",
        "year",
        "commodity",
        "original_demand",
        "modified_demand",
        "subtracted_amount",
        "subtraction_percentage",
    ]

    # # Filter out rows where no subtraction occurred
    # vetting_data = vetting_data[vetting_data["subtracted_amount"] > 0]

    # Sort by commodity, node, year for better readability
    vetting_data = vetting_data.sort_values(["commodity", "node", "year"])

    # Save to CSV
    vetting_data.to_csv(output_path, index=False)

    log.info(f"Vetting CSV saved to: {output_path}")

    # Log summary statistics
    if len(vetting_data) > 0:
        avg_pct = vetting_data["subtraction_percentage"].mean()
        max_pct = vetting_data["subtraction_percentage"].max()
        log.info(f"Average subtraction percentage: {avg_pct:.2f}%")
        log.info(f"Max subtraction percentage: {max_pct:.2f}%")


# Maybe it is better to have one function for each method?
def subtract_material_demand(
    scenario: "Scenario",
    info: "ScenarioInfo",
    sturm_r: pd.DataFrame,
    sturm_c: pd.DataFrame,
    method: str = "bm_subtraction",
    generate_vetting_csv: bool = True,
    vetting_output_path: str = "material_demand_subtraction_vetting.csv",
) -> pd.DataFrame:
    """Subtract inter-sector material demand from existing demands in scenario.

    This function provides different approaches for subtracting inter-sector material
    demand from the original material demand, due to BM (material inputs for residential
    and commercial building construction), PM (material inputs for power capacity), IM
    (material inputs for infrastructures), TM (material inputs for new vehicles) links.

    Parameters
    ----------
    scenario : message_ix.Scenario
        The scenario to modify
    info : ScenarioInfo
        Scenario information
    sturm_r : pd.DataFrame
        Residential STURM data
    sturm_c : pd.DataFrame
        Commercial STURM data
    method : str, optional
        Method to use for subtraction:
        - "bm_subtraction": default, substract entire trajectory
        - "im_subtraction": substract base year and rerun material demand projection
        - "pm_subtraction": to be determined (currently treated as additional demand)
        - "tm_subtraction": to be determined
    generate_vetting_csv : bool, optional
        Whether to generate a CSV file showing subtraction details (default: True)
    vetting_output_path : str, optional
        Path for the vetting CSV file (default:
        "material_demand_subtraction_vetting.csv")

    Returns
    -------
    pd.DataFrame
        Modified demand data with material demand subtracted
    """
    # Retrieve data once
    mat_demand = scenario.par("demand", {"level": "demand"})
    index_cols = ["node", "year", "commodity"]

    if method == "bm_subtraction":
        # Store original demand for vetting if requested
        original_demand = mat_demand.copy() if generate_vetting_csv else None

        # Subtract the building material demand trajectory from existing demands
        for rc, base_data, how in (
            ("resid", sturm_r, "right"),
            ("comm", sturm_c, "outer"),
        ):
            new_col = f"demand_{rc}_const"

            # - Drop columns.
            # - Rename "value" to e.g. "demand_resid_const".
            # - Extract MESSAGEix-Materials commodity name from STURM commodity name.
            # - Drop other rows.
            # - Set index.
            df = (
                base_data.drop(columns=["level", "time", "unit"])
                .rename(columns={"value": new_col})
                .assign(
                    commodity=lambda _df: _df.commodity.str.extract(
                        f"{rc}_mat_demand_(cement|steel|aluminum)", expand=False
                    )
                )
                .dropna(subset=["commodity"])
                .set_index(index_cols)
            )

            # Merge existing demands at level "demand".
            # - how="right": drop all rows in par("demand", …) with no match in `df`.
            # - how="outer": keep the union of rows in `mat_demand` (e.g. from sturm_r)
            #   and in `df` (from sturm_c); fill NA with zeroes.
            mat_demand = mat_demand.join(df, on=index_cols, how=how).fillna(0)

        # False if main() is being run for the second time on `scenario`
        first_pass = "construction_resid_build" not in info.set["technology"]

        # If not on the first pass, this modification is already performed; skip
        if first_pass:
            # - Compute new value = (existing value - STURM values), but no less than 0.
            # - Drop intermediate column.
            mat_demand = (
                mat_demand.eval(
                    "value = value - demand_comm_const - demand_resid_const"
                )
                .assign(value=lambda df: df["value"].clip(0))
                .drop(columns=["demand_comm_const", "demand_resid_const"])
            )

            # Generate vetting CSV if requested
            if generate_vetting_csv and original_demand is not None:
                _generate_vetting_csv(original_demand, mat_demand, vetting_output_path)

    elif method == "im_subtraction":
        # TODO: to be implemented
        log.warning("Method 'im_subtraction' not implemented yet, using bm_subtraction")
        return subtract_material_demand(
            scenario, info, sturm_r, sturm_c, "bm_subtraction"
        )

    elif method == "pm_subtraction":
        # TODO: Implement alternative method 2
        log.warning("Method 'pm_subtraction' not implemented yet, using bm_subtraction")
        return subtract_material_demand(
            scenario, info, sturm_r, sturm_c, "bm_subtraction"
        )

    elif method == "tm_subtraction":
        # TODO: Implement alternative method 3
        log.warning("Method 'tm_subtraction' not implemented yet, using bm_subtraction")
        return subtract_material_demand(
            scenario, info, sturm_r, sturm_c, "bm_subtraction"
        )

    else:
        raise ValueError(f"Unknown method: {method}")

    return mat_demand
