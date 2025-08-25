import os
import pandas as pd
from message_ix_models.util import package_data_path

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
    inv_cost_filename : Original investment cost file name (located in package_data_path('investment'))
    out_filename : Output file name (saved in package_data_path('investment'))
    category_map_override : Optional mapping to override technology-to-category mapping, e.g., {'nuclear_':'nuclear'}
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

    folder_path = package_data_path("investment")

    # Load original investment cost data
    inv_cost_ori = pd.read_csv(package_data_path("investment", inv_cost_filename))
    inv_cost = inv_cost_ori.loc[inv_cost_ori["year_vtg"] >= baseline_year].copy()

    # Load and filter WACC data
    wacc = pd.read_csv(package_data_path("investment", wacc_csv_path))
    wacc = wacc[(wacc["Scenario"] == wacc_scenario) & (wacc["SSP"] == ssp)].copy()

    # Rename WACC columns for merging
    wacc = wacc.rename(columns={
        "Region": "node_loc",
        "Year": "year_vtg",
        "Tech": "category",
        "WACC": "A"
    })

    # Map technology to category
    inv_cost = inv_cost.rename(columns={"technology": "technology_ori"})
    inv_cost["category"] = inv_cost["technology_ori"].apply(map_category)

    # Merge investment cost data with WACC data
    inv_cost = inv_cost.merge(
        wacc[["node_loc", "year_vtg", "category", "A"]],
        on=["node_loc", "year_vtg", "category"],
        how="left"
    )

    # Fill missing WACC values with default
    inv_cost["A"] = inv_cost["A"].fillna(A_default)

    # First split into CoC and non-CoC parts
    inv_cost["coc_base"] = inv_cost["value"] * inv_cost["A"]
    inv_cost["non_coc_base"] = inv_cost["value"] - inv_cost["coc_base"]

    # Restore original technology column
    inv_cost["technology"] = inv_cost["technology_ori"]
    inv_cost = inv_cost.drop(columns=["technology_ori", "category"], errors="ignore")

    # Sort data and calculate year-on-year growth rate of original value
    inv_cost = inv_cost.sort_values(["node_loc", "technology", "year_vtg"]).copy()

    inv_cost["value_growth"] = (
        inv_cost
        .groupby(["node_loc", "technology"])["value"]
        .transform(lambda x: x.div(x.shift(1)))
        .fillna(1.0)
    )

    inv_cost['non_coc_base0'] = (
        inv_cost
        .groupby(['node_loc', 'technology'])['non_coc_base']
        .transform('first')
    )

    # 3) Accumulate the product of growth to obtain the cumulative growth factor for each year
    inv_cost['cum_growth'] = (
        inv_cost
        .groupby(['node_loc', 'technology'])['value_growth']
        .cumprod()
    )
    # Adjust non-CoC part using the growth rate
    inv_cost["non_coc_new"] = inv_cost["non_coc_base0"] * inv_cost["cum_growth"]

    # Recalculate total value and CoC part using updated non-CoC and A
    inv_cost["value"] = inv_cost["non_coc_new"] / (1.0 - inv_cost["A"])
    inv_cost["coc_base"] = inv_cost["value"] * inv_cost["A"]

    # Save final output
    cols = ['node_loc', 'technology', 'year_vtg', 'value', 'unit', 'coc_base', 'non_coc_base']
    out = inv_cost[cols]
    out.to_csv(os.path.join(str(folder_path), out_filename), index=False)
