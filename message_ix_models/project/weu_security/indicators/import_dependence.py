"""
Calculate import dependence indicators.
"""
import pandas as pd
import numpy as np


def _fso_stats(
    fso: pd.DataFrame,
    fso_fuel: str,
    region: list[str],
    use_units: str,
) -> pd.DataFrame:
    """Net Imports and Energy Demand from fuel_supply_out for one fuel.

    Energy Demand = Domestic + external Imports (total throughput, including
    crude processed for product re-export).
    Net Imports = external Imports + external Exports (export values are negative).
    """
    df = fso[(fso["fuel_type"] == fso_fuel) & (fso["unit"] == use_units)]

    # External imports: importer in region, exporter NOT in region
    ext_imp = df[
        (df["supply_type"] == "Imports")
        & df["region"].isin(region)
        & ~df["exporter"].isin(region)
    ]
    ext_imp = (
        ext_imp.groupby(["model", "scenario", "unit", "year"])["value"]
        .sum()
        .reset_index()
        .rename(columns={"value": "Gross Imports"})
    )

    # External exports: exporter in region, destination NOT in region
    # Destination encoded in variable as "Exports|<fuel>|<dest_region>"
    ext_exp = df[
        (df["supply_type"] == "Exports") & df["region"].isin(region)
    ].copy()
    ext_exp["dest"] = ext_exp["variable"].str.split("|").str[2]
    ext_exp = ext_exp[~ext_exp["dest"].isin(region)]
    ext_exp = (
        ext_exp.groupby(["model", "scenario", "unit", "year"])["value"]
        .sum()
        .reset_index()
        .rename(columns={"value": "Gross Exports"})  # negative values
    )

    # Domestic production in region
    dom = df[(df["supply_type"] == "Domestic") & df["region"].isin(region)]
    dom = (
        dom.groupby(["model", "scenario", "unit", "year"])["value"]
        .sum()
        .reset_index()
        .rename(columns={"value": "Domestic"})
    )

    result = ext_imp.merge(ext_exp, on=["model", "scenario", "unit", "year"], how="outer")
    result = result.merge(dom, on=["model", "scenario", "unit", "year"], how="outer")
    result = result.fillna(0)
    result["Net Imports"] = result["Gross Imports"] + result["Gross Exports"]
    result["Energy Demand"] = result["Domestic"] + result["Gross Imports"]

    return (
        result[["model", "scenario", "unit", "year", "Net Imports", "Energy Demand"]]
        .rename(columns={"model": "Model", "scenario": "Scenario", "unit": "Unit", "year": "Year"})
    )


def _bilateral_stats(
    input_data: pd.DataFrame,
    fuel: str,
    region: list[str],
    use_units: str,
    portfolio_level: str,
) -> pd.DataFrame:
    """Net Imports and Energy Demand from bilateral IAMC trade variables for one fuel."""
    trade = input_data[input_data["Unit"] == use_units].copy()
    trade = trade[trade["Variable"].str.split("|").str[0] == "Trade"]
    trade = trade[trade["Variable"].str.contains(f"\\|{fuel}", regex=True)]
    trade = trade[trade["Variable"].str.contains(portfolio_level)]

    trade["exporter"] = trade["Region"].str.split(">").str[0]
    trade["importer"] = trade["Region"].str.split(">").str[1]

    imp = trade[trade["importer"].isin(region) & ~trade["exporter"].isin(region)]
    exp = trade[trade["exporter"].isin(region) & ~trade["importer"].isin(region)]

    imp = (
        imp.groupby(["Model", "Scenario", "Unit", "Year"])["Value"]
        .sum()
        .reset_index()
        .rename(columns={"Value": "Total Imports"})
    )
    exp = (
        exp.groupby(["Model", "Scenario", "Unit", "Year"])["Value"]
        .sum()
        .reset_index()
        .rename(columns={"Value": "Total Exports"})
    )

    trade_stats = imp.merge(exp, on=["Model", "Scenario", "Unit", "Year"], how="outer").fillna(0)
    trade_stats["Net Imports"] = trade_stats["Total Imports"] - trade_stats["Total Exports"]

    # Energy demand from IAMC depth-1 variable
    demand = input_data[input_data["Unit"] == use_units].copy()
    demand = demand[demand["Variable"] == f"{portfolio_level}|{fuel}"]
    demand = demand[demand["Region"].isin(region)]
    demand = (
        demand.groupby(["Model", "Scenario", "Unit", "Year"])["Value"]
        .sum()
        .reset_index()
        .rename(columns={"Value": "Energy Demand"})
    )

    return trade_stats[["Model", "Scenario", "Unit", "Year", "Net Imports"]].merge(
        demand[["Model", "Scenario", "Unit", "Year", "Energy Demand"]],
        on=["Model", "Scenario", "Unit", "Year"],
        how="outer",
    ).fillna(0)


def calculate_import_dependence(
    input_data: pd.DataFrame,
    region: list[str],
    region_name: str,
    portfolio: list[str],
    portfolio_level: str,
    use_units: str = "EJ/yr",
    fuel_supply_data: pd.DataFrame | None = None,
    fuel_map: dict | None = None,
) -> pd.DataFrame:
    """
    Calculate import dependence for a given region.

    Args:
        input_data: IAMC-format DataFrame with trade and energy variables
        region: list of region identifiers to aggregate over
        region_name: human-readable name for the region (unused in computation)
        portfolio: list of fuel names (e.g. ["Biomass", "Coal", "Gas", "Oil"])
        portfolio_level: IAMC variable level ("Primary Energy" or "Secondary Energy")
        use_units: unit filter (default "EJ/yr")
        fuel_supply_data: optional fuel_supply_out DataFrame for FSO-based fuels
        fuel_map: mapping from portfolio fuel name to FSO fuel_type
                  (e.g. {"Oil": "Crude", "Gas": "Gas"})
    """
    fuel_map = fuel_map or {}

    per_fuel_dfs = []
    for fuel in portfolio:
        if fuel in fuel_map and fuel_supply_data is not None:
            stats = _fso_stats(fuel_supply_data, fuel_map[fuel], region, use_units)
        else:
            stats = _bilateral_stats(input_data, fuel, region, use_units, portfolio_level)

        stats["Fuel"] = fuel
        per_fuel_dfs.append(
            stats[["Model", "Scenario", "Fuel", "Unit", "Year", "Net Imports", "Energy Demand"]]
        )

    df = pd.concat(per_fuel_dfs, ignore_index=True)
    df["Net Import Dependence"] = df["Net Imports"] / df["Energy Demand"]

    # Total row: numerator = sum of per-fuel net imports;
    # denominator = portfolio_level aggregate from input_data
    net_imp_total = (
        df.groupby(["Model", "Scenario", "Unit", "Year"])["Net Imports"]
        .sum()
        .reset_index()
    )

    total_demand = input_data[input_data["Unit"] == use_units].copy()
    total_demand = total_demand[total_demand["Variable"] == portfolio_level]
    total_demand = total_demand[total_demand["Region"].isin(region)]
    total_demand = (
        total_demand.groupby(["Model", "Scenario", "Unit", "Year"])["Value"]
        .sum()
        .reset_index()
        .rename(columns={"Value": "Energy Demand"})
    )

    df_total = net_imp_total.merge(total_demand, on=["Model", "Scenario", "Unit", "Year"], how="left")
    df_total["Net Import Dependence"] = df_total["Net Imports"] / df_total["Energy Demand"]
    df_total["Fuel"] = portfolio_level
    df_total = df_total[
        ["Model", "Scenario", "Fuel", "Unit", "Year", "Net Imports", "Net Import Dependence", "Energy Demand"]
    ]

    df = pd.concat([df, df_total], ignore_index=True)

    # Make wide
    outdf = pd.DataFrame(columns=["Model", "Scenario", "Unit", "Year"])
    for f in portfolio + [portfolio_level]:
        tdf = df[df["Fuel"] == f].copy()
        tdf = tdf[["Model", "Scenario", "Unit", "Year", "Net Imports", "Net Import Dependence", "Energy Demand"]]
        tdf = tdf.rename(
            columns={
                "Net Import Dependence": f"Net Import Dependence ({f})",
                "Net Imports": f"Net Imports ({f})",
                "Energy Demand": f"Energy Demand ({f})",
            }
        )
        outdf = pd.merge(outdf, tdf, on=["Model", "Scenario", "Unit", "Year"], how="outer")

    return outdf
