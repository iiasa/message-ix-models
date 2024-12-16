import os
from typing import Literal

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path

from .config import Config
from .regional_differentiation import get_raw_technology_mapping, subset_module_map


def _get_module_scenarios_reduction(
    module: Literal["energy", "materials", "cooling"],
    energy_map_df: pd.DataFrame,
    tech_map_df: pd.DataFrame,
) -> pd.DataFrame:
    """Get scenarios reduction categories for a module.

    Parameters
    ----------
    module : str
        The module for which to get scenarios reduction categories.
    energy_map_df : pandas.DataFrame
        The technology mapping for the energy module.
    tech_map_df : pandas.DataFrame
        The technology mapping for the specific module.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: name of technology in MESSAGEix
        - SSP1: scenario reduction category for SSP1
        - SSP2: scenario reduction category for SSP2
        - SSP3: scenario reduction category for SSP3
        - SSP4: scenario reduction category for SSP4
        - SSP5: scenario reduction category for SSP5
        - LED: scenario reduction category for LED
    """
    # Get reduction scenarios for energy module
    scenarios_energy = pd.read_csv(
        package_data_path("costs", "energy", "scenarios_reduction.csv")
    )

    # for technologies in energy_map that are not in scenarios_energy,
    # assume scenario reduction across all scenarios is "none"
    # add same columns as scenarios_energy
    # and set all values to "none" except for message_technology column
    scenarios_energy_no_reduction = energy_map_df.query(
        "message_technology not in @scenarios_energy.message_technology"
    )[["message_technology"]].assign(
        **{
            col: "none"
            for col in scenarios_energy.columns
            if col != "message_technology"
        }
    )

    # combine scenarios_energy and scenarios_energy_no_reduction into scenarios_energy
    # order by message_technology
    scenarios_energy = (
        pd.concat([scenarios_energy, scenarios_energy_no_reduction], ignore_index=True)
        .sort_values("message_technology")
        .reset_index(drop=True)
    )

    if module != "energy":
        ffile = package_data_path("costs", module, "scenarios_reduction.csv")

        # if file exists, read it
        # else, scenarios_joined is the same as scenarios_energy
        if os.path.exists(ffile):
            scenarios_module = pd.read_csv(ffile)

            # if a technology exists in scenarios_module that exists in scen_red_energy,
            # remove it from scenarios_energy
            scenarios_energy = scenarios_energy[
                ~scenarios_energy["message_technology"].isin(
                    scenarios_module["message_technology"]
                )
            ]

            # concat scenarios_energy and scenarios_module
            scenarios_joined = pd.concat(
                [scenarios_energy, scenarios_module], ignore_index=True
            )
        else:
            scenarios_joined = scenarios_energy.copy()

        # In tech map, get technologies that are not in scenarios_joined
        # but are mapped to energy technologies
        # then use the scenarios reduction from the energy module for those technologies
        scenarios_module_map_to_energy = (
            tech_map_df.query(
                "(message_technology not in @scenarios_joined.message_technology) and \
                    (reg_diff_source == 'energy')"
            )
            .merge(
                scenarios_energy.rename(
                    columns={"message_technology": "base_message_technology"}
                ),
                how="left",
                left_on="reg_diff_technology",
                right_on="base_message_technology",
            )
            .drop(columns=["base_message_technology", "reg_diff_technology"])
            .drop_duplicates()
            .reset_index(drop=1)
        )

        scenarios_module_map_to_energy = scenarios_module_map_to_energy[
            scenarios_joined.columns.intersection(
                scenarios_module_map_to_energy.columns
            )
        ]

        scenarios_module_map_to_energy.query("message_technology == 'fc_h2_aluminum'")
        tech_map_df.query("message_technology == 'fc_h2_aluminum'")
        scenarios_energy.query("message_technology == 'h2_fc_I'")

        tech_map_df.query(
            "(message_technology not in @scenarios_joined.message_technology) and \
                    (reg_diff_source == 'energy')"
        ).query("message_technology == 'fc_h2_aluminum'")

        # for all technologies that are not in scenarios_module and
        # are not mapped to energy technologies,
        # assume scenario reduction across all scenarios is "none"
        # add same columns as scenarios_joined
        # and set all values to "none" except for message_technology column
        scenarios_module_no_reduction = tech_map_df.query(
            "message_technology not in @scenarios_joined.message_technology and \
                reg_diff_source != 'energy'"
        )[["message_technology"]].assign(
            **{
                col: "none"
                for col in scenarios_joined.columns
                if col != "message_technology"
            }
        )

        # combine scenarios_joined, scenarios_module_map_to_energy,
        # and scenarios_module_no_reduction
        # order by message_technology
        scenarios_all = (
            pd.concat(
                [
                    scenarios_joined,
                    scenarios_module_map_to_energy,
                    scenarios_module_no_reduction,
                ],
                ignore_index=True,
            )
            .sort_values("message_technology")
            .reset_index(drop=True)
        )

    return scenarios_all if module != "energy" else scenarios_energy


def _get_module_cost_reduction(
    module: Literal["energy", "materials", "cooling"],
    energy_map_df: pd.DataFrame,
    tech_map_df: pd.DataFrame,
) -> pd.DataFrame:
    """Get cost reduction values for technologies in a module.

    Parameters
    ----------
    module : str
        The module for which to get cost reduction values.
    energy_map_df : pandas.DataFrame
        The technology mapping for the energy module.
    tech_map_df : pandas.DataFrame
        The technology mapping for the specific module.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: name of technology in MESSAGEix
        - very_low: cost reduction for "none" scenario
        - low: cost reduction for "low" scenario
        - medium: cost reduction for "moderate" scenario
        - high: cost reduction for "high" scenario
        - very_high: cost reduction for "very high" scenario
    """
    # Get cost reduction for energy module
    reduction_energy = pd.read_csv(
        package_data_path("costs", "energy", "cost_reduction.csv"), comment="#"
    )

    # for technologies in energy_map that are not in reduction_energy,
    # assume scenario reduction across all scenarios is "none"
    # add same columns as reduction_energy
    # and set all values to "none" except for message_technology column
    reduction_energy_no_reduction = energy_map_df.query(
        "message_technology not in @reduction_energy.message_technology"
    )[["message_technology"]].assign(
        **{col: 0 for col in reduction_energy.columns if col != "message_technology"}
    )

    # combine reduction_energy and reduction_energy_no_reduction into reduction_energy
    # order by message_technology
    reduction_energy = (
        pd.concat([reduction_energy, reduction_energy_no_reduction], ignore_index=True)
        .sort_values("message_technology")
        .reset_index(drop=True)
    )

    if module != "energy":
        ffile = package_data_path("costs", module, "cost_reduction.csv")

        if os.path.exists(ffile):
            reduction_module = pd.read_csv(ffile, comment="#")

            # if a technology exists in scen_red_module that exists in scen_red_energy,
            # remove it from scen_red_energy
            reduction_energy = reduction_energy[
                ~reduction_energy["message_technology"].isin(
                    reduction_module["message_technology"]
                )
            ]

            # concat reduction_energy and reduction_module
            reduction_joined = pd.concat(
                [reduction_energy, reduction_module], ignore_index=True
            )
        else:
            reduction_joined = reduction_energy.copy()

        # In tech map, get technologies that are not in reduction_joined
        # but are mapped to energy technologies
        # then, use the reduction from the energy module for those technologies
        reduction_module_map_to_energy = (
            tech_map_df.query(
                "(message_technology not in @reduction_joined.message_technology) and \
                    (reg_diff_source == 'energy')"
            )
            .merge(
                reduction_energy.rename(
                    columns={"message_technology": "base_message_technology"}
                ),
                how="inner",
                left_on="reg_diff_technology",
                right_on="base_message_technology",
            )
            .drop(columns=["base_message_technology", "reg_diff_technology"])
            .drop_duplicates()
            .reset_index(drop=1)
        )

        reduction_module_map_to_energy = reduction_module_map_to_energy[
            reduction_joined.columns.intersection(
                reduction_module_map_to_energy.columns
            )
        ]

        # for all technologies that are not in reduction_module and
        # are not mapped to energy technologies,
        # assume scenario reduction across all scenarios is "none"
        # add same columns as reduction_joined
        # and set all values to "none" except for message_technology column
        reduction_module_no_reduction = tech_map_df.query(
            "message_technology not in @reduction_joined.message_technology and \
                reg_diff_source != 'energy'"
        )[["message_technology"]].assign(
            **{
                col: 0
                for col in reduction_joined.columns
                if col != "message_technology"
            }
        )

        # combine reduction_joined, reduction_module_map_to_energy,
        # and reduction_module_no_reduction
        # order by message_technology
        reduction_all = (
            pd.concat(
                [
                    reduction_joined,
                    reduction_module_map_to_energy,
                    reduction_module_no_reduction,
                ],
                ignore_index=True,
            )
            .sort_values("message_technology")
            .reset_index(drop=True)
        )

    return reduction_all if module != "energy" else reduction_energy


# create function to get technology reduction scenarios data
def get_technology_reduction_scenarios_data(
    first_year: int, module: Literal["energy", "materials", "cooling"]
) -> pd.DataFrame:
    # Get full list of technologies from mapping
    tech_map = energy_map = get_raw_technology_mapping("energy")

    # if module is not energy, run subset_module_map
    if module != "energy":
        module_map = get_raw_technology_mapping(module)
        module_sub = subset_module_map(module_map)

        # Remove energy technologies that exist in module mapping
        energy_map = energy_map.query(
            "message_technology not in @module_sub.message_technology"
        )

        tech_map = pd.concat([energy_map, module_sub], ignore_index=True)

    scenarios_reduction = _get_module_scenarios_reduction(module, energy_map, tech_map)
    cost_reduction = _get_module_cost_reduction(module, energy_map, tech_map)

    cost_reduction.query("message_technology == 'bio_hpl'")

    # get first year values
    adj_first_year = (
        tech_map[["message_technology", "first_year_original"]]
        .assign(
            first_technology_year=lambda x: np.where(
                x.first_year_original.isnull(),
                first_year,
                x.first_year_original,
            )
        )
        .assign(
            first_technology_year=lambda x: np.where(
                x.first_year_original > first_year, x.first_year_original, first_year
            )
        )
        .drop(columns=["first_year_original"])
    )

    # convert scenarios_reduction and cost_reduction to long format
    scenarios_reduction_long = scenarios_reduction.melt(
        id_vars=["message_technology"], var_name="scenario", value_name="reduction_rate"
    )
    cost_reduction_long = cost_reduction.melt(
        id_vars=["message_technology"],
        var_name="reduction_rate",
        value_name="cost_reduction",
    )

    # merge scenarios_reduction_long and cost_reduction_long
    # with adj_first_year
    df = scenarios_reduction_long.merge(
        cost_reduction_long, on=["message_technology", "reduction_rate"], how="left"
    ).merge(adj_first_year, on="message_technology", how="left")

    # if reduction_rate is "none", then set cost_reduction to 0
    df["cost_reduction"] = np.where(df.reduction_rate == "none", 0, df.cost_reduction)

    return df


def project_ref_region_inv_costs_using_reduction_rates(
    regional_diff_df: pd.DataFrame, config: Config
) -> pd.DataFrame:
    """Project investment costs for the reference region using cost reduction rates.

    This function uses the cost reduction rates for each technology under each scenario
    to project the capital costs for each technology in the reference region.

    The changing of costs is projected until the year 2100
    (hard-coded in this function), which might not be the same
    as :attr:`.Config.final_year` (:attr:`.Config.final_year` represents the final
    projection year instead). 2100 is hard coded because the cost reduction values are
    assumed to be reached by 2100.

    The returned data have the list of periods given by :attr:`.Config.seq_years`.

    Parameters
    ----------
    regional_diff_df : pandas.DataFrame
        Dataframe output from :func:`get_weo_region_differentiated_costs`
    config : .Config
        The function responds to, or passes on to other functions, the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.module`, and
        :attr:`~.Config.ref_region`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: name of technology in MESSAGEix
        - scenario: scenario (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - reference_region: reference region
        - first_technology_year: first year the technology is available in MESSAGEix.
        - year: year
        - inv_cost_ref_region_decay: investment cost in reference region in year.
    """

    # Get scenarios cost reduction data for technologies
    df_cost_reduction = get_technology_reduction_scenarios_data(
        config.y0, config.module
    )

    # Filter for reference region, and merge with reduction scenarios and discount rates
    # Calculate cost in reference region in 2100
    df_ref = (
        regional_diff_df.query("region == @config.ref_region")
        .merge(df_cost_reduction, on="message_technology")
        .assign(
            cost_region_2100=lambda x: x.reg_cost_base_year
            - (x.reg_cost_base_year * x.cost_reduction),
            b=lambda x: (1 - config.pre_last_year_rate) * x.cost_region_2100,
            r=lambda x: (1 / (2100 - config.base_year))
            * np.log((x.cost_region_2100 - x.b) / (x.reg_cost_base_year - x.b)),
            reference_region=config.ref_region,
        )
    )

    for y in config.seq_years:
        df_ref = df_ref.assign(
            ycur=lambda x: np.where(
                y <= config.base_year,
                x.reg_cost_base_year,
                (x.reg_cost_base_year - x.b) * np.exp(x.r * (y - config.base_year))
                + x.b,
            )
        ).rename(columns={"ycur": y})

    df_inv_ref = (
        df_ref.drop(
            columns=[
                "b",
                "r",
                "reg_diff_source",
                "reg_diff_technology",
                "region",
                "base_year_reference_region_cost",
                "reg_cost_ratio",
                "reg_cost_base_year",
                "fix_ratio",
                "reduction_rate",
                "cost_reduction",
                "cost_region_2100",
            ]
        )
        .melt(
            id_vars=[
                "message_technology",
                "scenario",
                "reference_region",
                "first_technology_year",
            ],
            var_name="year",
            value_name="inv_cost_ref_region_decay",
        )
        .assign(year=lambda x: x.year.astype(int))
    ).drop_duplicates()

    return df_inv_ref
