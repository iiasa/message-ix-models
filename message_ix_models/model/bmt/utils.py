"""Utility functions for MESSAGEix-BMT (Buildings, Materials, Transport) integration."""

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import ScenarioInfo

log = logging.getLogger(__name__)


def subtract_material_demand(
    scenario: "Scenario",
    info: "ScenarioInfo",
    sturm_r: pd.DataFrame,
    sturm_c: pd.DataFrame,
    method: str = "bm_subtraction",
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

    Returns
    -------
    pd.DataFrame
        Modified demand data with material demand subtracted
    """
    # Retrieve data once
    mat_demand = scenario.par("demand", {"level": "demand"})
    index_cols = ["node", "year", "commodity"]

    if method == "bm_subtraction":
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
