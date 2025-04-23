"""Prepare data for water use for cooling & energy technologies."""

import pandas as pd

from message_ix_models import Context
from message_ix_models.model.water.data.irrigation_rules import (
    INPUT_IRRIGATION_RULES,
    OUTPUT_IRRIGATION_RULES,
)
from message_ix_models.model.water.dsl_engine import run_standard
from message_ix_models.model.water.utils import safe_concat
from message_ix_models.util import package_data_path


# water & electricity for irrigation
def add_irr_structure(context: "Context") -> dict[str, pd.DataFrame]:
    """Add irrigation withdrawal infrastructure
    The irrigation demands are added in

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["water build info"]``, plus the additional year 2010.
    """

    # define an empty dictionary
    results = {}

    # reading basin_delineation
    FILE2 = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE2)
    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )
    # Reference to the water configuration
    info = context["water build info"]

    # probably can be removed
    year_wat = [2010, 2015]
    year_wat.extend(info.Y)

    inp_list = []
    extra_args = {"year_vtg": info.Y}
    current_args = {"rule_dfs": df_node}
    for rule in INPUT_IRRIGATION_RULES.get_rule():
        inp_list.append(
            run_standard(r=rule, base_args=current_args, extra_args=extra_args)
        )

    inp = safe_concat(inp_list)
    inp["year_act"] = inp["year_vtg"]

    results["input"] = inp

    irr_out_list = []
    for rule in OUTPUT_IRRIGATION_RULES.get_rule():
        irr_out_list.append(
            run_standard(r=rule, base_args=current_args, extra_args=extra_args)
        )

    irr_out = safe_concat(irr_out_list)
    irr_out["year_act"] = irr_out["year_vtg"]

    results["output"] = irr_out

    return results
