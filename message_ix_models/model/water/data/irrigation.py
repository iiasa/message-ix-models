"""Prepare data for water use for cooling & energy technologies."""

import pandas as pd
from message_ix import make_df

from message_ix_models.util import broadcast, private_data_path


# water & electricity for irrigation
def add_irr_structure(context):
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
    PATH = private_data_path("water", "delineation", FILE2)
    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    if context.type_reg == "country":
        df_node["region"] = context.map_ISO_c[context.regions]
    else:
        df_node["region"] = f"{context.regions}_" + df_node["REGION"].astype(str)

    # Reference to the water configuration
    info = context["water build info"]

    # probably can be removed
    year_wat = [2010, 2015]
    year_wat.extend(info.Y)

    inp = make_df(
        "input",
        technology="irrigation_cereal",
        value=1,
        unit="-",
        level="water_supply",
        commodity="freshwater",
        mode="M1",
        time="year",
        time_origin="year",
        node_origin=df_node["region"],
        node_loc=df_node["region"],
    ).pipe(broadcast, year_vtg=info.Y)

    inp = inp.append(
        make_df(
            "input",
            technology="irrigation_oilcrops",
            value=1,
            unit="-",
            level="water_supply",
            commodity="freshwater",
            mode="M1",
            time="year",
            time_origin="year",
            node_origin=df_node["region"],
            node_loc=df_node["region"],
        ).pipe(broadcast, year_vtg=info.Y)
    )

    inp = inp.append(
        make_df(
            "input",
            technology="irrigation_sugarcrops",
            value=1,
            unit="-",
            level="water_supply",
            commodity="freshwater",
            mode="M1",
            time="year",
            time_origin="year",
            node_origin=df_node["region"],
            node_loc=df_node["region"],
        ).pipe(broadcast, year_vtg=info.Y)
    )
    # year_act = year_vts for tecs with 1 time-step lifetime
    inp["year_act"] = inp["year_vtg"]

    # Electricity values per unit of irrigation water supply
    # Reference: Evaluation of Water and Energy Use in
    # Pressurized Irrigation Networks in Southern Spain
    # Diaz et al. 2011 https://ascelibrary.org/
    # doi/10.1061/%28ASCE%29IR.1943-4774.0000338
    # Low Value :0.04690743
    # Average Value :0.101598174
    # High Value : 0.017123288

    # inp = inp.append(
    #     make_df(
    #         "input",
    #         technology="irrigation_sugarcrops",
    #         value=0.04690743,
    #         unit="-",
    #         level="final",
    #         commodity="electr",
    #         mode="M1",
    #         time="year",
    #         time_origin="year",
    #         node_origin=df_node["region"],
    #         node_loc=df_node["region"],
    #     ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat)
    # )
    #
    # inp = inp.append(
    #     make_df(
    #         "input",
    #         technology="irrigation_oilcrops",
    #         value=0.04690743,
    #         unit="-",
    #         level="final",
    #         commodity="electr",
    #         mode="M1",
    #         time="year",
    #         time_origin="year",
    #         node_origin=df_node["region"],
    #         node_loc=df_node["region"],
    #     ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat)
    # )
    #
    # inp = inp.append(
    #     make_df(
    #         "input",
    #         technology="irrigation_cereal",
    #         value=0.04690743,
    #         unit="-",
    #         level="final",
    #         commodity="electr",
    #         mode="M1",
    #         time="year",
    #         time_origin="year",
    #         node_origin=df_node["region"],
    #         node_loc=df_node["region"],
    #     ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat)
    # )
    # inp.loc[(inp['node_loc'] == 'R11_SAS') &
    #         (inp['commodity'] == 'electr'),
    #         "value",
    # ] *= 0.00004690743

    results["input"] = inp

    irr_out = make_df(
        "output",
        technology="irrigation_cereal",
        value=1,
        unit="km3/year",
        level="irr_cereal",
        commodity="freshwater",
        mode="M1",
        time="year",
        time_dest="year",
        node_loc=df_node["region"],
        node_dest=df_node["region"],
    ).pipe(broadcast, year_vtg=info.Y)

    irr_out = irr_out.append(
        make_df(
            "output",
            technology="irrigation_sugarcrops",
            value=1,
            unit="km3/year",
            level="irr_sugarcrops",
            commodity="freshwater",
            mode="M1",
            time="year",
            time_dest="year",
            node_loc=df_node["region"],
            node_dest=df_node["region"],
        ).pipe(
            broadcast,
            year_vtg=info.Y,
        )
    )

    irr_out = irr_out.append(
        make_df(
            "output",
            technology="irrigation_oilcrops",
            value=1,
            unit="km3/year",
            level="irr_oilcrops",
            commodity="freshwater",
            mode="M1",
            time="year",
            time_dest="year",
            node_loc=df_node["region"],
            node_dest=df_node["region"],
        ).pipe(
            broadcast,
            year_vtg=info.Y,
        )
    )

    irr_out["year_act"] = irr_out["year_vtg"]

    results["output"] = irr_out

    return results
