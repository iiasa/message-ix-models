"""Functions used in :mod:`.project.ssp.script.scenarios`.

Originally added via :pull:`235`, cherry-picked and merged in :pull:`340`.

.. todo::
   - Move to other locations as appropriate.
   - Add tests.
   - Add documentation.
"""

import numpy as np
import pandas as pd


def modify_rc_bounds(s_original, s_target, mod_years):
    # get ACT for biomass_rc in SAS
    act_bio = s_original.var(
        "ACT", filters={"technology": "biomass_rc", "node_loc": "R12_SAS"}
    )
    del act_bio  # NB Currently unused

    # get ACT for loil_rc in SAS
    act_loil_sas = s_original.var(
        "ACT", filters={"technology": "loil_rc", "node_loc": "R12_SAS"}
    )

    val_bound_loil_sas = (
        act_loil_sas.loc[act_loil_sas["year_act"] == 2055]
        .reset_index()
        ._get_value(0, "lvl")
        .round(2)
    )

    # get ACT for loil_rc in MEA
    act_loil_mea = s_original.var(
        "ACT", filters={"technology": "loil_rc", "node_loc": "R12_MEA"}
    )

    val_bound_loil_mea = (
        act_loil_mea.loc[act_loil_mea["year_act"] == 2055]
        .reset_index()
        ._get_value(0, "lvl")
        .round(2)
    )

    # get bound_activity_up for biomass_rc in SAS
    orig_bound_bio = s_original.par(
        "bound_activity_up", filters={"technology": "biomass_rc", "node_loc": "R12_SAS"}
    )

    # get bound_activity_up for loil_rc in SAS
    orig_bound_loil_sas = s_original.par(
        "bound_activity_up", filters={"technology": "loil_rc", "node_loc": "R12_SAS"}
    )

    # get bound_activity_up for loil_rc in MEA
    orig_bound_loil_mea = s_original.par(
        "bound_activity_up", filters={"technology": "loil_rc", "node_loc": "R12_MEA"}
    )

    # function to get first row of bound and create new rows for set_years
    # and replace value with specified value
    def get_new_bound(df, set_years, value):
        new_rows = []
        for year in set_years:
            new_row = df.iloc[0].copy()
            new_row["year_act"] = year
            new_row["value"] = value
            new_rows.append(new_row)
        df_new = pd.DataFrame(new_rows).reset_index(drop=True)
        return df_new

    new_bound_bio = get_new_bound(orig_bound_bio, mod_years, 0)
    new_bound_loil_sas = get_new_bound(
        orig_bound_loil_sas, mod_years, val_bound_loil_sas
    )
    new_bound_loil_mea = get_new_bound(
        orig_bound_loil_mea, mod_years, val_bound_loil_mea
    )

    # check out and add each new bound_activity_up
    s_target.check_out()
    s_target.add_par("bound_activity_up", new_bound_bio)
    s_target.add_par("bound_activity_up", new_bound_loil_sas)
    s_target.add_par("bound_activity_up", new_bound_loil_mea)

    # commit the changes
    s_target.commit("New bounds added to biomass_rc and loil_rc")


def modify_tax_emission(s_original, s_target, scalar_val):
    # get PRICE_EMISSION variable
    price_emission = s_original.var(
        "PRICE_EMISSION", filters={"type_emission": "TCE_non-CO2"}
    )

    # get tax_emission parameter
    tax_emission_old = s_original.par(
        "tax_emission", filters={"type_emission": "TCE_non-CO2"}
    )

    # for PRICE_EMISSION after 2080, multiply by mult_price
    price_emission_mod = (
        price_emission.copy()
        .assign(lvl=lambda x: x.lvl * scalar_val)[["year", "lvl"]]
        .rename(columns={"year": "type_year"})
    )

    # merge with tax_emission
    tax_emission_new = (
        tax_emission_old.copy()
        .assign(type_year=lambda x: x.type_year.astype("int64"))
        .merge(price_emission_mod, on="type_year")
        .assign(value=lambda x: np.where(x.type_year >= 2080, x.lvl, x.value))
        .assign(type_emission="TCE")
        .drop(columns="lvl")
    )

    # check out and add new tax_emission
    s_target.check_out()
    s_target.remove_par("tax_emission", tax_emission_old)
    s_target.add_par("tax_emission", tax_emission_new)
    s_target.commit("New tax_emission added")


def remove_bof_steel_lower(s, rem_years):
    remove_growth_activity_lo = s.par(
        "growth_activity_lo",
        filters={"technology": ["bof_steel"], "year_act": rem_years},
    )
    remove_initial_activity_lo = s.par(
        "initial_activity_lo",
        filters={"technology": ["bof_steel"], "year_act": rem_years},
    )
    s.check_out()
    s.remove_par("growth_activity_lo", remove_growth_activity_lo)
    s.remove_par("initial_activity_lo", remove_initial_activity_lo)
    s.commit("bof_steel bounds removed")


def modify_steel_growth(s, techs, rem_years, growth_val):
    # get old values
    old_growth_activity_up = s.par(
        "growth_activity_up",
        filters={"technology": techs, "year_act": rem_years},
    )

    # modify values
    new_growth_activity_up = old_growth_activity_up.copy().assign(value=growth_val)

    # check out; remove old bounds and add new bounds
    s.check_out()
    s.remove_par("growth_activity_up", old_growth_activity_up)
    s.add_par("growth_activity_up", new_growth_activity_up)
    s.commit("Modified bounds for steel alternatives")


def modify_steel_initial(s, techs, rem_years, initial_val):
    # get old values
    old_initial_activity_up = s.par(
        "initial_activity_up",
        filters={"technology": techs, "year_act": rem_years},
    )

    # modify values
    new_initial_activity_up = old_initial_activity_up.copy().assign(value=initial_val)

    # check out; remove old bounds and add new bounds
    s.check_out()
    s.remove_par("initial_activity_up", old_initial_activity_up)
    s.add_par("initial_activity_up", new_initial_activity_up)
    s.commit("Modified bounds for steel alternatives")


def add_steel_sector_nze(s, steel_target_array):
    co2_ind = s.par(
        "relation_activity",
        filters={
            "relation": "CO2_ind",
            "technology": ["DUMMY_coal_supply", "DUMMY_gas_supply"],
        },
    )

    co2_emi = s.par(
        "output",
        filters={
            "commodity": "fic_co2",
            "technology": ["dri_gas_ccs_steel", "bf_ccs_steel"],
        },
    )

    co2_emi["relation"] = "CO2_Emission"
    co2_emi.rename(columns={"node_dest": "node_rel"}, inplace=True)
    co2_emi["year_rel"] = co2_emi["year_act"]
    co2_emi.drop(
        ["year_vtg", "commodity", "level", "time", "time_dest"], axis=1, inplace=True
    )

    co2_emi["value"] *= -1

    rel_new = pd.concat([co2_ind, co2_emi], ignore_index=True)

    rel_new = rel_new[rel_new["year_rel"] >= 2070]

    rel_new["node_rel"] = "R12_GLB"
    rel_new["relation"] = "steel_sector_target"

    rel_new = rel_new.drop_duplicates()

    s.check_out()
    s.add_set("relation", "steel_sector_target")

    relation_upper_df = pd.DataFrame(
        {
            "relation": "steel_sector_target",
            "node_rel": "R12_GLB",
            "year_rel": [2070, 2080, 2090, 2100],
            "value": steel_target_array,  # Slack values from Gamze, added manually
            "unit": "???",
        }
    )

    relation_lower_df = pd.DataFrame(
        {
            "relation": "steel_sector_target",
            "node_rel": "R12_GLB",
            "year_rel": [2070, 2080, 2090, 2100],
            "value": 0,
            "unit": "???",
        }
    )

    s.add_par("relation_activity", rel_new)
    s.add_par("relation_upper", relation_upper_df)
    s.add_par("relation_lower", relation_lower_df)

    s.commit("Steel sector target added.")


def add_balance_equality(scen):
    with scen.transact(""):
        scen.add_set("balance_equality", ["bic_co2", "secondary"])
        scen.add_set("balance_equality", ["fic_co2", "secondary"])
        scen.add_set("balance_equality", ["dac_co2", "secondary"])
        scen.add_set("balance_equality", ["methanol", "final_material"])
        scen.add_set("balance_equality", ["HVC", "demand"])
        scen.add_set("balance_equality", ["HVC", "export"])
        scen.add_set("balance_equality", ["HVC", "import"])
        scen.add_set("balance_equality", ["ethylene", "final_material"])
        scen.add_set("balance_equality", ["propylene", "final_material"])
        scen.add_set("balance_equality", ["BTX", "final_material"])
