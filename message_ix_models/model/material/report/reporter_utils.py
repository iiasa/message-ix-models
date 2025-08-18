import itertools
from typing import TYPE_CHECKING

import pandas as pd
from genno import Key, Quantity

comm_tec_map = {
    "coal": ["meth_coal", "meth_coal_ccs"],
    "gas": ["meth_ng", "meth_ng_ccs"],
    "bio": ["meth_bio", "meth_bio_ccs"],
    "h2": ["meth_h2"],
}

if TYPE_CHECKING:
    from message_ix import Reporter


def add_methanol_share_calculations(rep: message_ix.Reporter, mode: str = "feedstock"):
    """Prepare reporter to compute regional bio-methanol shares of regional production.

    Reporter can compute share with key: ``share::biomethanol``

    Computation steps:

    1. Select all methanol production output
    2. Aggregate vintages to get production by technology for each year and node
       (methanol-by-tec)
    3. Aggregate to get global totals (methanol-total)
    4. Calculate methanol output shares by technology
    5. Aggregate meth_bio_ccs and meth_bio shares to get total bio-methanol share
    """
    t_filter2 = {
        "t": [
            "meth_coal",
            "meth_coal_ccs",
            "meth_ng_ccs",
            "meth_ng",
            "meth_bio",
            "meth_bio_ccs",
            "meth_h2",
        ],
        "c": ["methanol"],
        "l": "primary",
    }
    if mode == "feedstock":
        t_filter2.update({"l": ["primary_material"]})

    rep.add("out::methanol-prod", "select", "out:nl-t-ya-c-l", t_filter2)
    rep.add(
        "out::methanol-prod-by-tec",
        "group_sum",
        "out::methanol-prod",
        group="t",
        sum="c",
    )
    rep.add(
        "out::methanol-prod-total",
        "group_sum",
        "out::methanol-prod",
        group=["nl", "ya"],
        sum="t",
    )
    rep.add(
        "share::methanol-prod-by-tec",
        "div",
        "out::methanol-prod-by-tec",
        "out::methanol-prod-total",
    )
    for comm, tecs in comm_tec_map.items():
        rep.add(
            f"share::{comm}-methanol-prod",
            "aggregate",
            "share::methanol-prod-by-tec",
            groups={"t": {f"{comm}-methanol": tecs}},
            keep=False,
        )


def add_meth_export_calculations(rep: "Reporter", mode: str = "feedstock"):
    """Prepare reporter to compute bio-methanol exports.

    Reporter can compute exports with key: ``out::biomethanol-export``

    Computation steps:

    1. Select all methanol export outputs.
    2. Aggregate to get global totals (methanol-total).
    3. Calculate bio-methanol exports by multiplying regional exports with regional
       bio-methanol production shares.
    """
    add_methanol_share_calculations(rep, mode=mode)
    t_filter2 = {"t": "meth_exp", "m": mode}
    rep.add("out::methanol-export", "select", "out:nl-t-ya-m", t_filter2)
    rep.add(
        "out::methanol-export-total",
        "group_sum",
        "out::methanol-export",
        group="ya",
        sum="nl",
    )
    for comm in comm_tec_map.keys():
        rep.add(
            f"out::{comm}methanol-export",
            "mul",
            "out::methanol-export",
            f"share::{comm}-methanol-prod",
        )


def add_meth_import_calculations(rep: "Reporter", mode: str = "feedstock"):
    """Prepare reporter to compute bio-methanol import indicators.

    Reporter can compute imports with key: ``out::biomethanol-import``

    1. Select all methanol import outputs.
    2. Calculate bio-methanol share of global trade pool by dividing all bio-methanol
       exports by all methanol exports.
    3. Compute bio-methanol imports by multiplying bio-methanol share of global trade
       pool with regional imports.
    4. Calculate share of bio-methanol import as a fraction of regional methanol
       production.
    """
    add_meth_export_calculations(rep, mode=mode)
    t_filter2 = {"t": "meth_imp", "m": mode}
    rep.add("out::methanol-import", "select", "out:nl-t-ya-m", t_filter2)
    for comm in comm_tec_map.keys():
        rep.add(
            f"out::{comm}methanol-export-total",
            "group_sum",
            f"out::{comm}methanol-export",
            group="ya",
            sum="nl",
        )
        rep.add(
            f"share::{comm}methanol-export",
            "div",
            f"out::{comm}methanol-export-total",
            "out::methanol-export-total",
        )
        rep.add(
            f"out::{comm}methanol-import",
            "mul",
            "out::methanol-import",
            f"share::{comm}methanol-export",
        )
        rep.add(
            f"share::{comm}methanol-import",
            "div",
            f"out::{comm}methanol-import",
            "out::methanol-prod-total",
        )


def add_biometh_final_share(rep: message_ix.Reporter, mode: str = "feedstock"):
    """Prepare reporter to compute bio-methanol supply to final level.

    Reporter can compute bio-methanol with key: ``out::biomethanol-final``

    Bio-methanol supply to final level is defined as:
    *Domestic production + Import - Exports*
    """
    add_meth_import_calculations(rep, mode=mode)
    if mode == "feedstock":
        t_filter2 = {
            "t": ["meth_t_d"],
            "m": [mode],
        }
    else:
        t_filter2 = {
            "t": ["meth_t_d", "furnace_methanol_refining"],
            "m": [mode, "high_temp"],
        }
    rep.add("in::methanol-final0", "select", "in:nl-t-ya-m", t_filter2)
    rep.add("in::methanol-final", "sum", "in::methanol-final0", dimensions=["t", "m"])
    for comm in comm_tec_map.keys():
        rep.add(
            f"out::{comm}methanol-prod",
            "mul",
            "out::methanol-prod-total",
            f"share::{comm}-methanol-prod",
        )
        rep.add(
            f"out::{comm}methanol-final",
            "combine",
            f"out::{comm}methanol-prod",
            f"out::{comm}methanol-export",
            f"out::{comm}methanol-import",
            weights=[1, -1, 1],
        )
        rep.add(
            f"share::{comm}methanol-final",
            "div",
            f"out::{comm}methanol-final",
            "in::methanol-final",
        )


def add_ammonia_non_energy_computations(rep: "Reporter"):
    """Prepare reporter to compute process energy input for ammonia production.

    Reporter can compute process energy inputs with key: ``in::nh3-process-energy``

    Computation steps:

    1. Select all feedstock inputs for ammonia production
    2. Subtract ammonia energy output from feedstock input to get process energy input
    """
    t_filter1 = {
        "t": [
            "coal_NH3",
            "gas_NH3",
            "coal_NH3_ccs",
            "gas_NH3_ccs",
            "biomass_NH3",
            "biomass_NH3_ccs",
        ],
        "c": ["coal", "gas", "fueloil", "biomass"],
    }
    t_filter2 = {"t": ["electr_NH3"], "c": ["electr"]}
    rep.add("in::nh3-feedstocks1", "select", "in:nl-t-ya-m-c", t_filter1)
    rep.add("in::nh3-feedstocks2", "select", "in:nl-t-ya-m-c", t_filter2)
    rep.add(
        "in::nh3-feedstocks", "concat", "in::nh3-feedstocks1", "in::nh3-feedstocks2"
    )

    rep.add(
        "in::nh3-process-energy", "sub", "in::nh3-feedstocks", "out::nh3-non-energy"
    )


def add_methanol_non_energy_computations(rep: "Reporter"):
    """Prepare reporter to compute process energy input for methanol production.

    Reporter can compute process energy inputs with key: ``in::nh3-process-energy``

    Computation steps:

    1. Select all feedstock inputs for methanol production.
    2. Subtract methanol energy output from feedstock input to get process energy input.
    """
    tecs = [
        "meth_coal",
        "meth_ng",
        "meth_coal_ccs",
        "meth_ng_ccs",
        "meth_bio",
        "meth_bio_ccs",
        "meth_h2",
    ]
    t_filter2 = {
        "t": tecs,
        "c": ["coal", "gas", "biomass", "hydrogen"],
    }
    k1 = Key("in:nl-t-ya-m-c")
    k2 = Key("out:nl-t-ya-m-c")
    rep.add(k1["meth-feedstocks"], "select", k1, t_filter2, sums=True)
    t_filter2 = {
        "t": tecs,
        "c": ["methanol"],
    }
    rep.add(k2["meth-non-energy"], "select", k2, t_filter2, sums=True)
    k = rep.add(
        "in:nl-t-ya-m:meth-process-energy",
        "sub",
        "in:nl-t-ya-m:meth-feedstocks",
        "out:nl-t-ya-m:meth-non-energy",
    )
    return k


def add_pass_out_turbine_inp(rep: "Reporter"):
    """Prepare reporter to compute regional pass-out turbine input"""
    po_trb_rel_po_act = {"r": "pass_out_trb", "t": "po_turbine"}
    # calculate ratio of pass-out turbine activity to maximum possible activity
    rep.add("rel::po_out_trb_act", "select", "rel:nl-ya-t-r", po_trb_rel_po_act)
    po_rel_thermal_ppls = {
        "r": "pass_out_trb",
        "t": [
            "bio_istig",
            "bio_istig_ccs",
            "bio_ppl",
            "coal_adv",
            "coal_adv_ccs",
            "coal_ppl",
            "coal_ppl_u",
            "foil_ppl",
            "gas_ct",
            "gas_ppl",
            "igcc",
            "igcc_ccs",
            "loil_ppl",
            "gas_cc",
            "gas_cc_ccs",
            "geo_ppl",
            "loil_cc",
            "nuc_hc",
            "nuc_lc",
        ],
    }
    rep.add("rel::po_out_ppl", "select", "rel:r-nl-t-ya", po_rel_thermal_ppls)
    rep.add(
        "rel::po_out_trb_max",
        "group_sum",
        "rel::po_out_ppl",
        group=["nl", "ya"],
        sum="t",
    )
    rep.add(
        "share::po_turbine_act_shr",
        "div",
        "rel::po_out_trb_act",
        "rel::po_out_trb_max",
    )
    # calculate pass-out turbine input coefficient per technology
    rep.add(
        "relation_activity::po_turbine",
        "select",
        "relation_activity:r-nl-t-ya",
        po_trb_rel_po_act,
    )
    rep.add(
        "relation_activity::po_turbine_parents",
        "select",
        "relation_activity:r-nl-t-ya",
        po_rel_thermal_ppls,
    )
    rep.add(
        "share::po_turbine_act_shrs",
        "div",
        "relation_activity::po_turbine_parents",
        "relation_activity::po_turbine",
    )
    # calculate average pass-out turbine input per active year
    # usually universally 0.2, computed just for robustness
    rep.add("in_avg", "div", "in:nl-t-ya-m-c-l-h", "ACT:nl-t-ya-m-h")
    rep.add("in_avg::po_turbine", "select", "in_avg:nl-t-ya-m-c", {"t": "po_turbine"})
    rep.add(
        "in::po_turbine_by_tec",
        "mul",
        "share::po_turbine_act_shrs",
        "in_avg::po_turbine",
    )
    # calculate maximum potential pass-out turbine input per power technology
    rep.add(
        "out::ppl_to_po_turbine_max",
        "mul",
        "in:nl-t-ya-m-c:po_turbine_by_tec",
        "out:nl-t-ya-m-c",
    )
    # allocate pass-out turbine input to all tecs
    rep.add(
        "out::ppl_to_po_turbine",
        "mul",
        "out:nl-t-ya-m-c:ppl_to_po_turbine_max",
        "share::po_turbine_act_shr",
    )
    # calculate electricity production excluding pass-out turbine input
    rep.add(
        "out::po_turbine_parents",
        "select",
        "out:nl-t-ya-m-c",
        {"c": ["electr"], "t": po_rel_thermal_ppls["t"]},
    )
    rep.add(
        "out::elec_wo_po_turbine",
        "sub",
        "out:nl-t-ya-m-c:po_turbine_parents",
        "out:nl-t-ya-m-c:ppl_to_po_turbine",
    )
    # aggregate pass-out turbine input for check sum
    rep.add("in::po_turbine", "select", "in:nl-t-ya-m-c", {"t": "po_turbine"})


def add_ccs_addon_calcs(rep: "Reporter"):
    """Prepare reporter to compute CCS scrubber calculations.
    1) Calculate activity share of CCS scrubber on parent technologies
    2) Allocate electricity production from parent to CCS based on activity share
    3) Deduct own consumption of scrubber
        - if single parent, then simply subtract
        - if multiple parents, then calculate activity share of parent
          and subtract proportional to share
    4) concat to one dataframe for all CCS addons
    """
    addon_parent_map = {
        "bio_ppl": {"addon": "bio_ppl_co2scr", "parents": ["bio_ppl"]},
        "gas_ppl": {"addon": "g_ppl_co2scr", "parents": ["gas_ppl", "gas_cc"]},
        "gas_cc": {"addon": "g_ppl_co2scr", "parents": ["gas_ppl", "gas_cc"]},
        "coal_ppl": {"addon": "c_ppl_co2scr", "parents": ["coal_ppl"]},
    }
    act = Key("ACT:nl-t-ya")
    ou_t = Key("out:nl-t-ya-m-c:elec_wo_po_turbine")
    ou = ou_t.drop("t")
    k_in = Key("in:nl-t-ya-m-c")
    sh = Key("share:nl-t-ya:")
    for parent, map in addon_parent_map.items():
        addon = map["addon"]
        parents = map["parents"]
        k = Key(act[addon])
        k2 = act.drop("t")[parent]

        rep.add(k, "select", "ACT:nl-t-ya", {"t": [addon]})
        rep.add(k["parents-t"], "select", act, {"t": parents})
        rep.add(k["parents"], "group_sum", k["parents-t"], group=["nl", "ya"], sum="t")
        rep.add(sh[addon], "div", k, k["parents"])

        rep.add(k2, "select", act, {"t": parent})

        rep.add(sh[f"non_{addon}_{parent}"], "sub", Quantity(1), sh[addon])
        # 2)
        rep.add(ou[parent], "select", ou_t, {"t": parent})
        rep.add(ou_t[f"{parent}_scr"], "mul", ou[parent], sh[addon])
        rep.add(
            ou_t[f"{parent}_wo_scr"], "mul", ou[parent], sh[f"non_{addon}_{parent}"]
        )
        rep.add(
            ou_t[f"{parent}_scr-t"],
            "relabel",
            ou_t[f"{parent}_scr"],
            {"t": {addon: f"{parent}_{addon}"}},
        )
        rep.add(
            ou_t[f"{parent}_wo_scr-t"],
            "relabel",
            ou_t[f"{parent}_wo_scr"],
            {"t": {addon: parent}},
        )
        # 3)
        rep.add(k_in[f"elec_{addon}"], "select", k_in, {"t": addon})
        rep.add(sh[f"{addon}_parents_{parent}"], "div", k2, k["parents"])
        rep.add(
            k_in[f"elec_{addon}_{parent}"],
            "mul",
            k_in[f"elec_{addon}"],
            sh[f"{addon}_parents_{parent}"],
        )
        rep.add(
            ou_t[f"{parent}_scr-t-excl_ccs_penalty"],
            "sub",
            ou_t[f"{parent}_scr-t"],
            k_in[f"elec_{addon}_{parent}"],
        )
    # 4)
    rep.add(
        ou_t["ccs_addons"],
        "concat",
        *[ou_t[f"{i}_scr-t-excl_ccs_penalty"] for i in addon_parent_map.keys()],
    )
    rep.add(
        ou_t["parents_wo_scrs"],
        "concat",
        *[ou_t[f"{i}_wo_scr-t"] for i in addon_parent_map.keys()],
    )


def add_renewable_curtailment_calcs(rep: "Reporter"):
    # calculate electricity production excluding pass-out turbine input
    k1 = Key("in:nl-t-ya-m-c")
    k11 = k1.drop("t")
    k2 = Key("out:nl-t-ya-m-c")
    k3 = k2.drop("t")
    k4 = Key("share:nl-ya-m-c:")
    wind_curt = ["wind_curtailment1", "wind_curtailment2", "wind_curtailment3"]
    rep.add(
        k1["wind_curtailment-by-tec"],
        "select",
        k1,
        {"c": ["electr"], "t": wind_curt},
    )
    rep.add(
        k11["wind_curtailment"],
        "group_sum",
        k1["wind_curtailment-by-tec"],
        group=["nl", "ya", "m", "c"],
        sum="t",
    )
    pv_curt = ["solar_curtailment1", "solar_curtailment2", "solar_curtailment3"]
    rep.add(
        k1["pv_curtailment-by-tec"],
        "select",
        k1,
        {"c": ["electr"], "t": pv_curt},
    )
    rep.add(
        k11["pv_curtailment"],
        "group_sum",
        k1["pv_curtailment-by-tec"],
        group=["nl", "ya", "m", "c"],
        sum="t",
    )
    pv_ut = [
        "solar_res1",
        "solar_res2",
        "solar_res3",
        "solar_res4",
        "solar_res5",
        "solar_res6",
        "solar_res7",
        "solar_res8",
        "solar_res_hist_2000",
        "solar_res_hist_2005",
        "solar_res_hist_2010",
        "solar_res_hist_2015",
        "solar_res_hist_2020",
        "solar_res_hist_2025",
    ]
    onshore = [
        "wind_res1",
        "wind_res2",
        "wind_res3",
        "wind_res4",
        "wind_res_hist_2000",
        "wind_res_hist_2005",
        "wind_res_hist_2010",
        "wind_res_hist_2015",
        "wind_res_hist_2020",
        "wind_res_hist_2025",
    ]
    offshore = [
        "wind_ref1",
        "wind_ref2",
        "wind_ref3",
        "wind_ref4",
        "wind_ref5",
        "wind_ref_hist_2000",
        "wind_ref_hist_2005",
        "wind_ref_hist_2010",
        "wind_ref_hist_2015",
        "wind_ref_hist_2020",
        "wind_ref_hist_2025",
    ]
    rep.add(
        k2["wind_onshore-by-tec"],
        "select",
        k2,
        {"c": ["electr"], "t": onshore},
    )
    rep.add(
        k2["wind_offshore-by-tec"],
        "select",
        k2,
        {"c": ["electr"], "t": offshore},
    )
    rep.add(
        k2["pv_utility-by-tec"],
        "select",
        k2,
        {"c": ["electr"], "t": pv_ut},
    )

    rep.add(
        k3["wind_offshore"],
        "group_sum",
        k2["wind_offshore-by-tec"],
        group=["nl", "ya", "m", "c"],
        sum="t",
    )
    rep.add(
        k3["wind_onshore"],
        "group_sum",
        k2["wind_onshore-by-tec"],
        group=["nl", "ya", "m", "c"],
        sum="t",
    )
    rep.add(
        k3["pv_utility"],
        "group_sum",
        k2["pv_utility-by-tec"],
        group=["nl", "ya", "m", "c"],
        sum="t",
    )

    rep.add(k3["wind_total"], "add", k3["wind_onshore"], k3["wind_offshore"])
    rep.add(k4["wind_offshore"], "div", k3["wind_offshore"], k3["wind_total"])
    rep.add(k4["wind_onshore"], "div", k3["wind_onshore"], k3["wind_total"])
    rep.add(
        k11["wind_curtailment_offshore"],
        "mul",
        k11["wind_curtailment"],
        k4["wind_offshore"],
    )
    rep.add(
        k11["wind_curtailment_onshore"],
        "mul",
        k11["wind_curtailment"],
        k4["wind_onshore"],
    )
    rep.add(
        k3["wind_onshore_wo_curtailment"],
        "sub",
        k3["wind_onshore"],
        k11["wind_curtailment_onshore"],
    )
    rep.add(
        k3["wind_offshore_wo_curtailment"],
        "sub",
        k3["wind_offshore"],
        k11["wind_curtailment_offshore"],
    )
    rep.add(
        k3["pv_utility_wo_curtailment"], "sub", k3["pv_utility"], k11["pv_curtailment"]
    )

    rep.add(
        k2["wind_onshore_wo_curtailment-t"],
        "expand_dims",
        k3["wind_onshore_wo_curtailment"],
        {"t": ["wind_res"]},
    )
    rep.add(
        k2["wind_offshore_wo_curtailment-t"],
        "expand_dims",
        k3["wind_offshore_wo_curtailment"],
        {"t": ["wind_ref"]},
    )
    rep.add(
        k2["pv_utility_wo_curtailment-t"],
        "expand_dims",
        k3["pv_utility_wo_curtailment"],
        {"t": ["solar_res"]},
    )
    rep.add(
        k2["curtailed_renewables"],
        "concat",
        k2["pv_utility_wo_curtailment-t"],
        k2["wind_offshore_wo_curtailment-t"],
        k2["wind_onshore_wo_curtailment-t"],
    )
    print()


def add_se_elec(rep: "Reporter"):
    add_renewable_curtailment_calcs(rep)
    add_pass_out_turbine_inp(rep)
    add_ccs_addon_calcs(rep)
    po_out = Key("out:nl-t-ya-m-c:elec_wo_po_turbine")
    ccs_addons = Key("out:nl-t-ya-m-c:elec_wo_po_turbine")["ccs_addons"]
    addon_parents = Key("out:nl-t-ya-m-c:elec_wo_po_turbine")["parents_wo_scrs"]
    parent_tecs = rep.get(addon_parents).index.get_level_values("t").unique().tolist()
    rep.add(
        "se:nl-t-ya-m-c:elec_po", "select", po_out, {"t": parent_tecs}, inverse=True
    )
    rep.add(
        "se:nl-t-ya-m-c:po+ccs",
        "concat",
        ccs_addons,
        "se:nl-t-ya-m-c:elec_po",
        addon_parents,
    )


def add_cement_heat_share_calculations(rep: "Reporter"):
    rep.set_filters(
        t=[
            "clinker_dry_ccs_cement",
            "clinker_dry_cement",
            "clinker_wet_ccs_cement",
            "clinker_wet_cement",
        ],
        c="ht_heat",
    )
    t_filter2 = {"t": ["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"]}
    t_filter1 = {"t": ["clinker_dry_cement", "clinker_wet_cement"]}
    t_filter = {
        "t": [
            "clinker_dry_ccs_cement",
            "clinker_dry_cement",
            "clinker_wet_ccs_cement",
            "clinker_wet_cement",
        ]
    }
    rep.add("in::heat-cement", "select", "in:nl-t-ya-m-c-l", t_filter)
    rep.add(
        "in::cement-heat-total",
        "group_sum",
        "in::heat-cement",
        group=["nl", "ya"],
        sum="t",
    )
    rep.add("in::heat-cement-non-ccs", "select", "in:nl-t-ya-m-c-l", t_filter1)
    rep.add("in::heat-cement-ccs", "select", "in:nl-t-ya-m-c-l", t_filter2)
    rep.add(
        "in::heat-cement-ccs2",
        "group_sum",
        "in::heat-cement-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        "in::heat-cement-non-ccs2",
        "group_sum",
        "in::heat-cement-non-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        "out::methanol-prod-by-tec",
        "group_sum",
        "out::methanol-prod",
        group="t",
        sum="c",
    )
    rep.add(
        "share::cement-heat-ccs",
        "div",
        "in::heat-cement-ccs2",
        "in::cement-heat-total",
    )
    rep.add(
        "share::cement-heat-non-ccs",
        "div",
        "in::heat-cement-non-ccs2",
        "in::cement-heat-total",
    )


def add_cement_elec_share_calculations(rep: "Reporter"):
    rep.set_filters(
        t=[
            "clinker_dry_ccs_cement",
            "clinker_dry_cement",
            "clinker_wet_ccs_cement",
            "clinker_wet_cement",
        ],
        c="electr",
    )
    t_filter2 = {"t": ["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"]}
    t_filter1 = {"t": ["clinker_dry_cement", "clinker_wet_cement"]}
    t_filter = {
        "t": [
            "clinker_dry_ccs_cement",
            "clinker_dry_cement",
            "clinker_wet_ccs_cement",
            "clinker_wet_cement",
        ]
    }
    rep.add("in::elec-cement", "select", "in:nl-t-ya-m-c-l", t_filter)
    rep.add(
        "in::cement-elec-total",
        "group_sum",
        "in::elec-cement",
        group=["nl", "ya"],
        sum="t",
    )
    rep.add("in::elec-cement-non-ccs", "select", "in:nl-t-ya-m-c-l", t_filter1)
    rep.add("in::elec-cement-ccs", "select", "in:nl-t-ya-m-c-l", t_filter2)
    rep.add(
        "in::elec-cement-ccs2",
        "group_sum",
        "in::elec-cement-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        "in::elec-cement-non-ccs2",
        "group_sum",
        "in::elec-cement-non-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        "out::methanol-prod-by-tec",
        "group_sum",
        "out::methanol-prod",
        group="t",
        sum="c",
    )
    rep.add(
        "share::cement-elec-ccs",
        "div",
        "in::elec-cement-ccs2",
        "in::cement-elec-total",
    )
    rep.add(
        "share::cement-elec-non-ccs",
        "div",
        "in::elec-cement-non-ccs2",
        "in::cement-elec-total",
    )


if __name__ == "__main__":
    import ixmp
    import message_ix

    # mp = ixmp.Platform("ixmp_dev")
    # scen = message_ix.Scenario(mp, "SSP_SSP2_v5.0", "baseline")
    mp = ixmp.Platform("local2")
    scen = message_ix.Scenario(mp, "MESSAGEix-Materials", "baseline")
    rep = message_ix.Reporter.from_scenario(scen)
    add_se_elec(rep)
    add_renewable_curtailment_calcs(rep)
    add_pass_out_turbine_inp(rep)
    add_ccs_addon_calcs(rep)
    print()
    # main = pd.DataFrame(rep.get("out::elec_wo_po_turbine_gas_ppl"))
    # sub = pd.DataFrame(
    #     (rep.get("out:nl-ya-m-c:elec_wo_po_turbine_gas_ppl_wo_scrubber"))
    # )
    # check = sub[sub[0] != 0].join(main[main[0] != 0])
    # rep.get("in::po_turbine")
