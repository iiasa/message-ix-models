import itertools
from typing import TYPE_CHECKING

import pandas as pd
from genno import Key, Quantity

from message_ix_models.report import prepare_reporter

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


def add_pass_out_turbine_inp(rep: "Reporter", po_tecs_filter, po_filter):
    """Prepare reporter to compute regional pass-out turbine input"""
    rel = Key("rel:r-nl-t-ya")
    rel1 = rel.drop("t", "r")
    rel2 = rel.drop("r")
    k_util = Key("util_rate:nl-ya:po_turbine")
    relation = "pass_out_trb"
    tec = "po_turbine"
    # calculate ratio of pass-out turbine activity to maximum possible activity
    rep.add(rel1[relation][tec], "select", rel, po_filter)
    rep.add(rel2[relation]["powerplants"], "select", rel, po_tecs_filter)
    rep.add(
        rel1["po_trb_potential"],
        "sum",
        rel2[relation]["powerplants"],
        dimensions=["t"],
    )
    rep.add(k_util, "div", rel1[relation][tec], rel1["po_trb_potential"])

    # calculate pass-out turbine input coefficient per technology
    rel_act = Key("relation_activity:r-nl-t-ya")
    rel_act1 = rel_act.drop("r", "t")
    rel_act2 = rel_act.drop("r")
    rep.add(rel_act1[relation][tec], "select", rel_act, po_filter)
    rep.add(rel_act2[relation]["powerplants"], "select", rel_act, po_tecs_filter)
    kappa = Key("coupling_coeff:nl-t-ya:po_turbine")
    rep.add(kappa, "div", rel_act2[relation]["powerplants"], rel_act1[relation][tec])

    # calculate average pass-out turbine input per active year
    # usually universally 0.2, computed just for robustness
    eff = Key("eff:nl-t-ya-m-c")
    k_in = Key("in:nl-t-ya-m-c")
    rep.add(eff, "div", k_in, "ACT:nl-t-ya-m")
    rep.add(eff[tec], "select", eff, {"t": tec})
    inp = Key("input_coeff:nl-t-ya-m-c")
    rep.add(inp["po_turbine"], "mul", kappa, eff[tec])

    # calculate maximum potential pass-out turbine input per power technology
    out = Key("out:nl-t-ya-m-c")
    rep.add(k_in["elec+po_turbine_max"], "mul", inp["po_turbine"], out)
    # allocate pass-out turbine input to all tecs
    rep.add(k_in["elec+po_turbine"], "mul", k_in["elec+po_turbine_max"], k_util)

    # calculate electricity production excluding pass-out turbine input
    rep.add(
        out["po_turbine_ppls"],
        "select",
        out,
        {"c": ["electr"], "t": po_tecs_filter["t"]},
    )
    rep.add(
        out["elec_wo_po_turbine"],
        "sub",
        out["po_turbine_ppls"],
        k_in["elec+po_turbine"],
    )
    # aggregate pass-out turbine input for check sum
    rep.add(k_in.drop("t")[tec], "select", k_in, {"t": tec})


def add_ccs_addon_calcs(rep: "Reporter", addon_parent_map):
    """Prepare reporter to compute CCS scrubber calculations.
    1) Calculate activity share of CCS scrubber on parent technologies
    2) Allocate electricity production from parent to CCS based on activity share
    3) Deduct own consumption of scrubber
        - if single parent, then simply subtract
        - if multiple parents, then calculate activity share of parent
          and subtract proportional to share
    4) concat to one dataframe for all CCS addons
    """
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


def add_renewable_curtailment_calcs(
    rep: "Reporter", renewable_tecs: dict[str, list], curtailment_tecs: list, name: str
):
    # calculate electricity production excluding pass-out turbine input
    k1 = Key("in:nl-t-ya-m-c")
    k11 = k1.drop("t")
    k2 = Key("out:nl-t-ya-m-c")
    k3 = k2.drop("t")
    k4 = Key("share:nl-ya-m-c:")
    rep.add(
        k1[f"{name}_curtailment"],
        "select",
        k1,
        {"c": ["electr"], "t": curtailment_tecs},
    )
    rep.add(
        k11[f"{name}_curtailment"], "sum", k1[f"{name}_curtailment"], dimensions=["t"]
    )
    ks = []
    for tname, tecs in renewable_tecs.items():
        rep.add(
            k2[f"{tname}"],
            "select",
            k2,
            {"c": ["electr"], "t": tecs},
        )
        rep.add(k3[f"{tname}"], "sum", k2[f"{tname}"], dimensions=["t"])
        ks.append(k3[f"{tname}"])
    rep.add(k3[f"{name}_total"], "add", *ks)
    for tname in renewable_tecs.keys():
        rep.add(k4[f"{tname}"], "div", k3[f"{tname}"], k3[f"{name}_total"])
        rep.add(
            k11[f"{tname}_curtailment"],
            "mul",
            k11[f"{name}_curtailment"],
            k4[f"{tname}"],
        )
        rep.add(
            k3[f"{tname}_wo_curtailment"],
            "sub",
            k3[f"{tname}"],
            k11[f"{tname}_curtailment"],
        )
        rep.add(
            k2[f"{tname}_wo_curtailment"],
            "expand_dims",
            k3[f"{tname}_wo_curtailment"],
            {"t": [f"{tname}"]},
        )


def add_se_elec(rep: "Reporter"):
    wind_curt = ["wind_curtailment1", "wind_curtailment2", "wind_curtailment3"]
    tec_map = {
        "wind_onshore": [
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
        ],
        "wind_offshore": [
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
        ],
    }
    add_renewable_curtailment_calcs(rep, tec_map, wind_curt, "wind")
    pv_curt = ["solar_curtailment1", "solar_curtailment2", "solar_curtailment3"]
    pv_ut = {
        "pv_util": [
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
    }
    add_renewable_curtailment_calcs(rep, pv_ut, pv_curt, "pv")
    k2 = Key("out:nl-t-ya-m-c")
    rep.add(
        k2["curtailed_renewables"],
        "concat",
        k2["pv_util_wo_curtailment"],
        k2["wind_offshore_wo_curtailment"],
        k2["wind_onshore_wo_curtailment"],
    )
    rep.get(k2["curtailed_renewables"])

    po_trb_rel_po_act = {"r": "pass_out_trb", "t": "po_turbine"}
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
    add_pass_out_turbine_inp(rep, po_rel_thermal_ppls, po_trb_rel_po_act)

    addon_parent_map = {
        "bio_ppl": {"addon": "bio_ppl_co2scr", "parents": ["bio_ppl"]},
        "gas_ppl": {"addon": "g_ppl_co2scr", "parents": ["gas_ppl", "gas_cc"]},
        "gas_cc": {"addon": "g_ppl_co2scr", "parents": ["gas_ppl", "gas_cc"]},
        "coal_ppl": {"addon": "c_ppl_co2scr", "parents": ["coal_ppl"]},
    }
    add_ccs_addon_calcs(rep, addon_parent_map)
    po_out = Key("out:nl-t-ya-m-c:elec_wo_po_turbine")
    ccs_addons = po_out["ccs_addons"]
    addon_parents = po_out["parents_wo_scrs"]
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


def add_net_co2_calcs(
    rep: "Reporter",
    non_ccs_filters: dict[str, list | str],
    ccs_filters: dict[str, list | str],
    name: str,
):
    k1 = Key("rel:r-nl-t-ya")
    k2 = k1[f"{name}_co2"]
    k_ccs = Key("out:nl-t-ya-m-c")
    k_ccs2 = k_ccs[f"{name}_ccs"]

    rep.add(k2, "select", k1, non_ccs_filters)
    rep.add(k2.drop("t", "r"), "sum", k2, dimensions=["r", "t"])
    rep.add(k_ccs2, "select", k_ccs, ccs_filters)
    rep.add(k_ccs2.drop("t", "c", "m"), "sum", k_ccs2, dimensions=["m", "c", "t"])
    rep.add(f"co2:nl-ya:{name}", "sub", k2.drop("t", "r"), k_ccs2.drop("t", "c", "m"))
    rep.add(
        f"co2:nl-t-ya:{name}", "expand_dims", f"co2:nl-ya:{name}", {"t": [f"{name}"]}
    )


def biogas_calc(rep, context):
    """Partially duplicate the behaviour of :func:`.default_tables.retr_CO2emi`.

    Currently, this prepares the following keys and the necessary preceding
    calculations:

    - "transport emissions full::iamc": data for the IAMC variable
      "Emissions|CO2|Energy|Demand|Transportation|Road Rail and Domestic Shipping"
    """
    from functools import partial

    from genno.core.key import single_key

    from message_ix_models.model.bare import get_spec
    from message_ix_models.report.compat import (
        anon,
        assert_dims,
        emi,
        get_techs,
        inp,
        out,
        pe_w_ccs_retro,
        prepare_techs,
    )


    # Structure information
    spec = get_spec(context)
    prepare_techs(rep, spec.add.set["technology"])

    # Constants from report/default_units.yaml
    rep.add("conv_c2co2:", 44.0 / 12.0)  # dimensionless
    # “Carbon content of natural gas”
    rep.add("crbcnt_gas:", Quantity(0.482, units="Mt / GWa / a"))

    # Shorthand for get_techs(rep, …)
    techs = partial(get_techs, rep)

    def full(name: str) -> Key:
        """Return the full key for `name`."""
        return single_key(rep.full_key(name))

    # L3059 from message_data/tools/post_processing/default_tables.py
    # "gas_{cc,ppl}_share": shares of gas_cc and gas_ppl in the summed output of both
    k0 = out(rep, ["gas_cc", "gas_ppl"])
    for t in "gas_cc", "gas_ppl":
        k1 = out(rep, [t])
        k2 = rep.add(Key(f"{t}_share", k1.dims), "div", k0, k1)
        assert_dims(rep, single_key(k2))

    # L3026
    # "in:*:nonccs_gas_tecs": Input to non-CCS technologies using gas at l=(secondary,
    # final), net of output from transmission and distribution technologies.
    c_gas = dict(c=["gas"])
    k0 = inp(rep, techs("gas", "all extra"), filters=c_gas)
    k1 = out(rep, ["gas_t_d", "gas_t_d_ch4"], filters=c_gas)
    k2 = rep.add(Key("in", k1.dims, "nonccs_gas_tecs"), "sub", k0, k1)
    assert_dims(rep, single_key(k2))

    # L3091
    # "Biogas_tot_abs": absolute output from t=gas_bio [energy units]
    # "Biogas_tot": above converted to its CO₂ content = CO₂ emissions from t=gas_bio
    # [mass/time]
    Biogas_tot_abs = out(rep, ["gas_bio"], name="Biogas_tot_abs")
    rep.add("Biogas_tot", "mul", Biogas_tot_abs, "crbcnt_gas", "conv_c2co2")

    # L3052
    # "in:*:all_gas_tecs": Input to all technologies using gas at l=(secondary, final),
    # including those with CCS.
    k0 = inp(
        rep,
        ["gas_cc_ccs", "meth_ng", "meth_ng_ccs", "h2_smr", "h2_smr_ccs"],
        filters=c_gas,
    )
    k1 = rep.add(
        Key("in", k0.dims, "all_gas_tecs"), "add", full("in::nonccs_gas_tecs"), k0
    )
    assert_dims(rep, k1)

    # L3165
    # "Hydrogen_tot:*": CO₂ emissions from t=h2_mix [mass/time]
    k0 = emi(
        rep,
        ["h2_mix"],
        name="_Hydrogen_tot",
        filters=dict(r=["CO2_cc"]),
        unit_key="CO2 emissions",
    )
    # NB Must alias here, otherwise full("Hydrogen_tot") below gets a larger set of
    #    dimensions than intended
    rep.add(Key("Hydrogen_tot", k0.dims), k0)

    # L3063
    # "in:*:nonccs_gas_tecs_wo_ccsretro": "in:*:nonccs_gas_tecs" minus inputs to
    # technologies fitted with CCS add-on technologies.
    filters = dict(c=["gas"], l=["secondary"])
    pe_w_ccs_retro_keys = [
        pe_w_ccs_retro(rep, *args, filters=filters)
        for args in (
            ("gas_cc", "g_ppl_co2scr", full("gas_cc_share")),
            ("gas_ppl", "g_ppl_co2scr", full("gas_ppl_share")),
            # FIXME Raises KeyError
            # ("gas_htfc", "gfc_co2scr", None),
        )
    ]
    k0 = rep.add(anon(dims=pe_w_ccs_retro_keys[0]), "add", *pe_w_ccs_retro_keys)
    k1 = rep.add(
        Key("in", k0.dims, "nonccs_gas_tecs_wo_ccsretro"),
        "sub",
        full("in::nonccs_gas_tecs"),
        k0,
    )
    assert_dims(rep, k0, k1)

    # L3144, L3234
    # "Biogas_trp", "Hydrogen_trp": transportation shares of emissions savings from
    # biogas production/use, and from hydrogen production, respectively.
    # X_trp = X_tot * (trp input of gas / `other` inputs)
    k0 = inp(rep, techs("trp gas"), filters=c_gas)
    for name, other in (
        ("Biogas", full("in::all_gas_tecs")),
        ("Hydrogen", full("in::nonccs_gas_tecs_wo_ccsretro")),
    ):
        k1 = rep.add(anon(dims=other), "div", k0, other)
        k2 = rep.add(f"{name}_trp", "mul", f"{name}_tot", k1)
        assert_dims(rep, single_key(k1))


if __name__ == '__main__':
    from message_ix_models import Context
    ctx = Context()
    import ixmp
    import message_ix

    mp = ixmp.Platform('local3')
    scen = message_ix.Scenario(mp, 'SSP_SSP2_v6.2', "baseline_wo_GLOBIOM_ts")
    rep = message_ix.Reporter.from_scenario(scen)
    prepare_reporter(ctx, reporter=rep)
    biogas_calc(rep, ctx)
    print()
