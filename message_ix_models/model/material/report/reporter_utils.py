from typing import TYPE_CHECKING, Any

import pandas as pd
from genno import Key, Quantity

from message_ix_models.report import compat, prepare_reporter

comm_tec_map = {
    "coal": ["meth_coal", "meth_coal_ccs"],
    "gas": ["meth_ng", "meth_ng_ccs"],
    "bio": ["meth_bio", "meth_bio_ccs"],
    "h2": ["meth_h2"],
}

if TYPE_CHECKING:
    from message_ix import Reporter

HIST_TRUE: bool = compat.HIST_TRUE

if HIST_TRUE:
    OUT = "out_hist"
    IN = "in_hist"
    REL = "rel_hist"
    ACT = "historical_activity"
else:
    OUT = "out"
    IN = "in"
    REL = "rel"
    ACT = "ACT"
compat.HIST_TRUE = HIST_TRUE


def add_methanol_share_calculations(rep: "Reporter", mode: str = "feedstock"):
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

    rep.add(f"{OUT}::methanol-prod", "select", f"{OUT}:nl-t-ya-c-l", t_filter2)
    rep.add(
        f"{OUT}::methanol-prod-by-tec",
        "group_sum",
        f"{OUT}::methanol-prod",
        group="t",
        sum="c",
    )
    rep.add(
        f"{OUT}::methanol-prod-total",
        "group_sum",
        f"{OUT}::methanol-prod",
        group=["nl", "ya"],
        sum="t",
    )
    rep.add(
        "share::methanol-prod-by-tec",
        "div",
        f"{OUT}::methanol-prod-by-tec",
        f"{OUT}::methanol-prod-total",
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
    rep.add(f"{OUT}::methanol-export", "select", f"{OUT}:nl-t-ya-m", t_filter2)
    rep.add(
        f"{OUT}::methanol-export-total",
        "group_sum",
        f"{OUT}::methanol-export",
        group="ya",
        sum="nl",
    )
    for comm in comm_tec_map.keys():
        rep.add(
            f"{OUT}::{comm}methanol-export",
            "mul",
            f"{OUT}::methanol-export",
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
    rep.add(f"{OUT}::methanol-import", "select", f"{OUT}:nl-t-ya-m", t_filter2)
    for comm in comm_tec_map.keys():
        rep.add(
            f"{OUT}::{comm}methanol-export-total",
            "group_sum",
            f"{OUT}::{comm}methanol-export",
            group="ya",
            sum="nl",
        )
        rep.add(
            f"share::{comm}methanol-export",
            "div",
            f"{OUT}::{comm}methanol-export-total",
            f"{OUT}::methanol-export-total",
        )
        rep.add(
            f"{OUT}::{comm}methanol-import",
            "mul",
            f"{OUT}::methanol-import",
            f"share::{comm}methanol-export",
        )
        rep.add(
            f"share::{comm}methanol-import",
            "div",
            f"{OUT}::{comm}methanol-import",
            f"{OUT}::methanol-prod-total",
        )


def add_biometh_final_share(rep: "Reporter", mode: str = "feedstock"):
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
    rep.add(f"{IN}::methanol-final0", "select", f"{IN}:nl-t-ya-m", t_filter2)
    rep.add(
        f"{IN}::methanol-final", "sum", f"{IN}::methanol-final0", dimensions=["t", "m"]
    )
    for comm in comm_tec_map.keys():
        rep.add(
            f"{OUT}::{comm}methanol-prod",
            "mul",
            f"{OUT}::methanol-prod-total",
            f"share::{comm}-methanol-prod",
        )
        rep.add(
            f"{OUT}::{comm}methanol-final",
            "combine",
            f"{OUT}::{comm}methanol-prod",
            f"{OUT}::{comm}methanol-export",
            f"{OUT}::{comm}methanol-import",
            weights=[1, -1, 1],
        )
        rep.add(
            f"share::{comm}methanol-final",
            "div",
            f"{OUT}::{comm}methanol-final",
            f"{IN}::methanol-final",
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
    rep.add(f"{IN}::nh3-feedstocks1", "select", f"{IN}:nl-t-ya-m-c", t_filter1)
    rep.add(f"{IN}::nh3-feedstocks2", "select", f"{IN}:nl-t-ya-m-c", t_filter2)
    rep.add(
        f"{IN}::nh3-feedstocks",
        "concat",
        f"{IN}::nh3-feedstocks1",
        f"{IN}::nh3-feedstocks2",
    )

    rep.add(
        f"{IN}::nh3-process-energy",
        "sub",
        f"{IN}::nh3-feedstocks",
        f"{OUT}::nh3-non-energy",
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
    k1 = Key(f"{IN}:nl-t-ya-m-c")
    k2 = Key(f"{OUT}:nl-t-ya-m-c")
    rep.add(k1["meth-feedstocks"], "select", k1, t_filter2, sums=True)
    t_filter2 = {
        "t": tecs,
        "c": ["methanol"],
    }
    rep.add(k2["meth-non-energy"], "select", k2, t_filter2, sums=True)
    k = rep.add(
        f"{IN}:nl-t-ya-m:meth-process-energy",
        "sub",
        f"{IN}:nl-t-ya-m:meth-feedstocks",
        f"{OUT}:nl-t-ya-m:meth-non-energy",
    )
    return k


def add_pass_out_turbine_inp(rep: "Reporter", po_tecs_filter, po_filter):
    """Prepare reporter to compute regional pass-out turbine input"""
    rel = Key(f"{REL}:r-nl-t-ya")
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
    # usually uniformly set to 0.2, computed just for robustness
    eff = Key("eff:nl-t-ya-m-c")
    k_in = Key(f"{IN}:nl-t-ya-m-c")
    rep.add(eff, "div", k_in, f"{ACT}:nl-t-ya-m:")
    rep.add(eff[tec], "select", eff, {"t": tec})
    inp = Key("input_coeff:nl-t-ya-m-c")
    rep.add(inp["po_turbine"], "mul", kappa, eff[tec])

    # calculate maximum potential pass-out turbine input per power technology
    out = Key(f"{OUT}:nl-t-ya-m-c")
    rep.add(k_in["elec+po_turbine_max"], "mul", inp["po_turbine"], out)
    # allocate pass-out turbine input to all tecs
    rep.add(k_in["elec+po_turbine"], "mul", k_in["elec+po_turbine_max"], k_util)
    # allocate pass-out turbine heat output to all tecs by scaling input with io ratio
    k = Key(f"{OUT}:nl-t-ya-m:heat+po_turbine")
    k_heat = rep.add(
        k["wrong_commodity"],
        "div",
        k_in["elec+po_turbine"],
        eff[tec],
    )
    rep.add(k.drop("c"), "drop_vars", k_heat, ["c"])
    rep.add(
        k,
        "expand_dims",
        k_heat.drop("c"),
        {"c": ["d_heat"]},
    )

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


def add_heat_calcs(rep: "Reporter") -> Key:
    k = Key(f"{OUT}:nl-t-ya-m-c")
    tec_filter = {"c": ["d_heat"]}
    rep.add(k["district_heat"], "select", k, tec_filter)
    rep.add(
        k["district_heat2"],
        "concat",
        k["district_heat"],
        f"{OUT}:nl-t-ya-m:heat+po_turbine",
    )
    return k["district_heat2"]


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
    act = Key(f"{ACT}:nl-t-ya:")
    ou_t = Key(f"{OUT}:nl-t-ya-m-c:elec_wo_po_turbine")
    ou = ou_t.drop("t")
    k_in = Key(f"{IN}:nl-t-ya-m-c")
    sh = Key("share:nl-t-ya:")
    for parent, map in addon_parent_map.items():
        addon = map["addon"]
        parents = map["parents"]
        k = Key(act[addon])
        k2 = act.drop("t")[parent]

        rep.add(k, "select", f"{ACT}:nl-t-ya", {"t": [addon]})
        rep.add(k["parents-t"], "select", act, {"t": parents})
        rep.add(k["parents"], "group_sum", k["parents-t"], group=["nl", "ya"], sum="t")
        rep.add(sh[addon], "div", k, k["parents"])

        rep.add(k2, "select", act, {"t": parent})
        mi = pd.MultiIndex.from_product(
            [rep.get("y"), [addon], rep.get("n")], names=["ya", "t", "nl"]
        )
        s = pd.Series(1.0, index=mi, name=None)
        rep.add(sh[f"non_{addon}_{parent}"], "sub", Quantity(s), sh[addon])
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
        rep.add(k_in[f"elec_{addon}"], "select", k_in, {"t": [addon]})
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
    k1 = Key(f"{IN}:nl-t-ya-m-c")
    k11 = k1.drop("t")
    k2 = Key(f"{OUT}:nl-t-ya-m-c")
    k3 = k2.drop("t")
    k4 = Key("share:nl-ya-m-c")
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


def add_se_elec(rep: "Reporter") -> Key:
    """Prepare reporter to compute electricity production.

    The computation steps added are:

    - Renewable curtailment deductions for wind and solar
    - Pass-out turbine deductions for thermal power plants
    - Production allocation for power plants with CCS add-on option
    - Biogas and hydrogen deductions on gas power plants
    """
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
    pv_rt = {
        "pv_rooftop": [
            "solar_res_RT1",
            "solar_res_RT2",
            "solar_res_RT3",
            "solar_res_RT4",
            "solar_res_RT5",
            "solar_res_RT6",
            "solar_res_RT7",
            "solar_res_RT8",
            "solar_res_rt_hist_2000",
            "solar_res_rt_hist_2005",
            "solar_res_rt_hist_2010",
            "solar_res_rt_hist_2015",
            "solar_res_rt_hist_2020",
            "solar_res_rt_hist_2025",
        ]
    }
    k2 = Key(f"{OUT}:nl-t-ya-m-c")
    rep.add(
        k2["pv_rooftop-t"], "select", k2, {"c": ["electr"], "t": pv_rt["pv_rooftop"]}
    )
    rep.add(k2["pv_rooftop_agg"], "sum", k2["pv_rooftop-t"], dimensions=["t"])
    rep.add(
        k2["pv_rooftop"],
        "expand_dims",
        k2["pv_rooftop_agg"],
        {"t": ["pv_rooftop"]},
    )
    rep.add(
        k2["curtailed_renewables"],
        "concat",
        k2["pv_util_wo_curtailment"],
        k2["wind_offshore_wo_curtailment"],
        k2["wind_onshore_wo_curtailment"],
        k2["pv_rooftop"],
    )
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
    po_out = Key(f"{OUT}:nl-t-ya-m-c:elec_wo_po_turbine")
    ccs_addons = po_out["ccs_addons"]
    addon_parents = po_out["parents_wo_scrs"]
    parent_tecs = rep.get(addon_parents).index.get_level_values("t").unique().tolist()
    rep.add(
        "se:nl-t-ya-m-c:elec_po", "select", po_out, {"t": parent_tecs}, inverse=True
    )
    k = rep.add(
        "se:nl-t-ya-m-c:po+ccs",
        "concat",
        ccs_addons,
        "se:nl-t-ya-m-c:elec_po",
        addon_parents,
        sums=False,
    )
    # derive electricity production from biogas and h2 by scaling electricity production
    # of gas power plants with proportion of h2 and biogas inflows to gas ppls
    # also rescale production from natural gas by inverse share of h2 and biogas
    gas_ppls_wo_ccs = [
        "gas_ppl",
        "gas_cc",
        "gas_ct",
    ]
    gas_ppls = [
        "gas_ppl",
        "gas_cc",
        "gas_ct",
        "gas_cc_g_ppl_co2scr",
        "gas_ppl_g_ppl_co2scr",
    ]
    h2_key = Key("share:nl-ya:hydrogen_in_pe_wo_ccs_retro")
    bio_key = Key("share:nl-ya:biogas_in_gas_consumer_biogas+incl_t_d_loss")
    rep.add(h2_key.append("t"), "expand_dims", h2_key, {"t": gas_ppls_wo_ccs})
    rep.add(bio_key.append("t"), "expand_dims", bio_key, {"t": gas_ppls})
    rep.add(k.drop("m"), "drop_vars", k, names="m")
    k1 = rep.add(
        "elec_from_h2:nl-t-ya-c", "mul", k.drop("m"), h2_key.append("t"), sums=False
    )
    k2 = rep.add(
        "elec_from_biogas:nl-t-ya-c",
        "mul",
        k.drop("m"),
        bio_key.append("t"),
        sums=False,
    )
    k3 = rep.add(
        "elec_gas_ppls_excl_blends:nl-ya-m-c-t",
        "combine",
        k,
        k1,
        k2,
        weights=[1, -1, -1],
    )
    rep.add(k1.append("m"), "expand_dims", k1, {"m": ["hydrogen"]})
    rep.add(k2.append("m"), "expand_dims", k2, {"m": ["biogas"]})
    se = rep.add("se:nl-t-ya-m-c:po+ccs2", "concat", k1.append("m"), k2.append("m"), k3)
    return se


def add_se_elec_stor(rep: "Reporter") -> Key:
    k = Key(f"{IN}:nl-t-ya-m-c")
    k1 = rep.add(k["stor_ppl"], "select", k, {"t": ["stor_ppl"]})
    k3 = rep.add("elec_from_storage:nl-t-ya-m-c", "mul", k1, Quantity(3))
    return k3


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
    rep.add(f"{IN}::heat-cement", "select", f"{IN}:nl-t-ya-m-c-l", t_filter)
    rep.add(
        f"{IN}::cement-heat-total",
        "group_sum",
        f"{IN}::heat-cement",
        group=["nl", "ya"],
        sum="t",
    )
    rep.add(f"{IN}::heat-cement-non-ccs", "select", f"{IN}:nl-t-ya-m-c-l", t_filter1)
    rep.add(f"{IN}::heat-cement-ccs", "select", f"{IN}:nl-t-ya-m-c-l", t_filter2)
    rep.add(
        f"{IN}::heat-cement-ccs2",
        "group_sum",
        f"{IN}::heat-cement-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        f"{IN}::heat-cement-non-ccs2",
        "group_sum",
        f"{IN}::heat-cement-non-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        f"{OUT}::methanol-prod-by-tec",
        "group_sum",
        f"{OUT}::methanol-prod",
        group="t",
        sum="c",
    )
    rep.add(
        "share::cement-heat-ccs",
        "div",
        f"{IN}::heat-cement-ccs2",
        f"{IN}::cement-heat-total",
    )
    rep.add(
        "share::cement-heat-non-ccs",
        "div",
        f"{IN}::heat-cement-non-ccs2",
        f"{IN}::cement-heat-total",
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
    rep.add(f"{IN}::elec-cement", "select", f"{IN}:nl-t-ya-m-c-l", t_filter)
    rep.add(
        f"{IN}::cement-elec-total",
        "group_sum",
        f"{IN}::elec-cement",
        group=["nl", "ya"],
        sum="t",
    )
    rep.add(f"{IN}::elec-cement-non-ccs", "select", f"{IN}:nl-t-ya-m-c-l", t_filter1)
    rep.add(f"{IN}::elec-cement-ccs", "select", f"{IN}:nl-t-ya-m-c-l", t_filter2)
    rep.add(
        f"{IN}::elec-cement-ccs2",
        "group_sum",
        f"{IN}::elec-cement-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        f"{IN}::elec-cement-non-ccs2",
        "group_sum",
        f"{IN}::elec-cement-non-ccs",
        group="c",
        sum="t",
    )
    rep.add(
        f"{OUT}::methanol-prod-by-tec",
        "group_sum",
        f"{OUT}::methanol-prod",
        group="t",
        sum="c",
    )
    rep.add(
        "share::cement-elec-ccs",
        "div",
        f"{IN}::elec-cement-ccs2",
        f"{IN}::cement-elec-total",
    )
    rep.add(
        "share::cement-elec-non-ccs",
        "div",
        f"{IN}::elec-cement-non-ccs2",
        f"{IN}::cement-elec-total",
    )


def add_net_co2_calcs(
    rep: "Reporter",
    non_ccs_filters: dict[str, list | str],
    ccs_filters: dict[str, list | str],
    name: str,
):
    """Calculates net CO2 emissions for technologies with CCS addons.

    Parameters
    ----------
    rep
    non_ccs_filters :
        map of technologies and CO2 accounting relation for parents
    ccs_filters :
        map of technologies and captured CO2 commodity name for CCS addons
    name
    """
    k1 = Key(f"{REL}:r-nl-t-ya")
    k2 = k1[f"{name}_co2"]
    k_ccs = Key(f"{OUT}:nl-t-ya-m-c")
    k_ccs2 = k_ccs[f"{name}_ccs"]

    rep.add(k2, "select", k1, non_ccs_filters)
    rep.add(k2.drop("t", "r"), "sum", k2, dimensions=["r", "t"])
    rep.add(k_ccs2, "select", k_ccs, ccs_filters)
    rep.add(k_ccs2.drop("t", "c", "m"), "sum", k_ccs2, dimensions=["m", "c", "t"])
    rep.add(f"co2:nl-ya:{name}", "sub", k2.drop("t", "r"), k_ccs2.drop("t", "c", "m"))
    rep.add(
        f"co2:nl-t-ya:{name}", "expand_dims", f"co2:nl-ya:{name}", {"t": [f"{name}"]}
    )


def add_non_ccs_gas_consumption(rep: "Reporter", addon_map: dict[str, dict]):
    # L3059 from message_data/tools/post_processing/default_tables.py
    # "gas_{cc,ppl}_share": shares of gas_cc and gas_ppl in the summed output of both
    # k0 = out(rep, addon_parent_map.keys())
    filter = {"t": addon_map.keys(), "c": ["gas"]}
    key = Key(f"{IN}:nl-t-ya-m-c")
    tag = "ccs_retro_ppls"
    k1 = rep.add(key["sel"][tag], "select", key, filter, sums=True)
    k2 = rep.add(key.drop("t")[tag], "assign_units", k1[0].drop("t"), units="GWa")
    for t in addon_map.keys():
        # k1 = out(rep, [t])
        k3 = rep.add(key["sel"][t], "select", key, {"t": [t], "c": ["gas"]})
        k4 = rep.add(key[t], "assign_units", k3, units="GWa")
        rep.add(Key(f"{t}_share", k4.dims), "div", k4, k2)

    filters = dict(c=["gas"], l=["secondary"])
    _args = ((t, values["addon"], f"{t}_share") for t, values in addon_map.items())
    keys = [compat.pe_w_ccs_retro(rep, *args, filters=filters) for args in _args]
    rep.add(
        Key(f"{IN}", keys[0].dims, "pe_w_ccs_retro+gas"), "concat", *keys, sums=True
    )
    return


def gas_consumer(rep: "Reporter", name: str, tecs: list[str], ccs: bool = True) -> Key:
    """Prepare reporter to compute gas consumption by technology.

    The computation steps added are:

    1) Select all gas consumers
    2) Subtract gas consumption by transmission and distribution losses
    3) If ccs is False, subtract gas consumption by CCS retrofit technologies
    4) Return key with gas consumption by technology

    Parameters
    ----------
    rep
    name :
        name for gas consumer key
    tecs :
        list of gas consuming technologies
    ccs:
        If False, subtract gas consumption by CCS retrofit technologies

    Returns
    -------
    Key that computes gas consumption by technology
    """
    k = Key(f"{IN}:nl-t-ya-m-c:sel")[f"gas_consumer_{name}"]
    k1 = Key(f"{IN}:nl-t-ya-m-c")[f"gas_consumer_{name}"]
    rep.add(k, "select", f"{IN}:nl-t-ya-m-c", {"t": tecs, "c": ["gas"]})
    in_gas = rep.add(k1, "assign_units", k, units="GWa")

    # pre-calc to get gas consumption by tec
    # L3026
    # f"{IN}:*:nonccs_gas_tecs": Input to non-CCS technologies using gas at l=(secondary,
    # final), net of output from transmission and distribution technologies.
    c_gas = dict(c=["gas"])
    t_d_tecs = {"t": ["gas_t_d", "gas_t_d_ch4"]}
    in_t = Key(f"{IN}:nl-t-c-ya")
    out_t = Key(f"{OUT}:nl-t-c-ya")
    # k0 = inp(rep, techs("gas", "all extra"), filters=c_gas)
    # k1 = out(rep, ["gas_t_d", "gas_t_d_ch4"], filters=c_gas)
    k1 = rep.add(
        out_t["sel"]["gas_transmission"], "select", out_t, {**c_gas, **t_d_tecs}
    )
    k11 = rep.add(out_t["gas_transmission"], "assign_units", k1, units="GWa")
    k2 = rep.add(in_gas["incl_t_d_loss"], "sub", in_gas, k11)
    if not ccs:
        addon_parent_map = {
            "gas_ppl": {"addon": "g_ppl_co2scr", "parents": ["gas_ppl", "gas_cc"]},
            "gas_cc": {"addon": "g_ppl_co2scr", "parents": ["gas_ppl", "gas_cc"]},
        }
        add_non_ccs_gas_consumption(rep, addon_parent_map)
        k21 = rep.add(in_t["pe_wo_ccs_retro"], "sub", k2, in_t["pe_w_ccs_retro+gas"])
        return k21
    return k2  # .drop("m")


def gas_mix_calculation(rep, in_gas: Key, mix_tech: str, name: str) -> Key:
    """Function to allocate blended gas to gas consumers.

    Steps:

    - Query total blend gas output
    - Calculate share of blend gas in total gas consumption per nl-ya,
      by dividing total output by input of selected gas consumers
    - Allocate blend gas share proportionally to each gas technology,
      by multiplying nl-ya share with total gas input nl-t-ya-m
    - create new commodity dimension with blend gas name as entry

    Parameters
    ----------
    rep
    in_gas :
        Key with gas consumption by technology with dims: ``nl-t-ya-c``
    mix_tech :
        Technology representing the gas blending, e.g. "h2_mix" or "gas_bio"
    name :
        name for blend gas key

    Returns
    -------
    Key that computes gas consumption by technology
    """

    k1 = compat.out(rep, [mix_tech], name=f"{name}_tot_abs")
    k2 = rep.add(
        f"share:nl-ya:{name}_in_{in_gas.tag}",
        "div",
        k1.drop("yv", "m"),
        in_gas.drop("t", "c", "m"),
    )
    k3 = rep.add(f"{IN}:nl-t-ya-m:{name}+{in_gas.tag}", "mul", in_gas.drop("c"), k2)
    k4 = rep.add(k3.append("c"), "expand_dims", k3, {"c": [name]})
    return k4


def add_non_energy_calc(rep: "Reporter") -> Key:
    """Add procedure to derive non-energy fraction for methanol and ammonia.

    1) Select methanol technologies and ammonia technologies.
    2) Convert ammonia production from Mt to GWa.
    3) Combine non-energy technologies for gas consumers.
    4) Split biogas and natural gas non-energy technologies based on biogas share.
    5) Expand dimensions to create separate entries for biogas and natural gas.
    6) Repeat steps 1,2,4 for other methanol and ammonia technologies.

    Parameters
    ----------
    rep
        Reporter to add keys to.

    Returns
    -------
    Key with non-energy flows with dims: `nl-t-ya-m-c`
    """
    k = Key("ACT:nl-t-ya-m")
    k1 = rep.add(
        k["meth_ng"], "select", k, {"t": ["meth_ng", "meth_ng_ccs"], "m": ["feedstock"]}
    )
    k2 = rep.add(k["gas_NH3+Mt"], "select", k, {"t": ["gas_NH3", "gas_NH3_ccs"]})
    k3 = rep.add(k["gas_NH3+GWa"], "mul", k2, Quantity(0.697615))
    k4 = rep.add(k["gas_non_energy_tecs"], "concat", k3, k1)
    k5 = rep.add(
        k["biogas_non_energy_tecs"],
        "mul",
        k4,
        "share:nl-ya:biogas_in_gas_consumer_biogas+incl_t_d_loss",
    )
    k6 = rep.add(k["natgas_non_energy_tecs"], "sub", k4, k5)
    k7 = rep.add(
        k["biogas_non_energy_tecs"].append("c"), "expand_dims", k5, {"c": ["biogas"]}
    )
    k8 = rep.add(
        k["natgas_non_energy_tecs"].append("c"), "expand_dims", k6, {"c": ["gas"]}
    )
    k9 = rep.add(k["gas_non_energy_tecs"].append("c"), "concat", k8, k7)
    comm_tec_map = {
        "coal": {"t": ["meth_coal", "meth_coal_ccs"], "m": ["feedstock"]},
        "biomass": {"t": ["meth_bio", "meth_bio_ccs"], "m": ["feedstock"]},
        "hydrogen": {
            "t": ["meth_h2"],
            "m": ["feedstock_fic", "feedstock_bic", "feedstock_dac"],
        },
    }
    comm_tec_map2 = {
        "coal": {"t": ["coal_NH3", "coal_NH3_ccs"]},
        "biomass": {"t": ["biomass_NH3", "biomass_NH3_ccs"]},
        "fueloil": {"t": ["fueloil_NH3", "fueloil_NH3_ccs"]},
        "electr": {"t": ["electr_NH3"]},
    }
    keys = []
    for comm, filter in comm_tec_map.items():
        k1 = rep.add(k[f"{comm}_methanol"], "select", k, filter)
        k3 = rep.add(
            k[f"{comm}_methanol"].append("c"), "expand_dims", k1, {"c": [comm]}
        )
        keys.append(k3)
    for comm, filter in comm_tec_map2.items():
        k1 = rep.add(k[f"{comm}_NH3+Mt"], "select", k, filter)
        k2 = rep.add(k[f"{comm}_NH3+GWa"], "mul", k1, Quantity(0.697615))
        k4 = rep.add(k[f"{comm}_NH3"].append("c"), "expand_dims", k2, {"c": [comm]})
        keys.append(k4)
    k = rep.add(k["non_energy_tecs"].append("c"), "concat", k9, *keys)
    k1 = rep.add(k["non_energy_tecs+GWa"], "assign_units", k, units="GWa")
    return k1


def pe_gas(rep: "Reporter"):
    """Prepare reporter to compute gas consumption by origin.

    Allocates mixed gases (biogas, hydrogen) to gas consumers proportionally to their
    total gas consumption.
    """
    final = [
        "gas_t_d",
        "gas_t_d_ch4",
        "gas_i",
        "gas_rc",
        "gas_trp",
        "furnace_gas_steel",
        "furnace_gas_cement",
        "furnace_gas_aluminum",
        "furnace_gas_petro",
        "furnace_gas_resins",
        "dri_gas_steel",
        "eaf_steel",
    ]
    conversion = [
        "gas_cc",
        "gas_ct",
        "gas_ppl",
        "gas_hpl",
        "furnace_gas_refining",
        "h2_smr",
        "gas_NH3",
        "meth_ng",
    ]
    ccs_tecs = [
        "dri_gas_ccs_steel",
        "gas_NH3_ccs",
        "gas_cc_ccs",
        "h2_smr_ccs",
    ]
    h2_tecs = [
        "gas_t_d",
        "gas_t_d_ch4",
        "gas_rc",
        "gas_ppl",
        "gas_cc",
        "gas_ct",
        "gas_hpl",
    ]
    k1 = gas_consumer(rep, "h2", h2_tecs, ccs=False)
    k2 = gas_consumer(rep, "biogas", final + ccs_tecs + conversion, ccs=True)
    k_h2 = gas_mix_calculation(rep, k1, "h2_mix", "hydrogen")
    k_bio = gas_mix_calculation(rep, k2, "gas_bio", "biogas")
    k = Key(f"{IN}:nl-t-ya-m-c")
    k5 = rep.add(k["gas_excl_biogas"], "sub", k2, k_bio.drop("c"))
    k51 = rep.add(k["gas_excl_biogas_h2"], "sub", k5, k_h2.drop("c"))
    k6 = rep.add(k["pe+gas"], "concat", k_bio, k_h2, k51)
    return k51


def co2(rep: "Reporter"):
    """Prepare reporter to compute CO2 emission factors for gas consumption.

    CO2 emissions from gas need to be computed separately since gas can be a mix of
    natural gas, biogas and hydrogen. Thus, the original `CO2_cc/ind` factors cannot be
    applied directly. Instead, the allocated natural gas consumption is multiplied with
    the standard emission factor for natural gas used in MESSAGE.
    """
    k = pe_gas(rep)
    k1 = rep.add("emi:nl-t-ya-m:gas", "mul", k.drop("c"), Quantity(0.482))
    k2 = Key(f"{REL}:r-nl-t-ya-m")
    k3 = rep.add(k2["gas_co2"], "expand_dims", k1, {"r": ["CO2_gas"]})
    gas_tecs = rep.get(k).index.get_level_values("t").unique()
    k4 = rep.add(k2["excl_gas_tecs"], "select", k2, {"t": gas_tecs}, inverse=True)
    rep.add(f"{REL}:r-nl-t-ya-m:emi_factors", "concat", k3, k4)
    return


def ccs(rep: "Reporter"):
    """Prepare reporter to compute CO2 captured by CCS addons."""
    k = Key(f"{REL}:r-nl-t-ya-m")
    shipping_tecs = ["foil_occ_bunker", "loil_occ_bunker", "LNG_occ_bunker"]
    k2 = rep.add(
        k["ccs_ship"],
        "select",
        k,
        {"r": "CO2_shipping", "t": shipping_tecs},
    )
    k_ship = rep.add(
        k2["ccs_ship-t"], "expand_dims", k2, {"c": ["fic_co2"], "l": ["secondary"]}
    )
    trans_tecs = ["co2_trans1", "co2_trans2"]
    k2 = rep.add(
        k["leakage"],
        "select",
        k,
        {
            "r": "CO2_cc",
            "t": ["co2_stor", "co2_trans1", "co2_trans2"],
        },
    )
    k_leak = rep.add(
        k2["leakage-c-l"], "expand_dims", k2, {"c": ["fic_co2"], "l": ["secondary"]}
    )
    k_out = Key(f"{OUT}:nl-t-m-ya-c-l")
    # k_in = Key(f"{IN}:nl-t-ya-m")
    rep.add(k_out["co2"], "select", k_out, {"c": ["fic_co2", "bic_co2", "dac_co2"]})
    bio_key = Key("share:nl-ya:biogas_in_gas_consumer_biogas+incl_t_d_loss")
    gas_tecs = ["gas_cc_ccs", "g_ppl_co2scr", "h2_smr_ccs", "meth_ng_ccs"]
    rep.add(bio_key.append("t"), "expand_dims", bio_key, {"t": gas_tecs})
    rep.add(
        "ccs:nl-t-m-ya-c-l:biogas",
        "mul",
        k_out["co2"],
        bio_key.append("t"),
        sums=False,
    )
    k_ccs_biogas = rep.add(
        "ccs:nl-t-m-ya-c-l:biogas2",
        "relabel",
        "ccs:nl-t-m-ya-c-l:biogas",
        {"c": {"fic_co2": "bic_co2"}},
    )
    rep.add("share:t-nl-ya:gas_excl_biogas", "sub", Quantity(1), bio_key.append("t"))
    k_ccs_natgas = rep.add(
        "ccs:nl-t-m-ya-c-l:natgas",
        "mul",
        k_out["co2"],
        bio_key.append("t"),
        sums=False,
    )
    rep.add(
        k_out["co2_non_gas"],
        "select",
        k_out["co2"],
        {"t": gas_tecs + trans_tecs},
        inverse=True,
    )
    rep.add(
        "ccs",
        "concat",
        k_out["co2_non_gas"],
        k_ccs_biogas,
        k_ccs_natgas,
        k_ship,
        k_leak,
    )
    # TODO: finish draft of this function
    return


def fe_dri(rep: "Reporter", in_key):
    """Prepare reporter to compute energy consumption in steel production."""
    ccs_filter = {"t": ["dri_gas_ccs_steel"]}
    no_ccs_filter = {"t": ["dri_gas_steel"]}
    k = Key("ACT:nl-t-ya")
    rep.add(k["DRI-CCS"], "select", k, ccs_filter, sums=True)
    rep.add(k["DRI"], "select", k, no_ccs_filter, sums=True)
    shr = rep.add(
        "Share:nl-ya:dri_ccs", "div", k.drop("t")["DRI-CCS"], k.drop("t")["DRI"]
    )
    shr2 = rep.add("Share:nl-ya:dri_no_ccs", "sub", Quantity(1), shr)

    k2 = in_key  # Key("in:nl-t-ya-m-c")
    dri_in = rep.add(k["dri_tot"], "select", k2, no_ccs_filter)
    k3 = rep.add(k2["dri"], "mul", dri_in, shr2)
    k4 = rep.add(k2["dri_ccs_wrong_tec"], "mul", dri_in, shr)
    k5 = rep.add(
        k2["dri_ccs"], "relabel", k4, {"t": {"dri_gas_steel": "dri_gas_ccs_steel"}}
    )
    key = rep.add(k["dri_split"], "concat", k3, k5)
    return key


def add_fe_key(rep: "Reporter") -> Key:
    pe_gas(rep)
    gas_final = [
        "gas_t_d",
        "gas_t_d_ch4",
        "gas_i",
        "gas_rc",
        "gas_trp",
        "furnace_gas_steel",
        "furnace_gas_cement",
        "furnace_gas_aluminum",
        "furnace_gas_petro",
        "furnace_gas_resins",
        "dri_gas_steel",
        "eaf_steel",
        "gas_NH3",
        "meth_ng",
        "dri_gas_ccs_steel",
        "gas_NH3_ccs",
    ]
    k = Key(f"{IN}:nl-t-ya-m-c-l")
    rep.add(k["fe_non_gas"], "select", k, {"c": ["gas"]}, inverse=True)
    rep.add(k["fe_gases"], "expand_dims", k.drop("l")["pe+gas"], {"l": ["final"]})
    no_ccs_filter = {"t": ["dri_gas_steel"]}

    rep.add(k["FE"], "concat", k["fe_non_gas"], k["fe_gases"])
    splitted_dri = fe_dri(rep, k["FE"])
    fe_excl_dri = rep.add(
        k["FE"]["excl_dri"], "select", k["FE"], no_ccs_filter, inverse=True
    )
    rep.add(k["FE2"], "concat", fe_excl_dri, splitted_dri)
    rep.add(k["FE2+GWa"], "assign_units", k["FE2"], units="GWa")
    non_energy = add_non_energy_calc(rep)
    k4 = rep.add(
        non_energy.append("s"), "expand_dims", non_energy, {"l": ["feedstock"]}
    )
    rep.add(k["FE3"], "sub", k["FE2+GWa"], non_energy)
    rep.add(k["FE5"], "concat", k["FE3"], k4)
    k_solar = fe_solar_integrated(rep)
    rep.add(k["FE6"], "concat", k["FE5"], k_solar)
    return k["FE6"]


def concat_hist_and_act(rep: "Reporter"):
    cat_year = rep.get("cat_year")
    fmy = cat_year[cat_year["type_year"] == "firstmodelyear"]["year"].values[0]
    modelyears = list(set(cat_year[cat_year["year"] >= fmy]["year"].values))
    # hist_years = list(set(cat_year[cat_year["year"] < fmy]["year"].values))
    rep.add(
        "ACT:nl-t-ya-m:modelyears",
        "select",
        "ACT:nl-t-ya-m",
        {"ya": modelyears},
        sums=True,
    )
    rep.add(
        "ACT:nl-t-ya-m:incl_historical",
        "concat",
        "ACT:nl-t-ya-m:modelyears",
        "historical_activity:nl-t-ya-m",
        sums=True,
    )


def iron_prod(rep: "Reporter"):
    """Add key that allocates iron and steel production and append to ``out``"""
    k = Key(f"{ACT}:nl-t-ya-m")
    rep.add("out:nl-t-ya-m:dri_ccs", "select", k, {"t": ["dri_gas_ccs_steel"]})
    k2 = rep.add(
        "prod:nl-t-ya-m-c-l:dri_ccs",
        "expand_dims",
        k,
        {"c": ["sponge_iron"], "l": ["tertiary_material"]},
    )
    k_out = Key(f"{OUT}:nl-t-ya-m-c")
    bof = rep.add(k_out["bof"], "select", k_out, {"t": ["bof_steel"], "c": ["steel"]})
    prim = rep.add(k_out["bof_prim"], "mul", bof, Quantity(0.85))
    prim2 = rep.add(
        k_out.append("l")["bof_prim"], "expand_dims", prim, {"l": ["primary"]}
    )
    sec = rep.add(k_out["bof_sec"], "mul", bof, Quantity(0.15))
    sec2 = rep.add(
        k_out.append("l")["bof_sec"], "expand_dims", sec, {"l": ["secondary"]}
    )
    rep.add("prod:nl-t-ya-m-c-l", "concat", k_out.append("l"), k2, prim2, sec2)


def add_eff(rep: "Reporter"):
    # k = Key(f"{OUT}:nl-t-ya-m-c")
    k = Key("output:nl-t-ya-m-c")
    k_el = rep.add(k["elec"], "select", k, {"c": ["electr"]}, sums=True)
    k2 = Key("input:nl-t-ya-m-c")
    k3 = rep.add("eff", "div", k_el[0].drop("c"), k2)
    rep.add("eff+%", "mul", k3, Quantity(100))


def fe_solar_integrated(rep) -> Key:
    solar_integrated = [
        "solar_i",
        "solar_rc",
        "solar_res_rt_hist_2025",
        "solar_res_rt_hist_2020",
        "solar_res_rt_hist_2015",
        "solar_res_rt_hist_2010",
        "solar_res_rt_hist_2005",
        "solar_res_rt_hist_2000",
        "solar_res_RT8",
        "solar_res_RT7",
        "solar_res_RT6",
        "solar_res_RT5",
        "solar_res_RT4",
        "solar_res_RT3",
        "solar_res_RT2",
        "solar_res_RT1",
    ]
    k = Key(f"{OUT}:nl-t-ya-m-c-l")
    k1 = rep.add(k["solar_integrated"], "select", k, {"t": solar_integrated})
    return k1


if __name__ == "__main__":
    from message_ix_models import Context

    ctx = Context()
    import ixmp
    import message_ix

    mp = ixmp.Platform("local3")
    scen = message_ix.Scenario(mp, "SSP_SSP2_v6.2", "baseline_wo_GLOBIOM_ts")
    rep = message_ix.Reporter.from_scenario(scen)
    prepare_reporter(ctx, reporter=rep)
    add_eff(rep)
    concat_hist_and_act(rep)
    add_fe_key(rep)
    ccs(rep)
    print()
