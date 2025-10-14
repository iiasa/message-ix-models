from typing import TYPE_CHECKING

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
    # usually uniformly set to 0.2, computed just for robustness
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
    # allocate pass-out turbine heat output to all tecs by scaling input with io ratio
    k = Key("out:nl-t-ya-m:heat+po_turbine")
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
    k = Key("out:nl-t-ya-m-c")
    tec_filter = {"c": ["d_heat"]}
    rep.add(k["district_heat"], "select", k, tec_filter)
    rep.add(
        k["district_heat2"],
        "concat",
        k["district_heat"],
        "out:nl-t-ya-m:heat+po_turbine",
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
    k2 = Key("out:nl-t-ya-m-c")
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
    po_out = Key("out:nl-t-ya-m-c:elec_wo_po_turbine")
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
        "elec_from_biogas:nl-ya-c-t",
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


def add_non_ccs_gas_consumption(rep: "Reporter", addon_map: dict[str, dict]):
    from message_ix_models.report.compat import pe_w_ccs_retro

    # L3059 from message_data/tools/post_processing/default_tables.py
    # "gas_{cc,ppl}_share": shares of gas_cc and gas_ppl in the summed output of both
    # k0 = out(rep, addon_parent_map.keys())
    filter = {"t": addon_map.keys(), "c": ["gas"]}
    key = Key("in:nl-t-ya-m-c")
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
    keys = [pe_w_ccs_retro(rep, *args, filters=filters) for args in _args]
    rep.add(Key("in", keys[0].dims, "pe_w_ccs_retro+gas"), "concat", *keys, sums=True)
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
    k = Key("in:nl-t-ya-m-c:sel")[f"gas_consumer_{name}"]
    k1 = Key("in:nl-t-ya-m-c")[f"gas_consumer_{name}"]
    rep.add(k, "select", "in:nl-t-ya-m-c", {"t": tecs, "c": ["gas"]})
    in_gas = rep.add(k1, "assign_units", k, units="GWa")

    # pre-calc to get gas consumption by tec
    # L3026
    # "in:*:nonccs_gas_tecs": Input to non-CCS technologies using gas at l=(secondary,
    # final), net of output from transmission and distribution technologies.
    c_gas = dict(c=["gas"])
    t_d_tecs = {"t": ["gas_t_d", "gas_t_d_ch4"]}
    in_t = Key("in:nl-t-c-ya")
    out_t = Key("out:nl-t-c-ya")
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
        k21 = rep.add(
            in_t["pe_wo_ccs_retro"], "sub", k2.drop("m"), in_t["pe_w_ccs_retro+gas"]
        )
        return k21
    return k2.drop("m")


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
    from message_ix_models.report.compat import out

    k1 = out(rep, [mix_tech], name=f"{name}_tot_abs")
    k2 = rep.add(
        f"share:nl-ya:{name}_in_{in_gas.tag}",
        "div",
        k1.drop("yv", "m"),
        in_gas.drop("t", "c"),
    )
    k3 = rep.add(f"in:nl-t-ya-m:{name}+{in_gas.tag}", "mul", in_gas.drop("c"), k2)
    k4 = rep.add(k3.append("c"), "expand_dims", k3, {"c": [name]})
    return k4


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
    k = Key("in:nl-t-ya-m-c")
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
    k2 = Key("rel:r-nl-t-ya-m")
    k3 = rep.add(k2["gas_co2"], "expand_dims", k1, {"r": ["CO2_gas"]})
    gas_tecs = rep.get(k).index.get_level_values("t").unique()
    k4 = rep.add(k2["excl_gas_tecs"], "select", k2, {"t": gas_tecs}, inverse=True)
    rep.add("rel:r-nl-t-ya-m:emi_factors", "concat", k3, k4)
    return


if __name__ == "__main__":
    from message_ix_models import Context

    ctx = Context()
    import ixmp
    import message_ix

    mp = ixmp.Platform("local3")
    scen = message_ix.Scenario(mp, "SSP_SSP2_v6.2", "baseline_wo_GLOBIOM_ts")
    rep = message_ix.Reporter.from_scenario(scen)
    prepare_reporter(ctx, reporter=rep)
    k1 = pe_gas(rep)
    k2 = add_se_elec(rep)
    print()
