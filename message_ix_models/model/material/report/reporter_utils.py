import message_ix
from genno import Key

comm_tec_map = {
    "coal": ["meth_coal", "meth_coal_ccs"],
    "gas": ["meth_ng", "meth_ng_ccs"],
    "bio": ["meth_bio", "meth_bio_ccs"],
    "h2": ["meth_h2"],
}


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


def add_meth_export_calculations(rep: message_ix.Reporter, mode: str = "feedstock"):
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


def add_meth_import_calculations(rep: message_ix.Reporter, mode: str = "feedstock"):
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


def add_ammonia_non_energy_computations(rep: message_ix.Reporter):
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


def add_methanol_non_energy_computations(rep: message_ix.Reporter):
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
