"""Compatibility code that emulates :mod:`message_data` reporting."""
import logging
from functools import partial
from itertools import chain, count
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

from genno import Computer, Key, Quantity, quote
from genno.core.key import single_key

if TYPE_CHECKING:
    from ixmp.reporting import Reporter
    from sdmx.model.common import Code

    from message_ix_models import Context

log = logging.getLogger(__name__)

#: Filters for determining subsets of technologies.
#:
#: Each value is a Python expression :func:`eval`'d in an environment containing
#: variables derived from the annotations on codes for each technology. If the
#: expression evaluates to :obj:`True`, then the code is considered to belong to the set
#: identified by the key.
TECH_FILTERS = {
    "gas all": "c_in == 'gas' and l_in in 'secondary final' and '_ccs' not in id",
    "gas extra": "False",
    # Residential and commercial
    "trp coal": "sector == 'transport' and c_in == 'coal'",
    "trp gas": "sector == 'transport' and c_in == 'gas'",
    "trp foil": "sector == 'transport' and c_in == 'fueloil'",
    "trp loil": "sector == 'transport' and c_in == 'lightoil'",
    "trp meth": "sector == 'transport' and c_in == 'methanol'",
    # Transport
    "rc gas": "sector == 'residential/commercial' and c_in == 'gas'",
}


_ANON = map(lambda n: Key(f"_{n}"), count())


def anon(name: Optional[str] = None, dims: Optional[Key] = None) -> Key:
    """Create an ‘anonymous’ :class:`.Key` with `dims` optionally from another Key."""
    result = next(_ANON) if name is None else Key(name)

    return result.append(*getattr(dims, "dims", []))


def get_techs(c: Computer, prefix: str, kinds: Optional[str] = None) -> List[str]:
    """Return a list of technologies.

    The list is assembled from entries in :data:`TECHS` with the keys "{prefix} {kind}",
    with one `kind` for each space-separated item in `kinds`.
    """
    _kinds = kinds.split() if kinds else [""]
    return list(chain(*[c.graph[f"t::{prefix} {k}".rstrip()][0].data for k in _kinds]))


def make_shorthand_function(
    base_name: str, to_drop: str, default_unit_key: Optional[str] = None
):
    """Create a shorthand function for adding tasks to a :class:`.Reporter`."""
    _to_drop = to_drop.split()

    def func(
        c: Computer,
        technologies: List[str],
        *,
        name: Optional[str] = None,
        filters: Optional[dict] = None,
        unit_key: Optional[str] = default_unit_key,
    ) -> Key:
        base = single_key(c.full_key(base_name))
        key = anon(name, dims=base)

        indexers = dict(t=technologies)
        indexers.update(filters or {})

        if unit_key:
            c.add(key + "sel", "select", base, indexers=indexers)
            c.add(
                key,
                "assign_units",
                key + "sel",
                units=c.graph["config"]["model"].units[unit_key],
                sums=True,
            )
        else:
            c.add(key, "select", base, indexers=indexers, sums=True)

        # Return the partial sum over some dimensions
        return key.drop(*_to_drop)

    return func


inp = make_shorthand_function("in", "c h ho l no t", "energy")
emi = make_shorthand_function("rel", "nr r t yr")
out = make_shorthand_function("out", "c h hd l nd t", "energy")


def eff(
    c: Computer,
    technologies: List[str],
    filters_in: Optional[dict] = None,
    filters_out: Optional[dict] = None,
) -> Key:
    num = c.graph.unsorted_key(inp(c, technologies, filters=filters_in).append("t"))
    denom = c.graph.unsorted_key(out(c, technologies, filters=filters_out).append("t"))
    assert isinstance(num, Key)
    assert isinstance(denom, Key)

    key = anon(dims=num)

    c.add(key, "div", num, denom, sums=True)

    return key.drop("t")


def pe_wCSSretro(
    c: Computer,
    t: str,
    t_scrub: str,
    k_share: Optional[Key],
    filters: Optional[dict] = None,
) -> Key:
    """Equivalent to :func:`default_tables._pe_wCCS_retro` at L129."""
    ACT: Key = single_key(c.full_key("ACT"))

    k0 = out(c, [t_scrub])
    k1 = c.add(anon(), "mul", k0, k_share) if k_share else k0

    k2 = anon(dims=ACT).drop("t")
    c.add(k2, "select", ACT, indexers=dict(t=t), drop=True, sums=True)

    # TODO determine the dimensions to drop for the numerator
    k3 = anon(dims=k2)
    c.add(k3, "div", k2.drop("yv"), k2, sums=True)

    filters_out = dict(c=["electr"], l=["secondary"])
    k4 = eff(c, [t], filters_in=filters, filters_out=filters_out)
    k5 = single_key(c.add(anon(), "mul", k3, k4))
    k6 = single_key(c.add(anon(dims=k5), "div", k1, k5))

    return k6


def prepare_techs(c: Computer, technologies: List["Code"]) -> None:
    """Prepare sets of technologies in `c`.

    Add keys like "t::{key}" to `c` for each `key` in :data:`TECH_FILTERS`, by applying
    the filter expression to `technologies`.
    """
    result: Mapping[str, List[str]] = {k: list() for k in TECH_FILTERS}

    warned = set()  # Filters that raise some kind of Exception

    # Iterate over technologies
    for t in technologies:
        # Assemble information about `t` from its annotations
        info: Dict[str, Any] = dict(id=t.id)
        # Sector
        info["sector"] = str(t.get_annotation(id="sector").text)
        try:
            # Input commodity and level
            info["c_in"], info["l_in"] = t.eval_annotation("input")
        except (TypeError, ValueError):
            info["c_in"] = info["l_in"] = None

        # Iterate over keys and respective filters
        for key, expr in TECH_FILTERS.items():
            try:
                # Apply the filter to the `info` about `t`
                if eval(expr, None, info) is True:
                    # Filter evaluates to True → add `t` to the list of labels for `key`
                    result[key].append(t.id)
            except Exception as e:
                # Warn about this filter, only once
                if expr not in warned:
                    log.warning(f"{e!r} when evaluating {expr!r}")
                    warned.add(expr)

    # Add keys like "t::trp gas" corresponding to TECH_FILTERS["trp gas"]
    for k, v in result.items():
        c.add(f"t::{k}", quote(sorted(v)))


def callback(rep: "Reporter", context: "Context") -> None:
    """Partially duplicate the behaviour of :func:`.default_tables.retr_CO2emi`."""
    from message_ix_models.model.bare import get_spec

    from . import iamc

    N = len(rep.graph)

    # Structure information
    spec = get_spec(context)
    prepare_techs(rep, spec.add.set["technology"])

    # Constants from report/default_units.yaml
    rep.add("conv_c2co2:", 44.0 / 12.0)  # dimensionless
    # “Carbon content of natural gas”
    rep.add("crbcnt_gas:", Quantity(0.482, units="Mt / GWa / a"))

    # Shorthand for get_techs(rep, …)
    techs = partial(get_techs, rep)

    # L3059 from message_data/tools/post_processing/default_tables.py
    k0 = out(rep, ["gas_cc", "gas_ppl"])
    k1 = out(rep, ["gas_cc"])
    k2 = out(rep, ["gas_ppl"])
    gas_cc_share = Key("gas_cc_share", k0.dims)
    rep.add(gas_cc_share, "div", k0, k1)
    gas_ppl_share = Key("gas_ppl_share", k0.dims)
    rep.add(gas_ppl_share, "div", k0, k2)

    # L3026
    c_gas = dict(c=["gas"])
    k1 = inp(rep, techs("gas", "all extra"), filters=c_gas)
    k2 = out(rep, ["gas_t_d", "gas_t_d_ch4"], filters=c_gas)
    inp_nonccs_gas_tecs = Key("inp_nonccs_gas_tecs", k2.dims)
    rep.add(inp_nonccs_gas_tecs, "sub", k1, k2)

    # L3091
    Biogas_tot_abs = out(rep, ["gas_bio"])
    Biogas_tot = rep.add(
        "Biogas_tot", "mul", Biogas_tot_abs, "crbcnt_gas", "conv_c2co2"
    )

    # L3052
    key = inp(
        rep,
        ["gas_cc_ccs", "meth_ng", "meth_ng_ccs", "h2_smr", "h2_smr_ccs"],
        filters=c_gas,
    )
    inp_all_gas_tecs = Key("inp_all_gas_tecs", key.dims)
    rep.add(inp_all_gas_tecs, "add", inp_nonccs_gas_tecs, key)

    # L3165
    Hydrogen_tot = emi(
        rep, ["h2_mix"], filters=dict(r="CO2_cc"), unit_key="CO2 emissions"
    )

    # L3063
    filters = dict(c=["gas"], l=["secondary"])

    keys = [
        pe_wCSSretro(rep, *args, filters=filters)
        for args in (
            ("gas_cc", "g_ppl_co2scr", gas_cc_share),
            ("gas_ppl", "g_ppl_co2_scr", gas_ppl_share),
            # FIXME Raises KeyError
            # ("gas_htfc", "gfc_co2scr", None),
        )
    ]

    key = Key.product(anon().name, *keys)
    rep.add(key, "add", *keys)

    inp_nonccs_gas_tecs_wo_CCSRETRO = Key("inp_nonccs_gas_tecs_wo_CCSRETRO", key.dims)
    rep.add(inp_nonccs_gas_tecs_wo_CCSRETRO, "sub", inp_nonccs_gas_tecs, key)

    # L3144
    key = inp(rep, techs("trp gas"), filters=c_gas)
    key = rep.add(anon(), "mul", Biogas_tot, key)

    Biogas_trp = Key.product("Biogas_trp", key, inp_all_gas_tecs)
    rep.add(Biogas_trp, "div", key, inp_all_gas_tecs)

    # L3234
    key = inp(rep, techs("trp gas"), filters=c_gas)
    key = rep.add(anon(), "mul", Hydrogen_tot, key)

    Hydrogen_trp = Key.product("Hydrogen_trp", key, inp_nonccs_gas_tecs_wo_CCSRETRO)
    rep.add(Hydrogen_trp, "div", key, inp_nonccs_gas_tecs_wo_CCSRETRO)

    # L3346
    FE_Transport = emi(
        rep,
        techs("trp", "coal foil gas loil meth"),
        name="FE_Transport",
        filters=dict(r=["CO2_trp"]),
        unit_key="CO2 emissions",
    )

    # L3886
    k0 = Key.product(anon().name, FE_Transport, Biogas_trp)
    rep.add(k0, "sub", FE_Transport, Biogas_trp)

    k1 = Key.product("Transport", k0, Hydrogen_trp)
    rep.add(k1, "add", k0, Hydrogen_trp, sums=True)

    # TODO Identify where to sum on "h", "m", "yv" dimensions

    # Convert to IAMC format
    var = "Emissions|CO2|Energy|Demand|Transportation|Road Rail and Domestic Shipping"
    info = dict(variable="transport emissions", base=k1.drop("h", "m", "yv"), var=[var])
    iamc(rep, info)

    # TODO use store_ts()

    log.info(f"Added {len(rep.graph) - N} keys")
