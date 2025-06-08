"""Compatibility code that emulates legacy reporting."""

import logging
from collections.abc import Mapping
from functools import partial
from itertools import chain, count
from typing import TYPE_CHECKING, Any, Optional

from genno import Key, Quantity, quote
from genno.core.key import iter_keys, single_key

if TYPE_CHECKING:
    from genno import Computer
    from ixmp import Reporter
    from sdmx.model.common import Code

    from message_ix_models import Context

__all__ = [
    "TECH_FILTERS",
    "callback",
    "eff",
    "emi",
    "get_techs",
    "inp",
    "out",
    "prepare_techs",
]

log = logging.getLogger(__name__)

#: Filters for determining subsets of technologies.
#:
#: Each value is a Python expression :func:`eval`'d in an environment containing
#: variables derived from the annotations on :class:`Codes <.Code>` for each technology.
#: If the expression evaluates to :obj:`True`, then the code belongs to the set
#: identified by the key.
#:
#: See also
#: --------
#: get_techs
#: prepare_techs
#:
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


# Counter for anon()
_ANON = map(lambda n: Key(f"_{n}"), count())


def anon(name: Optional[str] = None, dims: Optional[Key] = None) -> Key:
    """Create an ‘anonymous’ :class:`.Key`, optionally with `dims` from another Key."""
    result = next(_ANON) if name is None else Key(name)

    return result.append(*getattr(dims, "dims", []))


def get_techs(c: "Computer", prefix: str, kinds: Optional[str] = None) -> list[str]:
    """Return a list of technologies.

    The list is assembled from lists in `c` with keys like "t::{prefix} {kind}",
    with one `kind` for each space-separated item in `kinds`. If no `kinds` are
    supplied, "t::{prefix}" is used.

    See also
    --------
    prepare_techs
    """
    _kinds = kinds.split() if kinds else [""]
    return list(chain(*[c.graph[f"t::{prefix} {k}".rstrip()][0].data for k in _kinds]))


def make_shorthand_function(
    base_name: str, to_drop: str, default_unit_key: Optional[str] = None
):
    """Create a shorthand function for adding tasks to a :class:`.Reporter`."""
    _to_drop = to_drop.split()

    def func(
        c: "Computer",
        technologies: list[str],
        *,
        name: Optional[str] = None,
        filters: Optional[dict] = None,
        unit_key: Optional[str] = default_unit_key,
    ) -> Key:
        f"""Select data from "{base_name}:*" and apply units.

        The returned key sums the result over the dimensions {_to_drop!r}.

        Parameters
        ----------
        technologies :
            List of technology IDs to include.
        name : str, optional
            If given, the name of the resulting key. Default: a name like "_123"
            generated with :func:`anon`.
        filters : dict, optional
            Additional filters for selecting data from "{base_name}:*". Keys are short
            dimension names (for instance, "c" for "commodity"); values are lists of
            IDs.
        unit_key : str, optional
            Key for units to apply to the result. Must appear in :attr:`.Config.units`.
        """
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
    c: "Computer",
    technologies: list[str],
    filters_in: Optional[dict] = None,
    filters_out: Optional[dict] = None,
) -> Key:
    """Throughput efficiency (input / output) for `technologies`.

    Equivalent to :meth:`PostProcess.eff`.

    Parameters
    ----------
    filters_in : dict, optional
        Passed as the `filters` parameter to :func:`inp`.
    filters_out : dict, optional
        Passed as the `filters` parameter to :func:`out`.
    """
    # TODO Check whether append / drop "t" is necessary
    num = c.graph.unsorted_key(inp(c, technologies, filters=filters_in).append("t"))
    denom = c.graph.unsorted_key(out(c, technologies, filters=filters_out).append("t"))
    assert isinstance(num, Key)
    assert isinstance(denom, Key)

    key = anon(dims=num)

    c.add(key, "div", num, denom, sums=True)

    return key.drop("t")


def pe_w_ccs_retro(
    c: "Computer",
    t: str,
    t_scrub: str,
    k_share: Optional[Key],
    filters: Optional[dict] = None,
) -> Key:
    """Calculate primary energy use of technologies with scrubbers.

    Equivalent to :func:`default_tables._pe_wCCS_retro` at L129.
    """
    ACT: Key = single_key(c.full_key("ACT"))

    k0 = out(c, [t_scrub])
    k1 = c.add(anon(), "mul", k0, k_share) if k_share else k0

    k2 = anon(dims=ACT).drop("t")
    c.add(k2, "select", ACT, indexers=dict(t=t), drop=True, sums=True)

    # TODO determine the dimensions to drop for the numerator
    k3, *_ = iter_keys(c.add(anon(dims=k2), "div", k2.drop("yv"), k2, sums=True))
    assert_dims(c, k3)

    filters_out = dict(c=["electr"], l=["secondary"])
    k4 = eff(c, [t], filters_in=filters, filters_out=filters_out)
    k5 = single_key(c.add(anon(), "mul", k3, k4))
    k6 = single_key(c.add(anon(dims=k5), "div", k1, k5))
    assert_dims(c, k6)

    return k6


def prepare_techs(c: "Computer", technologies: list["Code"]) -> None:
    """Prepare sets of technologies in `c`.

    For each `key` → `expr` in :data:`TECH_FILTERS` and each technology :class:`Code`
    `t` in `technologies`:

    - Apply the filter expression `expr` to information about `t`.
    - If the expression evaluates to :obj:`True`, add it to a list in `c` at "t::{key}".

    These lists of technologies can be used directly or retrieve with :func:`get_techs`.
    """
    result: Mapping[str, list[str]] = {k: list() for k in TECH_FILTERS}

    warned = set()  # Filters that raise some kind of Exception

    # Iterate over technologies
    for t in technologies:
        # Assemble information about `t` from its annotations
        info: dict[str, Any] = dict(id=t.id)
        try:
            # Sector
            info["sector"] = str(t.get_annotation(id="sector").text)
        except KeyError:  # No such annotation
            info["sector"] = None
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


def assert_dims(c: "Computer", *keys: Key):
    """Check the dimensions of `keys` for an "add", "sub", or "div" task.

    This is a sanity check needed because :py:`c.add("name", "div", …)` does not (yet)
    automatically infer the dimensions of the resulting key. This is in contrast to
    :py:`c.add("name", "mul", …)`, which *does* infer.

    Use this function after manual construction of a key for a "add", "div", or "sub"
    task, in order to ensure the key matches the dimensionality of the quantity that
    will result from the task.

    .. deprecated:: 2025-02-17
       Handled upstream in :func:`genno.operator.add_binop` with genno ≥1.20.
    """
    from warnings import warn

    warn(
        "message-ix-models.report.compat.assert_dims()",
        DeprecationWarning,
        stacklevel=2,
    )

    for key in keys:
        task = c.graph[key]
        expected = Key.product("foo", *task[1:])

        op = f" {task[0].__name__} "
        assert set(key.dims) == set(expected.dims), (
            f"Task should produce {op.join(repr(k) for k in task[1:])} = "
            f"{str(expected).split(':')[1]}; key indicates {str(key).split(':')[1]}"
        )


def callback(rep: "Reporter", context: "Context") -> None:
    """Partially duplicate the behaviour of :func:`.default_tables.retr_CO2emi`.

    Currently, this prepares the following keys and the necessary preceding
    calculations:

    - "transport emissions full::iamc": data for the IAMC variable
      "Emissions|CO2|Energy|Demand|Transportation|Road Rail and Domestic Shipping"
    """
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

    # L3346
    # "FE_Transport": CO₂ emissions from all transportation technologies directly using
    # fossil fuels.
    FE_Transport = emi(
        rep,
        techs("trp", "coal foil gas loil meth"),
        name="FE_Transport",
        filters=dict(r=["CO2_trp"]),
        unit_key="CO2 emissions",
    )

    # L3886
    # "Transport": CO₂ emissions from transport. "FE_Transport" minus emissions saved by
    # use of biogas in transport, plus emissions from production of hydrogen used in
    # transport.
    k0 = rep.add(anon(dims=FE_Transport), "sub", FE_Transport, full("Biogas_trp"))
    k1, *_ = iter_keys(
        rep.add(Key("Transport", k0.dims), "add", k0, full("Hydrogen_trp"), sums=True)
    )
    assert_dims(rep, k0, k1)

    # TODO Identify where to sum on "h", "m", "yv" dimensions

    # Convert to IAMC structure
    var = "Emissions|CO2|Energy|Demand|Transportation|Road Rail and Domestic Shipping"
    info = dict(variable="transport emissions", base=k1.drop("h", "m", "yv"), var=[var])
    iamc(rep, info)

    # TODO use store_ts() to store on scenario

    log.info(f"Added {len(rep.graph) - N} keys")
