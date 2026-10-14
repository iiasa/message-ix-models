"""Additive, pooled resource-origin estimates from observed IAMC flows.

These are production-share estimates, not traced molecules or bilateral suppliers.
Canonical producer output pools exclude captive hydrogen. Exports and implied
regional bunkers use local production shares; imports use regional-world production
shares. Domestic consumers use normalized P - E + I (bunkers are NOT subtracted).
Unreported whole leaf-years are structural zeros only for globally present leaves;
internal regional holes in required pool/trade observations are errors. Consumers
retain their observed coordinates, including sparse historical reporting.
"""

import numpy as np
import pandas as pd

ORIGIN_PREFIX: str = "Estimate|Origin|"
INDIRECT_PREFIX: str = ORIGIN_PREFIX + "Non-Additive|"
IAMC_COLUMNS: list[str] = [
    "model",
    "scenario",
    "region",
    "variable",
    "unit",
    "year",
    "value",
]
TARGET_COLUMNS: list[str] = [
    "model",
    "scenario",
    "region",
    "carrier",
    "kind",
    "use",
    "unit",
    "year",
    "value",
]
COORDINATES: list[str] = ["model", "scenario", "region", "year"]
GLOBAL_REGIONS: frozenset[str] = frozenset(
    {"World", "GLB", "R12_GLB", "GLB region (R12)"}
)
NH3_MT_PER_EJ: float = 53.220035
PRODUCER_ROUTES: dict[str, tuple[str, ...]] = {
    "Hydrogen": (
        "h2_smr",
        "h2_smr_ccs",
        "h2_coal",
        "h2_coal_ccs",
        "h2_bio",
        "h2_bio_ccs",
        "h2_elec_alk",
        "h2_elec_pem",
        "h2_elec_soe",
        "h2_pyro_elec",
    ),
    "Methanol": (
        "meth_bio",
        "meth_bio_ccs",
        "meth_coal",
        "meth_coal_ccs",
        "meth_ng",
        "meth_ng_ccs",
        "meth_h2",
    ),
    "Ammonia": (
        "biomass_NH3",
        "biomass_NH3_ccs",
        "coal_NH3",
        "coal_NH3_ccs",
        "electr_NH3",
        "fueloil_NH3",
        "fueloil_NH3_ccs",
        "gas_NH3",
        "gas_NH3_ccs",
    ),
}
POOL_UNITS: dict[str, str] = {
    "Hydrogen": "EJ/yr",
    "Methanol": "EJ/yr",
    "Ammonia": "Mt/yr",
}
PRODUCER_LEAVES: dict[str, tuple[str, ...]] = {
    carrier: tuple(f"out|{tech}|{commodity}" for tech in routes)
    for carrier, routes, commodity in (
        ("Hydrogen", PRODUCER_ROUTES["Hydrogen"], "hydrogen"),
        ("Methanol", PRODUCER_ROUTES["Methanol"], "methanol"),
        ("Ammonia", PRODUCER_ROUTES["Ammonia"], "NH3"),
    )
}
CONSUMER_MAP: dict[str, dict[str, tuple[str, ...]]] = {
    "Hydrogen": {
        **{
            tech: (f"in|Industry|Other|{tech}|hydrogen",)
            for tech in (
                *[
                    f"furnace_h2_{s}"
                    for s in (
                        "steel",
                        "cement",
                        "aluminum",
                        "petro",
                        "resins",
                        "refining",
                    )
                ],
                *[
                    f"fc_h2_{s}"
                    for s in ("steel", "aluminum", "petro", "resins", "refining")
                ],
                "h2_fc_I",
                "h2_i",
            )
        },
        "dri_h2_steel": ("in|Industry|Iron and Steel|dri_h2_steel|hydrogen",),
        "h2_liq": ("in|Industry|Chemicals|h2_liq|hydrogen",),
        "meth_h2": tuple(
            f"in|Industry|Chemicals|meth_h2|{source}_CO2|hydrogen"
            for source in ("BIC", "DAC", "FIC")
        ),
        "h2_ct": ("in|Power|h2_ct|hydrogen",),
        **{
            tech: (f"in|Transportation|{sector}|{tech}|hydrogen",)
            for tech, sector in (
                ("conh_ar", "Aviation"),
                ("FC_bus", "Bus"),
                ("HFC_ptrp", "Light-Duty Vehicle"),
                ("FR_FCH", "Truck"),
            )
        },
    },
    "Methanol": {
        **{
            f"furnace_methanol_{s}": (
                f"in|Industry|Other|furnace_methanol_{s}|methanol",
            )
            for s in ("steel", "cement", "aluminum", "petro", "resins", "refining")
        },
        **{
            tech: (f"in|Transportation|{sector}|{tech}|methanol",)
            for tech, sector in (
                ("FCm_bus", "Bus"),
                ("IAHm_ptrp", "Light-Duty Vehicle"),
                ("ICAm_ptrp", "Light-Duty Vehicle"),
                ("conm_ar", "Aviation"),
                ("f road methanol", "Truck"),
            )
        },
        "MTO_petro": ("in|Industry|Chemicals|MTO_petro|methanol",),
        "CH2O_synth": ("in|Industry|Chemicals|CH2O_synth|methanol",),
        "meth_ind_fs": ("in|Chemicals|meth_ind_fs|methanol feedstock material",),
    },
    "Ammonia": {
        "NH3_to_N_fertil": ("in|Industry|Chemicals|NH3_to_N_fertil|NH3",),
        "residual_NH3": ("in|Chemicals|residual_NH3|ammonia",),
    },
}
CONSUMER_UNITS: dict[str, dict[str, str]] = {
    carrier: {
        tech: (
            "Mt/yr"
            if carrier == "Ammonia"
            or tech in {"MTO_petro", "CH2O_synth", "meth_ind_fs"}
            else "EJ/yr"
        )
        for tech in consumers
    }
    for carrier, consumers in CONSUMER_MAP.items()
}
TRADE_LEAVES: dict[str, dict[str, str]] = {
    "Hydrogen": dict(
        Exports="out|lh2_exp",
        Imports="out|lh2_imp",
        Net="Trade|Secondary Energy|Hydrogen",
        Bunkers="in|Shipping|LH2_bunker|hydrogen",
    ),
    "Methanol": dict(
        Exports="out|meth_exp",
        Imports="out|meth_imp",
        Net="Trade|Goods|Methanol",
        Bunkers="in|Shipping|meth_bunker|methanol",
    ),
    "Ammonia": dict(
        Exports="out|export_NH3",
        Imports="out|import_NH3",
        Net="Trade|Goods|Ammonia",
        Bunkers="in|Shipping|NH3_bunker|ammonia",
    ),
}
CAPTIVE_PRODUCER: str = "out|catalytic_reforming_ref|hydrogen"
CAPTIVE_CONSUMER: str = "in|Industry|Refining|hydro_cracking_ref|hydrogen"
HVC_LEAVES: dict[str, tuple[str, ...]] = {
    "ethylene": (
        "in|Chemicals|production_HVC|ethylene",
        "out|MTO_petro|ethylene",
        "out|steam_cracker_petro|ethylene",
        "out|ethanol_to_ethylene_petro|ethylene",
    ),
    "propylene": (
        "in|Industry|Chemicals|production_HVC|propylene",
        "out|MTO_petro|propylene",
        "out|steam_cracker_petro|propylene",
    ),
}


def _validate(frame: pd.DataFrame, estimates_allowed: bool = False) -> pd.DataFrame:
    expected = set(IAMC_COLUMNS)
    if set(frame.columns) not in (expected, expected | {"subannual"}):
        raise ValueError(
            f"Expected annual IAMC columns {IAMC_COLUMNS}, optionally subannual=Year"
        )
    if "subannual" in frame and not frame.subannual.eq("Year").all():
        raise ValueError("Only subannual=Year is supported")
    if frame.drop(columns="value").isna().any().any():
        raise ValueError("Missing IAMC key")
    if frame.duplicated(IAMC_COLUMNS[:-1]).any():
        raise ValueError("Duplicate IAMC key")
    if not np.isfinite(frame.value.to_numpy(dtype=float)).all():
        raise ValueError("Nonfinite IAMC value")
    years = pd.to_numeric(frame.year, errors="raise").to_numpy(dtype=float)
    if not np.isfinite(years).all() or (years != np.floor(years)).any():
        raise ValueError("Expected integer annual years")
    estimated = frame.variable.str.startswith(ORIGIN_PREFIX)
    if estimated.any() and not estimates_allowed:
        raise ValueError("Existing origin estimates are not accepted")
    for legs in TRADE_LEAVES.values():
        bunker = frame.loc[frame.variable == legs["Bunkers"]]
        if bunker.empty:
            raise ValueError(f"Missing required observable leaf: {legs['Bunkers']}")
        if not bunker.unit.eq("EJ/yr").all() or (bunker.value < 0).any():
            raise ValueError(f"Invalid native bunker observation: {legs['Bunkers']}")
    return frame.loc[~estimated & ~frame.region.isin(GLOBAL_REGIONS)]


class _Observations:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.index = pd.MultiIndex.from_frame(
            frame[COORDINATES].drop_duplicates()
        ).sort_values()
        self.regions = frame.groupby(["model", "scenario"]).region.unique().to_dict()

    def leaf(
        self, name: str, unit: str, *, required: bool = True, complete: bool = True
    ) -> pd.Series:
        rows = self.frame.loc[self.frame.variable == name]
        if rows.empty:
            if required:
                raise ValueError(f"Missing required observable leaf: {name}")
            return pd.Series(dtype=float, index=self.index[:0])
        if not rows.unit.eq(unit).all():
            raise ValueError(f"Unexpected unit for {name}: expected {unit}")
        if rows.duplicated(COORDINATES).any():
            raise ValueError(f"Ambiguous observable coordinates: {name}")
        present_pairs = set(
            rows[["model", "scenario"]].itertuples(index=False, name=None)
        )
        if required and present_pairs != set(self.regions):
            raise ValueError(f"Missing required observable model/scenario: {name}")
        if complete:
            for (model, scenario, year), group in rows.groupby(
                ["model", "scenario", "year"]
            ):
                if set(group.region) != set(self.regions[model, scenario]):
                    raise ValueError(
                        f"Internal regional hole in {name}, {model}/{scenario}/{year}"
                    )
        values = rows.set_index(COORDINATES).value.astype(float)
        if (values < 0).any() and name not in {x["Net"] for x in TRADE_LEAVES.values()}:
            raise ValueError(f"Negative positive-flow observation: {name}")
        return values

    def pool(self, name: str, unit: str) -> pd.Series:
        # Only whole unsupported leaf-years become zero; leaf() rejects internal holes.
        return self.leaf(name, unit).reindex(self.index, fill_value=0.0)


def _shares(pool: pd.DataFrame, target: pd.Series, label: str) -> pd.DataFrame:
    selected = pool.reindex(target.index)
    active = target.ne(0)
    if (target < 0).any():
        raise ValueError(f"Negative positive-flow target: {label}")
    denominator = selected.sum(axis=1)
    if (
        (selected.lt(0).any(axis=1) | denominator.le(0) | ~np.isfinite(denominator))
        & active
    ).any():
        raise ValueError(
            f"Unusable or negative route availability for positive {label}"
        )
    result = selected * 0.0
    result.loc[active] = (
        selected.loc[active]
        .div(denominator.loc[active], axis=0)
        .mul(target.loc[active], axis=0)
    )
    if not np.isfinite(result.to_numpy()).all():
        raise ValueError(f"Nonfinite allocation: {label}")
    return result


def _observed_targets(obs: _Observations) -> list[tuple[str, str, str, str, pd.Series]]:
    targets: list[tuple[str, str, str, str, pd.Series]] = []

    def add(carrier: str, kind: str, use: str, unit: str, values: pd.Series) -> None:
        if not values.empty:
            targets.append((carrier, kind, use, unit, values))

    for carrier, consumers in CONSUMER_MAP.items():
        for tech, leaves in consumers.items():
            unit = CONSUMER_UNITS[carrier][tech]
            parts = [
                obs.leaf(leaf, unit, required=False, complete=False) for leaf in leaves
            ]
            if (
                len(leaves) > 1
                and any(p.empty for p in parts)
                and not all(p.empty for p in parts)
            ):
                raise ValueError(f"Missing partition of consumer {tech}")
            values = (
                pd.concat(parts, axis=1).sum(axis=1)
                if any(not p.empty for p in parts)
                else parts[0]
            )
            add(carrier, "Use", tech, unit, values)
        unit = POOL_UNITS[carrier]
        legs = TRADE_LEAVES[carrier]
        export = obs.pool(legs["Exports"], unit)
        imports = obs.pool(legs["Imports"], unit)
        net = obs.leaf(legs["Net"], unit)
        # Published net trade defines the joint trade-year support. A gross leg
        # absent for an entire leaf-year is its validated structural zero, so
        # all four allocated trade legs still reconcile on those coordinates.
        add(carrier, "Trade", "Exports", unit, export.reindex(net.index))
        add(carrier, "Trade", "Imports", unit, imports.reindex(net.index))
        add(carrier, "Trade", "Net", unit, net)
        add(
            carrier,
            "Trade",
            "Bunkers",
            unit,
            export.reindex(net.index) - imports.reindex(net.index) - net,
        )
    captive = obs.leaf(CAPTIVE_CONSUMER, "EJ/yr", required=False, complete=False)
    add("Hydrogen Captive", "Use", "hydro_cracking_ref", "EJ/yr", captive)
    components = []
    for olefin, leaves in HVC_LEAVES.items():
        use = obs.leaf(leaves[0], "Mt/yr", complete=False)
        mto = obs.pool(leaves[1], "Mt/yr").reindex(use.index)
        total = sum((obs.pool(leaf, "Mt/yr").reindex(use.index) for leaf in leaves[1:]))
        if ((total <= 0) & use.ne(0)).any():
            raise ValueError(f"Unusable olefin production denominator: {olefin}")
        contribution = use * 0.0
        active = use.ne(0)
        contribution.loc[active] = use.loc[active] * mto.loc[active] / total.loc[active]
        components.append(contribution)
    add(
        "Methanol",
        "Non-Additive",
        "HVC via MTO",
        "Mt/yr",
        pd.concat(components, axis=1).sum(axis=1),
    )
    return targets


def origin_targets(frame: pd.DataFrame) -> pd.DataFrame:
    """Return observed regional target coordinates (no route dimension).

    Persisted estimates are ignored, never used to derive targets. Native global
    leaves are excluded, especially shipping bunker inputs. Regional B is the
    published implied E-I-Net, not a reweighted global bunker. If and only if the
    input contains World, append World totals by summing regional targets. HVC is
    kind Non-Additive/use HVC via MTO: sum of olefin use times regional MTO output
    fraction, NOT the fraction of globally aggregated olefin production.
    Optional wholly unreported consumers are omitted, not zero-manufactured.
    """
    obs = _Observations(_validate(frame, estimates_allowed=True))
    chunks = []
    for carrier, kind, use, unit, values in _observed_targets(obs):
        chunk = (
            values.rename("value")
            .reset_index()
            .assign(carrier=carrier, kind=kind, use=use, unit=unit)
        )
        chunks.append(chunk[TARGET_COLUMNS])
    result = (
        pd.concat(chunks, ignore_index=True)
        if chunks
        else pd.DataFrame(columns=TARGET_COLUMNS)
    )
    if frame.region.eq("World").any() and not result.empty:
        keys = [c for c in TARGET_COLUMNS if c not in {"region", "value"}]
        world = (
            result.groupby(keys, as_index=False, sort=False)
            .value.sum()
            .assign(region="World")
        )
        result = pd.concat([result, world[TARGET_COLUMNS]], ignore_index=True)
    if not np.isfinite(result.value.to_numpy(dtype=float)).all():
        raise ValueError("Nonfinite origin target")
    return result


def origin_estimates(frame: pd.DataFrame) -> pd.DataFrame:
    """Return NEW annual IAMC rows only; never modify or include input rows.

    All routes are emitted, even zero routes, at each observed target coordinate.
    Positive targets require usable nonnegative route pools; zero targets may
    emit zero routes without claiming a witnessed share. Net routes are signed
    Eroute-Iroute-Broute. Captive hydrogen is allocated 100% to its separate
    producer when available, without requiring production to equal consumption.
    World aggregation belongs to the caller after this calculation.
    """
    obs = _Observations(_validate(frame))
    exports, imports, bunkers, domestic = {}, {}, {}, {}
    for carrier, routes in PRODUCER_ROUTES.items():
        unit = POOL_UNITS[carrier]
        pool = pd.concat(
            [obs.pool(leaf, unit) for leaf in PRODUCER_LEAVES[carrier]], axis=1
        )
        pool.columns = routes
        global_pool = pool.groupby(level=["model", "scenario", "year"]).transform("sum")
        legs = TRADE_LEAVES[carrier]
        e = obs.pool(legs["Exports"], unit)
        i = obs.pool(legs["Imports"], unit)
        net = obs.leaf(legs["Net"], unit)
        b = e.reindex(net.index) - i.reindex(net.index) - net
        exports[carrier] = _shares(pool, e, f"{carrier} exports")
        imports[carrier] = _shares(global_pool, i, f"{carrier} imports")
        bunkers[carrier] = _shares(pool, b, f"{carrier} bunkers")
        domestic[carrier] = pool - exports[carrier] + imports[carrier]
    chunks = []
    for carrier, kind, use, unit, target in _observed_targets(obs):
        if carrier == "Hydrogen Captive":
            producer = obs.pool(CAPTIVE_PRODUCER, "EJ/yr").to_frame(
                "catalytic_reforming_ref"
            )
            allocated = _shares(producer, target, "captive hydrogen")
        elif kind == "Trade":
            if use == "Net":
                allocated = (
                    exports[carrier].reindex(target.index)
                    - imports[carrier].reindex(target.index)
                    - bunkers[carrier].reindex(target.index)
                )
            else:
                allocated = {
                    "Exports": exports,
                    "Imports": imports,
                    "Bunkers": bunkers,
                }[use][carrier].reindex(target.index)
        else:
            allocated = _shares(domestic[carrier], target, f"{carrier} {use}")
        prefix = (
            f"{INDIRECT_PREFIX}{carrier}|{use}|"
            if kind == "Non-Additive"
            else f"{ORIGIN_PREFIX}{carrier}|{kind}|{use}|"
        )
        for route in allocated.columns:
            chunk = (
                allocated[route]
                .rename("value")
                .reset_index()
                .assign(variable=prefix + route, unit=unit)
            )
            chunks.append(chunk[IAMC_COLUMNS])
    result = (
        pd.concat(chunks, ignore_index=True)
        if chunks
        else pd.DataFrame(columns=IAMC_COLUMNS)
    )
    if not np.isfinite(result.value.to_numpy(dtype=float)).all():
        raise ValueError("Nonfinite origin estimate")
    if "subannual" in frame:
        result["subannual"] = "Year"
    return result.reindex(columns=frame.columns)
