"""Numerical invariants for pooled origin attribution; no reporting wiring."""

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from message_ix_models.report.hydrogen.origin import (
    CAPTIVE_CONSUMER,
    CAPTIVE_PRODUCER,
    CONSUMER_MAP,
    CONSUMER_UNITS,
    HVC_LEAVES,
    IAMC_COLUMNS,
    INDIRECT_PREFIX,
    ORIGIN_PREFIX,
    POOL_UNITS,
    PRODUCER_LEAVES,
    PRODUCER_ROUTES,
    TRADE_LEAVES,
    origin_estimates,
    origin_targets,
)


@pytest.fixture
def flows() -> pd.DataFrame:
    rows: list[tuple[str, str, str, str, str, int, float]] = []

    def add(variable: str, unit: str, a: float, b: float) -> None:
        rows.extend(
            ("model", "scenario", region, variable, unit, year, value)
            for year in (1990, 2050)
            for region, value in (("A", a), ("B", b))
        )

    for carrier, leaves in PRODUCER_LEAVES.items():
        for i, leaf in enumerate(leaves):
            add(
                leaf,
                POOL_UNITS[carrier],
                8 if i == 0 else 2 if i == 1 else 0,
                2 if i == 0 else 8 if i == 1 else 0,
            )
        legs = TRADE_LEAVES[carrier]
        for leg, values in (("Exports", (2, 0)), ("Imports", (0, 2)), ("Net", (1, -2))):
            add(legs[leg], POOL_UNITS[carrier], *values)
        # An arbitrarily large native global pool must not affect regional mixes.
        for year in (1990, 2050):
            rows.append(
                ("model", "scenario", "R12_GLB", legs["Bunkers"], "EJ/yr", year, 1000)
            )
            for leaf in leaves:
                rows.append(
                    (
                        "model",
                        "scenario",
                        "R12_GLB",
                        leaf,
                        POOL_UNITS[carrier],
                        year,
                        9999,
                    )
                )
        for tech, consumers in CONSUMER_MAP[carrier].items():
            for leaf in consumers:
                add(
                    leaf,
                    CONSUMER_UNITS[carrier][tech],
                    4 / len(consumers),
                    4 / len(consumers),
                )
    add(CAPTIVE_PRODUCER, "EJ/yr", 9, 9)
    add(CAPTIVE_CONSUMER, "EJ/yr", 3, 3)
    for olefin, leaves in HVC_LEAVES.items():
        olefin_values = (10, 2, 2, 4) if olefin == "ethylene" else (4, 1, 3)
        for leaf, value in zip(leaves, olefin_values):
            add(leaf, "Mt/yr", value, value)
    return pd.DataFrame(rows, columns=IAMC_COLUMNS)


def summed_estimates(estimates: pd.DataFrame) -> pd.DataFrame:
    parsed = []
    for row in estimates.itertuples(index=False):
        suffix = row.variable[len(ORIGIN_PREFIX) :].split("|")
        if suffix[0] == "Non-Additive":
            _, carrier, use, route = suffix
            kind = "Non-Additive"
        else:
            carrier, kind, use, route = suffix
        parsed.append(
            (
                row.model,
                row.scenario,
                row.region,
                carrier,
                kind,
                use,
                row.unit,
                row.year,
                row.value,
            )
        )
    columns = [
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
    return (
        pd.DataFrame(parsed, columns=columns)
        .groupby(columns[:-1], as_index=False)
        .value.sum()
    )


def test_route_sums_mixes_and_input_preservation(flows: pd.DataFrame) -> None:
    before = flows.copy(deep=True)
    estimates = origin_estimates(flows)
    assert_frame_equal(flows, before)
    assert not estimates.duplicated(IAMC_COLUMNS[:-1]).any()
    assert np.isfinite(estimates.value).all()
    keys = ["model", "scenario", "region", "carrier", "kind", "use", "unit", "year"]
    totals = summed_estimates(estimates).sort_values(keys).reset_index(drop=True)
    targets = origin_targets(flows).sort_values(keys).reset_index(drop=True)
    assert_frame_equal(totals, targets, check_exact=False, atol=1e-12, rtol=1e-12)
    first = PRODUCER_ROUTES["Hydrogen"][0]
    use = estimates.loc[
        estimates.variable.eq(f"{ORIGIN_PREFIX}Hydrogen|Use|h2_ct|{first}")
    ]
    assert use.loc[use.region.eq("A"), "value"].tolist() == pytest.approx([3.2, 3.2])
    assert use.loc[use.region.eq("B"), "value"].tolist() == pytest.approx([1, 1])
    net = estimates.loc[
        estimates.variable.eq(f"{ORIGIN_PREFIX}Hydrogen|Trade|Net|{first}")
    ]
    assert net.loc[net.region.eq("A"), "value"].tolist() == pytest.approx([0.8, 0.8])
    assert net.loc[net.region.eq("B"), "value"].tolist() == pytest.approx([-1, -1])
    captive = estimates.loc[
        estimates.variable.str.contains("Hydrogen Captive", regex=False)
    ]
    assert captive.value.tolist() == [3, 3, 3, 3]
    hvc = targets.loc[targets.kind.eq("Non-Additive")]
    assert hvc.value.tolist() == [3.5, 3.5, 3.5, 3.5]
    assert estimates.variable.str.startswith(INDIRECT_PREFIX).any()
    assert not estimates.region.isin(["World", "R12_GLB"]).any()


def test_persisted_targets_and_world_sum(flows: pd.DataFrame) -> None:
    estimates = origin_estimates(flows)
    # World production is intentionally misleading; targets must sum regions.
    world = flows.loc[flows.region.eq("A")].assign(region="World", value=99999)
    persisted = pd.concat([flows, world, estimates], ignore_index=True)
    targets = origin_targets(persisted)
    keys = ["model", "scenario", "carrier", "kind", "use", "unit", "year"]
    regional = targets.loc[targets.region.ne("World")].groupby(keys).value.sum()
    global_values = targets.loc[targets.region.eq("World")].set_index(keys).value
    pd.testing.assert_series_equal(regional.sort_index(), global_values.sort_index())
    with pytest.raises(ValueError, match="Existing origin"):
        origin_estimates(persisted)


def test_zero_activity_and_unobserved_year_support(flows: pd.DataFrame) -> None:
    zero = flows.assign(value=0.0)
    assert origin_estimates(zero).value.eq(0).all()
    # Missing whole historical producer/trade leaf-years represent structural zero.
    remove = flows.year.eq(1990) & flows.variable.isin(
        [*PRODUCER_LEAVES["Hydrogen"], *TRADE_LEAVES["Hydrogen"].values()]
    )
    sparse = zero.loc[~remove].copy()
    estimates = origin_estimates(sparse)
    observed = estimates.variable.eq(f"{ORIGIN_PREFIX}Hydrogen|Use|h2_ct|h2_smr")
    assert set(estimates.loc[observed, "year"]) == {1990, 2050}
    assert not (
        (estimates.year == 1990)
        & estimates.variable.str.contains("Hydrogen|Trade", regex=False)
    ).any()


def test_consumer_coordinates_are_not_cartesian(flows: pd.DataFrame) -> None:
    leaf = CONSUMER_MAP["Hydrogen"]["h2_ct"][0]
    sparse = flows.loc[
        ~(flows.variable.eq(leaf) & flows.region.eq("B") & flows.year.eq(1990))
    ]
    targets = origin_targets(sparse)
    assert len(targets.loc[targets.use.eq("h2_ct")]) == 3
    absent = flows.loc[~flows.variable.eq(leaf)]
    assert not origin_targets(absent).use.eq("h2_ct").any()
    assert (
        not origin_estimates(absent).variable.str.contains("|h2_ct|", regex=False).any()
    )


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "hole",
        "nonfinite",
        "duplicate",
        "negative",
        "denominator",
        "availability",
        "olefin",
    ],
)
def test_invalid_observations_fail(flows: pd.DataFrame, failure: str) -> None:
    frame = flows.copy()
    producer = PRODUCER_LEAVES["Hydrogen"][0]
    if failure == "missing":
        frame = frame.loc[~frame.variable.eq(producer)]
        match = "Missing required"
    elif failure == "hole":
        frame = frame.loc[
            ~(frame.variable.eq(producer) & frame.region.eq("A") & frame.year.eq(1990))
        ]
        match = "Internal regional hole"
    elif failure == "nonfinite":
        frame.loc[0, "value"] = np.inf
        match = "Nonfinite"
    elif failure == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
        match = "Duplicate"
    elif failure == "negative":
        frame.loc[0, "value"] = -1
        match = "Negative"
    elif failure == "denominator":
        frame.loc[frame.variable.isin(PRODUCER_LEAVES["Hydrogen"]), "value"] = 0
        match = "Unusable"
    elif failure == "availability":
        frame.loc[
            frame.variable.eq(TRADE_LEAVES["Hydrogen"]["Exports"])
            & frame.region.eq("A"),
            "value",
        ] = 20
        # Keep nonnegative implied B while forcing negative domestic availability.
        frame.loc[
            frame.variable.eq(TRADE_LEAVES["Hydrogen"]["Net"]) & frame.region.eq("A"),
            "value",
        ] = 19
        match = "negative route availability"
    else:
        frame.loc[frame.variable.isin(HVC_LEAVES["ethylene"][1:]), "value"] = 0
        match = "olefin production denominator"
    with pytest.raises(ValueError, match=match):
        origin_estimates(frame)


def test_optional_annual_dimension(flows):
    annual = flows.assign(subannual="Year")
    estimates = origin_estimates(annual)
    assert estimates.columns.tolist() == annual.columns.tolist()
    assert estimates.subannual.eq("Year").all()
    assert_frame_equal(origin_targets(annual), origin_targets(flows))
    with pytest.raises(ValueError, match="Only subannual=Year"):
        origin_estimates(flows.assign(subannual="Summer"))
