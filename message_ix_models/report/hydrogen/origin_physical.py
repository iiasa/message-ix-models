"""Physical-premise checks for EFC proportional resource-origin estimates.

These checks use model coefficients, not the reporter's original_unit declaration.
The audited carrier basis is GWa for H2/LH2/methanol producers and Mt for NH3.
Consumer estimates retain their own observed units; shares are dimensionless.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from message_ix import Scenario

# Actual baseline-v1 coordinate spelling and coefficients, inspected 2026-10-13.
# Losses are already present in ACT * input; none is multiplied into IAMC again.
BRIDGES: tuple[tuple[str, str, str, str, str, float, float, str], ...] = (
    ("h2_liq", "hydrogen", "secondary", "lh2", "primary", 1.0, 1.0, "M1"),
    ("lh2_bal", "lh2", "primary", "lh2", "secondary", 1.0, 1.0, "M1"),
    ("lh2_regas", "lh2", "secondary", "hydrogen", "secondary", 1.0, 1.0, "M1"),
    ("lh2_exp", "lh2", "primary", "lh2", "export", 1.0, 1.0, "M1"),
    ("lh2_imp", "lh2", "import", "lh2", "secondary", 1.0, 1.0, "M1"),
    ("lh2_t_d", "lh2", "secondary", "lh2", "final", 1.0 / 0.95, 1.0, "M1"),
    ("LH2_tobunker", "lh2", "export", "lh2", "final", 1.0, 1.0, "M1"),
    ("h2_t_d", "hydrogen", "secondary", "hydrogen", "final", 1.0 / 0.85, 1.0, "M1"),
    ("meth_bal", "methanol", "primary", "methanol", "secondary", 1.0, 1.0, "fuel"),
    (
        "meth_bal",
        "methanol",
        "primary_material",
        "methanol",
        "secondary_material",
        1.0,
        1.0,
        "feedstock",
    ),
    ("meth_t_d", "methanol", "secondary", "methanol", "final", 1.0, 1.0, "fuel"),
    (
        "meth_t_d",
        "methanol",
        "secondary_material",
        "methanol",
        "final_material",
        0.697615423642821,
        1.0,
        "feedstock",
    ),
    ("meth_tobunker", "methanol", "export", "methanol", "final", 1.0, 1.0, "M1"),
    ("NH3_tobunker", "NH3", "export", "NH3", "final", 1.6785598921276463, 1.0, "M1"),
)


def _select(
    frame: pd.DataFrame,
    technology: str,
    commodity: str,
    level: str,
    mode: str | None = None,
) -> pd.DataFrame:
    mask = (
        (frame["technology"] == technology)
        & (frame["commodity"] == commodity)
        & (frame["level"] == level)
    )
    if mode is not None:
        mask &= frame["mode"] == mode
    result = frame.loc[mask]
    if result.empty:
        raise ValueError(
            f"Origin physical basis missing {technology}/{commodity}@{level}/{mode}"
        )
    return result


def _coefficient(frame: pd.DataFrame, expected: float, label: str) -> None:
    if not np.isfinite(frame["value"]).all() or not np.allclose(
        frame["value"], expected, rtol=1e-12, atol=0.0
    ):
        raise ValueError(
            f"Origin physical coefficient changed for {label}; expected {expected}, "
            f"found {sorted(frame['value'].unique())}"
        )


def _validate_producers(outputs: pd.DataFrame) -> None:
    """Check canonical producer units and correlated modes before allocation."""
    from .origin import PRODUCER_ROUTES

    for carrier, routes in PRODUCER_ROUTES.items():
        commodity, levels = {
            "Hydrogen": ("hydrogen", {"secondary"}),
            "Methanol": ("methanol", {"primary", "primary_material"}),
            "Ammonia": ("NH3", {"secondary_material"}),
        }[carrier]
        for technology in routes:
            production = outputs[
                (outputs["technology"] == technology)
                & (outputs["commodity"] == commodity)
            ]
            if production.empty or set(production["level"]) != levels:
                raise ValueError(
                    f"Unexpected origin producer pool: {technology}/{commodity}, "
                    f"levels={sorted(production['level'].unique())}"
                )
            if carrier == "Methanol":
                allowed = (
                    {
                        "fuel_bic",
                        "fuel_dac",
                        "fuel_fic",
                        "feedstock_bic",
                        "feedstock_dac",
                        "feedstock_fic",
                    }
                    if technology == "meth_h2"
                    else {"fuel", "feedstock"}
                )
                pairs = {
                    (
                        mode,
                        "primary_material"
                        if mode.startswith("feedstock")
                        else "primary",
                    )
                    for mode in allowed
                }
                if set(zip(production["mode"], production["level"])) != pairs:
                    raise ValueError(
                        f"Unexpected correlated methanol mode/level pool: {technology}"
                    )
            elif carrier == "Ammonia":
                if set(production["mode"]) != {"M1"}:
                    raise ValueError(f"Unexpected ammonia producer mode: {technology}")
            else:
                allowed = (
                    {
                        "M1",
                        "fuel_bic",
                        "fuel_dac",
                        "fuel_fic",
                        "feedstock_bic",
                        "feedstock_dac",
                        "feedstock_fic",
                    }
                    if technology.startswith("h2_elec_")
                    else {"M1"}
                )
                if not set(production["mode"]) <= allowed:
                    raise ValueError(f"Unexpected hydrogen producer mode: {technology}")
            if (
                not np.isfinite(production["value"]).all()
                or (production["value"] <= 0).any()
            ):
                raise ValueError(f"Invalid physical producer coefficient: {technology}")


def validate_origin_coefficients(inputs: pd.DataFrame, outputs: pd.DataFrame) -> None:
    """Fail on a physical pool/bridge outside the inspected EFC contract."""
    from .origin import PRODUCER_ROUTES

    for technology, ci, li, co, lo, vi, vo, mode in BRIDGES:
        _coefficient(
            _select(inputs, technology, ci, li, mode), vi, f"{technology} input"
        )
        _coefficient(
            _select(outputs, technology, co, lo, mode), vo, f"{technology} output"
        )
    _validate_producers(outputs)
    expected_pools = (
        ("hydrogen", {"secondary"}, set(PRODUCER_ROUTES["Hydrogen"]) | {"lh2_regas"}),
        ("methanol", {"primary", "primary_material"}, set(PRODUCER_ROUTES["Methanol"])),
        (
            "NH3",
            {"secondary_material"},
            set(PRODUCER_ROUTES["Ammonia"]) | {"import_NH3"},
        ),
    )
    for commodity, levels, technologies in expected_pools:
        actual = outputs[
            (outputs["commodity"] == commodity) & outputs["level"].isin(levels)
        ]
        if set(actual["technology"]) != technologies:
            raise ValueError(
                f"Unexpected {commodity} source pool: "
                f"{sorted(actual['technology'].unique())}"
            )
    # Do not infer captive production == use: the commodity inequality can leave a
    # surplus. Only its identity/pool separation is asserted, not global closure.
    captive_out = outputs[
        (outputs["commodity"] == "hydrogen") & (outputs["level"] == "refinery")
    ]
    captive_in = inputs[
        (inputs["commodity"] == "hydrogen") & (inputs["level"] == "refinery")
    ]
    if set(captive_out["technology"]) != {"catalytic_reforming_ref"} or set(
        captive_in["technology"]
    ) != {"hydro_cracking_ref"}:
        raise ValueError("Unexpected captive refinery H2 producer/consumer pool")
    _coefficient(
        _select(inputs, "LH2_bunker", "lh2", "final"),
        1.3,
        "LH2_bunker input (already includes loss)",
    )
    for commodity in ("ethylene", "propylene", "BTX"):
        inp = _select(inputs, "production_HVC", commodity, "final_material", commodity)
        out = _select(outputs, "production_HVC", "HVC", "demand", commodity)
        _coefficient(inp, 1.0, f"HVC {commodity} input")
        _coefficient(out, 1.0, f"HVC {commodity} output")
        dimensions = ["node_loc", "technology", "year_vtg", "year_act", "mode", "time"]
        if set(map(tuple, inp[dimensions].to_numpy())) != set(
            map(tuple, out[dimensions].to_numpy())
        ):
            raise ValueError(f"HVC {commodity} input/output coordinates do not pair")
    for commodity, producers in {
        "ethylene": {"MTO_petro", "steam_cracker_petro", "ethanol_to_ethylene_petro"},
        "propylene": {"MTO_petro", "steam_cracker_petro"},
        "BTX": {"steam_cracker_petro"},
    }.items():
        actual = outputs[
            (outputs["commodity"] == commodity) & (outputs["level"] == "final_material")
        ]
        if set(actual["technology"]) != producers:
            raise ValueError(
                f"Unexpected {commodity} producer pool: "
                f"{sorted(actual['technology'].unique())}"
            )


def validate_origin_basis(scenario: Scenario) -> None:
    """Read only model coefficients on the reporting job's already-open scenario."""
    commodities = [
        "hydrogen",
        "lh2",
        "methanol",
        "NH3",
        "ethylene",
        "propylene",
        "BTX",
        "HVC",
    ]
    validate_origin_coefficients(
        scenario.par("input", filters={"commodity": commodities}),
        scenario.par("output", filters={"commodity": commodities}),
    )
