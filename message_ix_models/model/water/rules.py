from __future__ import annotations

import warnings
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
from numbers import Number
from typing import Any, Dict, Iterable, List, Tuple

from message_ix_models.util.citation_wrapper import register_citation_entry

# --------------------------------------------------------------------------- #
# Units                                                                       #
# --------------------------------------------------------------------------- #
# All factors are expressed in the basis of m³ (or m³ / a for flows)
UNIT_FACTORS: Dict[str, float] = {
    # volumes (static)
    "m3": 1.0,
    "MCM": 1e6,
    "km3": 1e9,
    # volume flows
    "m3/year": 1.0,
    "Mm3/year": 1e6,
    "MCM/year": 1e6,
    "km3/year": 1e9,
    # energy-related
    "GWa": 1e9,  # 1 GW·a = 1e9 W·a
    "MCM/GWa": 1e-3,  # 1 MCM per GW·a
    # prices
    "USD/m3": 1.0,
    "USD/MCM": 1e-6,
    "USD/km3": 1e-9,
    # energy per volume
    "GWh/km3": 1e-9,
    "GWh/MCM": 1e-6,
    # dimensionless
    "-": 1.0,
    "%": 1.0,
    "y": 1.0,
}

# Define fundamental dimensions for each unit
# L: Length, T: Time, C: Currency, E: Energy
# Dimensionless units are represented by an empty dict {}
UNIT_DIMENSIONS: Dict[str, Dict[str, int]] = {
    # volumes
    "m3": {"L": 3},
    "MCM": {"L": 3},
    "km3": {"L": 3},
    # volume flows
    "m3/year": {"L": 3, "T": -1},
    "Mm3/year": {"L": 3, "T": -1},
    "MCM/year": {"L": 3, "T": -1},
    "km3/year": {"L": 3, "T": -1},
    # energy-related
    "GWa": {"E": 1},  # GigaWatt-annum is Energy
    "MCM/GWa": {"L": 3, "E": -1},
    # prices
    "USD/m3": {"C": 1, "L": -3},
    "USD/MCM": {"C": 1, "L": -3},
    "USD/km3": {"C": 1, "L": -3},
    # energy per volume
    "GWh/km3": {"E": 1, "L": -3},
    "GWh/MCM": {"E": 1, "L": -3},
    # dimensionless
    "-": {},
    "%": {},
    # time
    "y": {"T": 1},  # 'year' as a unit of Time
}


def get_conversion_factor(from_unit: str, to_unit: str) -> float:
    """
    Multiplier to convert *value* [from_unit] → [to_unit].

    Performs a dimensional consistency check before returning the factor.

    Raises
    ------
    ValueError
        If either unit is missing in ``UNIT_FACTORS``, if dimensions for a unit
        are not defined in ``UNIT_DIMENSIONS``, or if units are dimensionally
        incompatible.
    """
    if from_unit == to_unit:
        return 1.0

    try:
        f_from = UNIT_FACTORS[from_unit]
        f_to = UNIT_FACTORS[to_unit]
        dim_from = UNIT_DIMENSIONS[from_unit]
        dim_to = UNIT_DIMENSIONS[to_unit]
    except KeyError as exc:
        unit_key = exc.args[0]
        if unit_key not in UNIT_FACTORS:
            raise ValueError(f"Unsupported unit: {unit_key!r}") from None
        else:  # Unit in UNIT_FACTORS but not UNIT_DIMENSIONS
            raise ValueError(f"Dimensions not defined for unit: {unit_key!r}") from None

    if dim_from != dim_to:
        raise ValueError(
            f"Dimension mismatch: cannot convert from {from_unit} (dims: {dim_from}) "
            f"to {to_unit} (dims: {dim_to})"
        )

    if not f_to:  # defensive; zero not expected in the table
        raise ValueError(f"Cannot convert to unit {to_unit!r} with factor 0.")
    return f_from / f_to


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #
def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge *b* into *a* without mutating either."""
    result: Dict[str, Any] = deepcopy(a)
    for key, val in b.items():
        if (
            key in result
            and isinstance(result[key], Mapping)
            and isinstance(val, Mapping)
        ):
            result[key] = deep_merge(result[key], val)  # type: ignore[arg-type]
        else:
            result[key] = deepcopy(val)
    return result


# --------------------------------------------------------------------------- #
# Constants container                                                         #
# --------------------------------------------------------------------------- #
class Constants:
    """
    Hold named constants (possibly with units) and provide
    value scaling plus optional citation registration.
    Supports multiple citations with mappings to constants.
    """

    __slots__ = (
        "_data",
        "citations",  # List of citation dicts
        "citation_map",  # Dict mapping constant names to citation indices
        "metadata",
        "date",
    )

    def __init__(
        self,
        data: Iterable[Tuple[str, Any, str]],
        *,
        citations: List[Dict[str, str]] | None = None,
        citation_map: Dict[str, int] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        self._data: Dict[str, Tuple[Any, str]] = {}
        for name, value, unit in data:
            if unit not in UNIT_FACTORS:
                raise ValueError(f"Unsupported unit for {name!r}: {unit!r}")
            if name in self._data:
                warnings.warn(f"Constant {name!r} redefined; using last value.")
            self._data[name] = (value, unit)

        self.citations = citations or []
        self.citation_map = citation_map or {}
        self.metadata = metadata or {}
        self.date: str = datetime.now(timezone.utc).date().isoformat()

        # Register all citations
        for idx, citation in enumerate(self.citations):
            if "citation" not in citation or "doi" not in citation:
                warnings.warn(f"Citation at index {idx} missing required fields.")
                continue

            registry_key = citation.get(
                "description", f"constants_data_doi_{citation['doi']}"
            )
            register_citation_entry(
                key=registry_key,
                citation=citation["citation"],
                doi=citation["doi"],
                description=citation.get("description"),
                metadata=self.metadata,
                date=self.date,
            )

    # --------------------------------------------------------------------- #
    # Public helpers                                                        #
    # --------------------------------------------------------------------- #
    def as_dict(self) -> Dict[str, Any]:
        """`{name: raw_value}` – no scaling, no units."""
        return {name: val for name, (val, _) in self._data.items()}

    def scaled(self, target_unit: str) -> Dict[str, Any]:
        """
        Return a mapping with all *numeric* entries converted to `target_unit`.

        Non‑numeric or incompatible units are passed through unchanged.
        """
        if target_unit not in UNIT_FACTORS:
            raise ValueError(f"Unknown target unit {target_unit!r}")

        out: Dict[str, Any] = {}
        for name, (val, base_unit) in self._data.items():
            if not isinstance(val, Number) or base_unit in ("-", target_unit):
                out[name] = val
                continue
            try:
                out[name] = val * get_conversion_factor(base_unit, target_unit)
            except ValueError:
                out[name] = val  # incompatible units – leave untouched
        return out


# --------------------------------------------------------------------------- #
# Rule builder                                                                #
# --------------------------------------------------------------------------- #
class Rule:
    """
    Combine a *Base* template with one or more *Diff* patches,
    resolving embedded constants and ensuring coherent units.
    """

    def __init__(
        self,
        *,
        Base: Dict[str, Any] | None = None,
        Diff: List[Dict[str, Any]] | None = None,
        flag_manual_convert: bool = False,
        constants_manager: Constants | None = None,
    ) -> None:
        defaults: Dict[str, Any] = {
            "pipe": {
                "flag_broadcast": False,
                "flag_map_yv_ya_lt": False,
                "flag_same_time": False,
                "flag_same_node": False,
                "flag_time": False,
                "flag_node_loc": False,
            }
        }
        self.Base: Dict[str, Any] = deep_merge(defaults, Base or {})
        self.Diff: List[Dict[str, Any]] = Diff or [{}]
        self.flag_manual_convert = flag_manual_convert
        self.constants_manager = constants_manager

    # --------------------------------------------------------------------- #
    # Public interface                                                      #
    # --------------------------------------------------------------------- #
    def get_rule(self) -> List[Dict[str, Any]]:
        """
        Materialise every entry in ``Diff`` into a fully‑specified rule dict,
        ready for downstream consumption.
        """
        rendered: List[Dict[str, Any]] = []
        for diff in self.Diff:
            rule = deep_merge(self.Base, diff)

            if rule.get("condition") == "SKIP":
                continue

            unit = self._require_unit(rule)
            constants = (
                self.constants_manager.scaled(unit) if self.constants_manager else {}
            )

            value, was_scaled = self._resolve_value(rule.get("value"), constants)
            rule["value"] = value

            if not self.flag_manual_convert:
                factor = self._compute_factor(value, was_scaled, rule, unit)
                self._apply_factor(rule, value, factor)

            rule.pop("unit_in", None)
            rendered.append(rule)
        return rendered

    def change_unit(self, new_unit: str) -> None:
        """Mutate `Base["unit"]` (only if *new_unit* is recognised)."""
        if new_unit not in UNIT_FACTORS:
            raise ValueError(f"Unsupported unit: {new_unit!r}")
        self.Base["unit"] = new_unit

    # --------------------------------------------------------------------- #
    # Internals                                                             #
    # --------------------------------------------------------------------- #
    @staticmethod
    def _require_unit(rule: Dict[str, Any]) -> str:
        unit = rule.get("unit")
        if unit is None:
            raise ValueError("Each rule must define a 'unit'.")
        if unit not in UNIT_FACTORS:
            raise ValueError(f"Unsupported unit {unit!r} in rule.")
        return unit

    @staticmethod
    def _resolve_value(
        value: Any, consts: Dict[str, Any]
    ) -> Tuple[Any, bool]:  # (converted_value, numeric?)
        if isinstance(value, str) and consts:
            if value in consts:
                v = consts[value]
                return v, isinstance(v, Number)
            if "{" in value:
                try:
                    formatted = value.format(**consts)
                    return float(formatted), True
                except Exception:
                    return formatted if "formatted" in locals() else value, False
        return value, False

    @staticmethod
    def _compute_factor(
        value: Any, was_scaled: bool, rule: Dict[str, Any], unit: str
    ) -> float:
        if was_scaled:
            return 1.0

        src_unit = rule.get("unit_in")
        if src_unit is None or src_unit == unit:
            return 1.0

        try:
            return get_conversion_factor(src_unit, unit)
        except ValueError as exc:
            warnings.warn(
                f"Conversion error {src_unit!r} → {unit!r}: {exc}. "
                "Leaving value unchanged."
            )
            return 1.0

    @staticmethod
    def _apply_factor(rule: Dict[str, Any], value: Any, factor: float) -> None:
        if factor == 1.0 or value is None:
            return
        if isinstance(value, Number):
            rule["value"] = value * factor
        else:
            try:
                rule["value"] = float(value) * factor
            except Exception:
                rule["value"] = f"({value}) * {factor}"
