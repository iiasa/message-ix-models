"""BMT workflow configuration.

Loads :file:`data/bmt/config.yaml` (sectors: buildings, transport, materials,
others). Each sector is passed to the corresponding context key (e.g.
:attr:`context.buildings`).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from message_ix_models.util import package_data_path


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load and return the full BMT config from YAML. No defaults applied here."""
    import yaml

    with open(path) as f:
        return yaml.safe_load(f) or {}


def get_bmt_config_path(path: Path | None = None) -> Path:
    """Return path to :file:`data/bmt/config.yaml`."""
    return path or package_data_path("bmt", "config.yaml")


# Defaults used only when a key is missing in the YAML (single source: this constant).
BUILDINGS_DEFAULTS: dict[str, Any] = {
    "prices": "input_prices_R12.csv",
    "sturm_r": "report_MESSAGE_resid_SSP2_nopol_post.csv",
    "sturm_c": "report_MESSAGE_comm_SSP2_nopol_post.csv",
    "demand_static": "static_20251227.csv",
    "with_materials": False,
}


@dataclass
class BuildingsConfig:
    """Configuration for build_B when run from the BMT workflow.

    All values come from the ``buildings`` section of :file:`data/bmt/config.yaml`.
    Paths are filenames under :func:`~message_ix_models.util.private_data_path`
    (``buildings``) unless overridden with absolute paths in the YAML.
    """

    prices: str
    sturm_r: str
    sturm_c: str
    demand_static: str
    with_materials: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BuildingsConfig":
        """Build from a dict (e.g. the ``buildings`` section of the BMT config).

        Missing keys are filled from :data:`BUILDINGS_DEFAULTS` so the YAML
        only needs to override what it wants to change.
        """
        merged = {**BUILDINGS_DEFAULTS, **data}
        return cls(
            prices=merged["prices"],
            sturm_r=merged["sturm_r"],
            sturm_c=merged["sturm_c"],
            demand_static=merged["demand_static"],
            with_materials=merged["with_materials"],
        )


def load_buildings_config(path: Path | None = None) -> BuildingsConfig:
    """Load the ``buildings`` section from :file:`data/bmt/config.yaml` for
    :attr:`context.buildings`.

    Missing keys in the ``buildings`` section are filled from
    :data:`BUILDINGS_DEFAULTS`. If the section is missing, all defaults are used.
    """
    data = _load_yaml(get_bmt_config_path(path))
    buildings = data.get("buildings") or {}
    return BuildingsConfig.from_dict(buildings)
