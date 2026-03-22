"""BMT workflow configuration.

Loads :file:`data/bmt/config.yaml` once into :attr:`context.bmt`, then sets:

- :attr:`context.buildings` — ``SimpleNamespace`` of file stems for :func:`build_B`
  (defaults merged with the ``buildings`` mapping).
- :attr:`context.macro` — ``macro`` string (macro calibration workbook).
- :attr:`context.transport` — full
  :class:`message_ix_models.model.transport.config.Config` from
  :meth:`~message_ix_models.model.transport.config.Config.from_context`, with the
  YAML ``transport`` section passed as ``options`` (e.g. ``code: "M SSP2"``).

The transport object must stay as that :class:`Config` class: the rest of
MESSAGEix-Transport reads ``context.transport.spec``, ``.modules``, etc., not a raw
dict.
"""

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from message_ix_models.util import package_data_path
from message_ix_models.util.context import Context

# Defaults when the ``buildings`` section omits a key.
_BUILDINGS_DEFAULTS: dict[str, Any] = {
    "prices": "input_prices_R12.csv",
    "sturm_r": "report_MESSAGE_resid_SSP2_nopol_post.csv",
    "sturm_c": "report_MESSAGE_comm_SSP2_nopol_post.csv",
    "demand_static": "static_20251227.csv",
    "with_materials": False,
}


def apply_bmt_config(context: Context, path: Path | None = None) -> None:
    """Load BMT YAML into ``context`` (bmt, buildings, macro, transport)."""
    p = path or package_data_path("bmt", "config.yaml")
    with open(p) as f:
        data = yaml.safe_load(f) or {}
    context.bmt = data

    b = {**_BUILDINGS_DEFAULTS, **(data.get("buildings") or {})}
    context.buildings = SimpleNamespace(**b)

    context.macro = data.get("macro")

    from message_ix_models.model.transport.config import Config

    Config.from_context(context, options=dict(data.get("transport") or {}))
