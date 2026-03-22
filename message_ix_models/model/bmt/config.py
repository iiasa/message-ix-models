"""BMT workflow configuration.

This module loads a file :file:`data/bmt/config.yaml`. This YAML file has top-level
keys:

.. code-block:: yaml

   buildings:
      # ...
   macro: "..."
   materials:
      # ...
   transport:
      # ...

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

from typing import TYPE_CHECKING

from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from pathlib import Path

    from message_ix_models.util.context import Context


def apply_bmt_config(context: "Context", path: "Path | None" = None) -> None:
    """Load BMT YAML into ``context`` (bmt, buildings, macro, transport)."""
    import yaml

    from message_ix_models.model.buildings.config import METHOD
    from message_ix_models.model.buildings.config import Config as BuildingsConfig
    from message_ix_models.model.transport.config import Config as TransportConfig

    p = path or package_data_path("bmt", "config.yaml")

    # Create a buildings Config
    result = BuildingsConfig(sturm_scenario="NONE", method=METHOD.B)

    # Update from the "buildings:" key in the YAML file
    result.read_file(p, key="buildings")

    # Read the entire file
    with open(p) as f:
        data = yaml.safe_load(f) or {}

    # Store data, MACRO data directly on `context`
    context.bmt = data
    context.macro = data.get("macro")

    # Create a .transport.Config object, overriding defaults with values from the
    # "transport:" key in the file
    TransportConfig.from_context(context, options=data.get("transport", {}))
