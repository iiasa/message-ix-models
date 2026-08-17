"""BMT workflow configuration."""

from typing import TYPE_CHECKING

from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from pathlib import Path

    from message_ix_models.util.context import Context


def apply_bmt_config(context: "Context", path: "Path | None" = None) -> None:
    """Load BMT configuration file into `context` (bmt, buildings, macro, transport).

    The file given by `path` **must** be in YAML format and have the top-level keys:

    .. code-block:: yaml

       model_name: "..."
       buildings:
          # ...
       macro: "..."
       materials:  # Optional
          # ...
       transport:  # Optional
          # ...

    All data from the file is stored at the key :py:`context.bmt` as a :class:`dict`.
    The function then sets or converts other values:

    - :attr:`context.buildings`: an instance of :class:`~.buildings.config.Config`
      (including :attr:`.Config.data_paths`, :attr`~code`` for STURM, etc.) from the
      ``buildings`` mapping.
    - :attr:`context.macro`: ``macro`` string (macro calibration workbook).
    - :attr:`context.transport`: and instance of :class:`transport.config.Config` using
      :meth:`~.transport.config.Config.from_context`, with the YAML ``transport``
      mapping passed as `options` (e.g. ``code: "M SSP2"``).

      .. note:: The :py:`context.transport` key must be an instance of that class,
         because :mod:`.model.transport` code expects its various attributes.

    Parameters
    ----------
    path :
        Configuration file path. If not given, defaults to :file:`data/bmt/config.yaml`.
        This file **must** be in YAML format has top-leve
    """
    import yaml

    from message_ix_models.model.buildings.config import METHOD
    from message_ix_models.model.buildings.config import Config as BuildingsConfig
    from message_ix_models.model.transport.config import Config as TransportConfig

    p = path or package_data_path("bmt", "config.yaml")

    # Create a buildings Config
    result = BuildingsConfig(sturm_scenario="NONE", method=METHOD.B)
    context.buildings = result

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
