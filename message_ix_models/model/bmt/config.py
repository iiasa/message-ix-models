"""BMT workflow configuration.

This module loads a file :file:`data/bmt/config.yaml`. This YAML file has top-level
keys:

.. code-block:: yaml

   buildings:
      # ...
   materials:
      # ...
   transport:
      # ...

Of these, only the first is currently required; the latter two are omitted. The function
:func:`_load_yaml` reads each section and passes it to the corresponding
:class:`.Context` key, for instance :py:`Context.buildings`.
"""

from typing import TYPE_CHECKING

from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from pathlib import Path

    from message_ix_models.model.buildings import Config


def load_buildings_config(path: "Path | None" = None) -> "Config":
    """Load the ``buildings`` section from :file:`data/bmt/config.yaml` for
    :attr:`context.buildings`.

    Missing keys in the ``buildings`` section are filled from
    :data:`BUILDINGS_DEFAULTS`. If the section is missing, all defaults are used.
    """
    from message_ix_models.model.buildings.config import METHOD, Config

    # Create a buildings Config
    result = Config(sturm_scenario="NONE", method=METHOD.B)

    # Update from the "buildings:" key in the YAML file
    result.read_file(path or package_data_path("bmt", "config.yaml"), key="buildings")

    return result
