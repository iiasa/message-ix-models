from collections import ChainMap
from functools import lru_cache

from sdmx.model import Annotation

from message_ix_models.util import as_codes, load_package_data


@lru_cache()
def get_codes(name):
    """Return codes for the set `name` in MESSAGE-GLOBIOM scenarios.

    The information is read from :file:`data/{name}.yaml`, e.g.
    :file:`data/technology.yaml`.

    Parameters
    ----------
    name : :class:`str`
        Any :file:`.yaml` file in the folder :file:`message_ix_models/data/`.

    Returns
    -------
    list of :class:`.Code`
    """
    # Raw contents of the config file
    config = load_package_data(name)

    if "node" in name:
        # Automatically add information for countries within regions in the node
        # codelists
        from pycountry import countries

        # Use a ChainMap to combine a new dict and the `config` loaded from file, in
        # that order
        config = ChainMap(
            config,
            # Create codes using the ISO database via pycountry
            {c.alpha_3: dict(id=c.alpha_3, name=c.name) for c in countries},
        )

    # Convert to codes
    data = as_codes(config)

    if name == "technology":
        for code in data:
            try:
                anno = code.pop_annotation(id="vintaged")
            except KeyError:
                # Default value for 'vintaged'
                anno = Annotation(id="vintaged", text=repr(False))
            code.annotations.append(anno)

    return data
