"""Types for hinting."""

from collections.abc import Mapping, MutableMapping
from typing import Any, Optional, TypedDict, Union

import pandas as pd
import sdmx.model.common
from genno.core.key import KeyLike  # TODO Import from genno.types, when possible

__all__ = [
    "KeyLike",
    "MaintainableArtefactArgs",
    "MutableParameterData",
    "ParameterData",
]

#: Collection of :mod:`message_ix` or :mod:`ixmp` parameter data. Keys should be
#: parameter names (:class:`str`), and values should be data frames with the same
#: structure returned by :func:`.make_df`. See also :func:`.add_par_data`.
ParameterData = Mapping[str, pd.DataFrame]

#: Mutable collection of :mod:`message_ix` or :mod:`ixmp` parameter data.
MutableParameterData = MutableMapping[str, pd.DataFrame]


class MaintainableArtefactArgs(TypedDict):
    """Some keyword arguments to :class:`sdmx.model.common.MaintainableArtefact`."""

    is_external_reference: Optional[bool]
    is_final: Optional[bool]
    maintainer: Any
    version: Optional[Union[str, sdmx.model.common.Version]]
