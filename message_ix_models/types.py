"""Types for hinting."""

from collections.abc import Mapping, MutableMapping
from typing import Any, Optional, TypedDict, Union

import pandas as pd
import sdmx.model.common
from genno.core.key import KeyLike  # TODO Import from genno.types, when possible

try:
    from genno.core.quantity import AnyQuantity
except ImportError:  # genno <= 1.24
    from genno.core.quantity import Quantity

    AnyQuantity: type = Quantity  # type: ignore [no-redef]

__all__ = [
    "AnyQuantity",
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


# For sdmx1


class AnnotableArtefactArgs(TypedDict, total=False):
    annotations: list[sdmx.model.common.BaseAnnotation]


class NameableArtefactArgs(AnnotableArtefactArgs, total=False):
    name: Optional[str]
    description: Optional[str]


class MaintainableArtefactArgs(NameableArtefactArgs, total=False):
    """Some keyword arguments to :class:`sdmx.model.common.MaintainableArtefact`."""

    is_external_reference: Optional[bool]
    is_final: Optional[bool]
    maintainer: Any
    # NB Only present from sdmx1 2.16; minimum for message-ix-models is sdmx1 2.13.1
    version: Optional[Union[str, "sdmx.model.common.Version"]]
