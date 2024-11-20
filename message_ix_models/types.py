"""Types for hinting."""

from collections.abc import Mapping, MutableMapping
from typing import Hashable

import pandas as pd

#: Collection of :mod:`message_ix` or :mod:`ixmp` parameter data. Keys should be
#: parameter names (:class:`str`), and values should be data frames with the same
#: structure returned by :func:`.make_df`. See also :func:`.add_par_data`.
ParameterData = Mapping[Hashable, pd.DataFrame]

#: Mutable collection of :mod:`message_ix` or :mod:`ixmp` parameter data.
MutableParameterData = MutableMapping[Hashable, pd.DataFrame]
