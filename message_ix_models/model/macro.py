"""Tools for calibrating MACRO for MESSAGEix-GLOBIOM.

See :doc:`message-ix:macro` for *general* documentation on MACRO and MESSAGE-MACRO. This
module contains tools specifically for using these models with MESSAGEix-GLOBIOM.
"""
import logging
from functools import lru_cache
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, List, Literal, Mapping, Optional, Union

import pandas as pd

from message_ix_models.model.bare import get_spec
from message_ix_models.util import nodes_ex_world

if TYPE_CHECKING:
    from sdmx.model.v21 import Code

    from message_ix_models import Context

log = logging.getLogger(__name__)

#: Default set of commodities to include in :func:`generate`.
COMMODITY = ["i_therm", "i_spec", "rc_spec", "rc_therm", "transport"]


def generate(
    parameter: Literal["aeei", "config", "depr", "drate", "lotol"],
    context: "Context",
    commodities: Union[List[str], List["Code"]] = COMMODITY,
    value: Optional[float] = None,
) -> pd.DataFrame:
    """Generate uniform data for one :mod:`message_ix.macro` `parameter`.

    :meth:`message_ix.Scenario.add_macro` expects as its `data` parameter a
    :class:`dict` that maps certain MACRO parameter names (or the special name "config")
    to :class:`.pandas.DataFrame`. This function generates data for those data frames.

    For the particular dimensions, generate automatically includes:

    - "node": All nodes in the node code list given by :func:`.nodes_ex_world`, for the
      node list indicated by :attr:`.model.Config.regions`.
    - "year": All periods from the period *before* the first model year.
    - "commodity": The elements of `commodities`.
    - "sector": If each entry of `commodities` is a |Code| and has an annotation with
      id="macro-sector", the value of that annotation. Otherwise, the same as
      `commodity`.

    `value` supplies the parameter value, which is the same for all observations.
    The labels level="useful" and unit="-" are fixed.

    Parameters
    ----------
    parameter : str
        MACRO parameter for which to generate data.
    context
        Used with :func:`.bare.get_spec`.
    commodities : list of str or |Code|
        Commodities to include in the MESSAGE-MACRO linkage.
    value : float
        Parameter value.

    Returns
    -------
    pandas.DataFrame
        The columns vary according to `parameter`:

        - "aeei": node, sector, year, value, unit.
        - "depr", "drate", or "lotol": node, value, unit.
        - "config": node, sector, commodity, level, year.
    """
    spec = get_spec(context)

    if isinstance(commodities[0], str):
        c_codes = spec.add.set["commodity"]
    else:
        c_codes = commodities

    @lru_cache
    def _sector(commodity: str) -> str:
        try:
            idx = c_codes.index(commodity)
            return str(c_codes[idx].get_annotation(id="macro-sector").text)
        except (KeyError, ValueError) as e:
            log.info(e)
            return str(commodity)

    # AEEI data must begin from the period before the first model period
    y0_index = spec.add.set["year"].index(spec.add.y0)
    iterables = dict(
        c_s=zip(  # Paired commodity and sector
            map(str, commodities), map(_sector, commodities)
        ),
        level=["useful"],
        node=nodes_ex_world(spec.add.N),
        sector=map(_sector, commodities),
        year=spec.add.set["year"][y0_index:],
    )

    if parameter == "aeei":
        dims = ["node", "year", "sector"]
        iterables.update(year=spec.add.set["year"][y0_index - 1 :])
    elif parameter == "config":
        dims = ["node", "c_s", "level", "year"]
        assert value is None
    elif parameter in ("depr", "drate", "lotol"):
        dims = ["node"]
    else:
        raise NotImplementedError(f"generate(…) for MACRO parameter {parameter!r}")

    result = pd.DataFrame(
        [tuple(values) for values in product(*[iterables[d] for d in dims])],
        columns=dims,
    )

    if parameter == "config":
        return pd.concat(
            [
                result.drop("c_s", axis=1),
                pd.DataFrame(result["c_s"].tolist(), columns=["commodity", "sector"]),
            ],
            axis=1,
        )
    else:
        return result.assign(value=value, unit="-")


def load(base_path: Path) -> Mapping[str, pd.DataFrame]:
    """Load MACRO data from CSV files.

    The function reads files in the simple/long CSV format understood by
    :func:`genno.computations.load_file`. For use with
    :meth:`~message_ix.Scenario.add_macro`, the dimension names should be given in full,
    for instance "node" or "sector".

    Parameters
    ----------
    base_path : pathlib.Path
        Directory containing zero or more CSV files.

    Returns
    -------
    dict of (str -> pandas.DataFrame)
        Mapping from MACRO calibration parameter names to data; one entry for each file
        in `base_path`.
    """
    from genno.computations import load_file

    result = {}
    for filename in base_path.glob("*.csv"):
        name = filename.stem

        q = load_file(filename, name=name)

        result[name] = (
            q.to_frame()
            .reset_index()
            .rename(columns={name: "value"})
            .assign(unit=f"{q.units:~}" or "-")
        )

    return result
