"""MACRO utilities."""
import logging
from functools import lru_cache
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, List, Literal, Mapping, Optional, Union

import pandas as pd

from message_ix_models.model.bare import get_spec
from message_ix_models.util import nodes_ex_world

if TYPE_CHECKING:  # pragma: no cover
    from sdmx.model.v21 import Code

    from message_ix_models import Context

log = logging.getLogger(__name__)

#: Default set of commodities to include.
COMMODITY = ["i_therm", "i_spec", "rc_spec", "rc_therm", "transport"]


def generate(
    parameter: Literal["aeei", "config", "depr", "drate", "lotol"],
    context: "Context",
    commodities: Union[List[str], List["Code"]] = COMMODITY,
    value: Optional[float] = None,
) -> pd.DataFrame:
    """Generate uniform data for one :mod:`message_ix.macro` `parameter`.

    :meth:`message_ix.Scenario.add_macro` expects as its `data` parameter a dictionary
    mapping certain MACRO parameter names (or the special name "config") to
    :class:`.pandas.DataFrame`. This function generates data for those data frames.

    For the particular dimensions:

    - "node": All nodes in the node code list given by :func:`.nodes_ex_world`.
    - "year": All periods from the period *before* the first model year.
    - "commodity": The elements of `commodities`.
    - "sector": If each entry of `commodities` is a :class:`.Code` and has an annotation
      with id="macro-sector", the given value. Otherwise, the same as `commodity`.

    `value` supplies the parameter value, which is the same for all observations.
    The labels level="useful" and unit="-" are fixed.

    Parameters
    ----------
    parameter : str
        MACRO parameter for which to generate data.
    context
        Used with :func:`.bare.get_spec`.
    commodities : list of str or Code
        Commodities to include in the MESSAGE-MACRO linkage.
    value : float
        Parameter value.

    Returns
    -------
    pandas.DataFrame
        The columns vary according to `parameter`:

        - "aeei": node, year, sector, value, unit.
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
            result = str(c_codes[idx].get_annotation(id="macro-sector").text)
        except (KeyError, ValueError) as e:
            log.info(e)
            result = commodity
        return result

    # AEEI data must begin from the period before the first model period
    y0_index = spec.add.set["year"].index(spec.add.y0)
    iterables = dict(
        c_s=zip(commodities, map(_sector, commodities)),  # Paired commodity and sector
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
    """Load MACRO data from CSV files."""
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
