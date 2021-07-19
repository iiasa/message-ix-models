"""Reporting computations for MESSAGEix-Transport."""
from typing import Hashable, Mapping, Union

import numpy as np
import pandas as pd
import xarray as xr
from genno import Quantity, computations
from ixmp import Scenario
from message_ix_models import ScenarioInfo


def as_quantity(info: dict) -> Quantity:
    dim = info.pop("_dim")
    unit = info.pop("_unit")

    return Quantity(pd.Series(info).rename_axis(dim), units=unit)


def dummy_prices(gdp: Quantity) -> Quantity:
    # Commodity prices: all equal to 0.1

    # Same coords/shape as `gdp`, but with c="transport"
    coords = [(dim, item.data) for dim, item in gdp.coords.items()]
    coords.append(("c", ["transport"]))
    shape = list(len(c[1]) for c in coords)

    return Quantity(xr.DataArray(np.full(shape, 0.1), coords=coords), units="USD / km")


def ldv_distance(config: dict) -> Quantity:
    """Return annual driving distance per LDV.

    - Regions other than R11_NAM have M/F values in same proportion to their A value as
      in NAM
    """
    # Load from config.yaml
    result = computations.product(
        as_quantity(config["ldv activity"]),
        as_quantity(config["factor"]["activity"]["ldv"]),
    )

    result.name = "ldv distance"

    return result


def rename(
    qty: Quantity,
    new_name_or_name_dict: Union[Hashable, Mapping[Hashable, Hashable]] = None,
    **names: Hashable
) -> Quantity:
    """Like :meth:`xarray.DataArray.rename`.

    .. todo:: Upstream to :mod:`genno`.
    """
    return qty.rename(new_name_or_name_dict, **names)


def transport_check(scenario: Scenario, ACT: Quantity) -> pd.Series:
    """Reporting computation for :func:`check`.

    Imported into :mod:`.reporting.computations`.
    """
    info = ScenarioInfo(scenario)

    # Mapping from check name → bool
    checks = {}

    # Correct number of outputs
    ACT_lf = ACT.sel(t=["transport freight load factor", "transport pax load factor"])
    checks["'transport * load factor' technologies are active"] = len(
        ACT_lf
    ) == 2 * len(info.Y) * (len(info.N) - 1)

    # # Force the check to fail
    # checks['(fail for debugging)'] = False

    return pd.Series(checks)
