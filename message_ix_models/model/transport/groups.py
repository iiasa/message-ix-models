"""Consumer groups data."""

import logging
from copy import deepcopy
from typing import TYPE_CHECKING

import pandas as pd
import xarray as xr
from genno import Quantity

if TYPE_CHECKING:
    from genno import Computer

log = logging.getLogger(__name__)


def prepare_computer(c: "Computer") -> None:
    """Prepare `rep` for calculating transport consumer groups."""
    from .key import cg, exo, pop_at

    c.add("indexers:n-cd", "indexers_n_cd", "config")
    # Population shares by area_type
    c.add(pop_at, urban_rural_shares, "population:n-y", "config")
    # Exogenous data for consumer group sizes
    keys = [
        exo.population_suburb_share,
        exo.pop_share_attitude,
        exo.pop_share_driver,
        exo.pop_share_cd_at,
    ]
    c.add(cg, cg_shares, pop_at, *keys, "indexers:n-cd", "indexers:cg")


def cg_shares(
    ursu_ru: Quantity,
    su_share: Quantity,
    ma3t_attitude: Quantity,
    ma3t_driver: Quantity,
    ma3t_pop: Quantity,
    n_cd_indexers: dict[str, xr.DataArray],
    cg_indexers: dict[str, xr.DataArray],
) -> Quantity:
    """Return shares of transport consumer groups.

    Parameters
    ----------
    ursu_ru : Quantity
        Population shares with "UR+SU" and "RU" on the ``area_type`` dimension.
    su_share : Quantity
        Share of suburban ("SU") population within "UR+SU".
    ma3t_attitude: Quantity
    ma3t_driver : Quantity
    ma3t_pop : Quantity
        Population shares between urban, suburban, and rural.

        DLM: “Values from MA3T are based on 2001 NHTS survey and some more recent
        calculations done in 2008 timeframe. Therefore, I assume that the numbers here
        are applicable to the US in 2005.”

        NB in the spreadsheet, the data are also filled forward to 2110.

        Currently not used.
    n_cd_indexers : dict
    cg_indexers : dict

    Returns
    -------
    .Quantity
        Dimensions: n, y, cg. Units.dimensionless.
    """
    from genno.operator import concat, mul

    cg_indexers = deepcopy(cg_indexers)
    consumer_group = cg_indexers.pop("consumer_group")

    # Assumption: split of population between area_type 'UR' and 'SU'
    # - Fill forward along years, for nodes where only a year 2010 value is assumed.
    # - Fill backward 2010 to 2005, in order to compute.
    su_share = su_share.ffill("y").bfill("y")

    # Split the 'UR+SU' population share to "UR", "SU" using `su_share`
    assert {"UR+SU", "RU"} == set(ursu_ru.coords["area_type"].data)
    pop_share = (
        concat(
            ursu_ru.sel(area_type="UR+SU", drop=True) * (1 - su_share),
            ursu_ru.sel(area_type="UR+SU", drop=True) * su_share,
            ursu_ru.sel(area_type="RU", drop=True),
            dim=pd.Index(["UR", "SU", "RU"], name="area_type"),
        )
        .ffill("y")
        .bfill("y")
    )

    # - Compute the group shares.
    # - Select using matched sequences, i.e. select a sequence of (node,
    #   census_division) coordinates.
    # - Drop the census_division.
    # - Collapse area_type, attitude, driver_type dimensions into consumer_group.
    # - Convert to short dimension names.
    groups = (
        mul(pop_share, ma3t_attitude, ma3t_driver)
        .sel(n_cd_indexers)
        .sel(cg_indexers)
        .assign_coords(consumer_group=consumer_group.values)
        .rename(dict(node="n", year="y", consumer_group="cg"))
    )

    # Assert that the sum across groups is 1
    assert isinstance(groups, Quantity)
    assert (groups.sum("cg") - 1 < 1e-5).all(), "Groups not balanced"

    return groups


def urban_rural_shares(pop: Quantity, config: dict) -> Quantity:
    """Return shares of urban and rural population.

    Parameters
    ----------
    pop : .Quantity
        Must have n and y dimensions.
    config : dict
        The ``regions`` and ``data source/population`` keys are used.

    Returns
    -------
    .Quantity
        Dimensions: at least area_type, possibly also n, y. Units: dimensionless.
    """
    from genno.operator import div

    from message_ix_models.util import broadcast

    if "area_type" in pop.dims:
        result = div(
            pop.sel(area_type=["UR+SU", "RU"]), pop.sel(area_type="total", drop=True)
        )
    else:
        log.warning("Population data lack 'area_type' dimension")
        df = (
            pd.DataFrame([["UR+SU", 0.6], ["RU", 0.4]], columns=["area_type", "value"])
            .assign(n=None, y=None)
            .pipe(broadcast, n=pop.coords["n"].data, y=pop.coords["y"].data)
            .set_index(["area_type", "n", "y"])
        )
        result = Quantity(df)

    assert {"area_type", "n", "y"} == set(result.dims), result.dims

    return result
