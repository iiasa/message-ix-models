"""Add a global CO2 price to `scen`.

.. caution:: |gh-350|
"""

import logging
from typing import TYPE_CHECKING

from message_ix_models import ScenarioInfo

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)


def main(scen: "Scenario", price: float, conversion_factor: float = 44 / 12) -> None:
    """Add a global CO2 price to `scen`.

    A global carbon price is implemented with an annual growth rate equal to the
    discount rate.

    Parameters
    ----------
    scen :
        Scenario for which a carbon price should be added.
    price :
        Carbon price which should be added to the model. This value will be applied from
        the 'firstmodelyear' onwards.
    conversion_factor :
        The conversion_factor with which the input value is multiplied. The default
        assumption assumes that the price is specified in US$2005/tCO2, hence it is
        converted to US$2005/tC as required by the model.
    """
    years = ScenarioInfo(scen).Y
    df = (
        scen.par("duration_period", filters={"year": years})
        .drop(["unit"], axis=1)
        .rename(columns={"value": "duration"})
        .set_index(["year"])
    )
    for yr in years:
        if years.index(yr) == 0:
            val = price * conversion_factor
        else:
            val = (
                df.loc[years[years.index(yr) - 1]].value
                * (1 + scen.par("drate").value.unique()[0]) ** df.loc[yr].duration
            )
        df.at[yr, "value"] = val
    df = (
        df.reset_index()
        .drop(["duration"], axis=1)
        .rename(columns={"year": "type_year"})
        .assign(node="World", type_emission="TCE", type_tec="all", unit="USD/tC")
    )

    # log.debug(df.to_string())

    with scen.transact("Added carbon price"):
        scen.add_par("tax_emission", df)
