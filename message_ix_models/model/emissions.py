import logging
from typing import Optional

import pandas as pd
from iam_units import convert_gwp
from message_ix import Scenario, make_df

from message_ix_models import ScenarioInfo

log = logging.getLogger(__name__)


def add_tax_emission(
    scen: Scenario,
    price: float,
    conversion_factor: Optional[float] = None,
    drate_parameter="drate",
) -> None:
    """Add a global CO₂ price to `scen`.

    A carbon price is implemented on node=“World” by populating the
    :ref:`MESSAGEix parameter <message-ix:section_parameter_emissions>`
    ``tax_emission``, starting from the first model year and covering the entire model
    horizon. The tax has an annual growth rate equal to the discount rate.

    The other dimensions of ``tax_emission`` are filled with type_emission=“TCE” and
    type_tec=“all”.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
    price : float
        Price in the first model year, in USD / tonne CO₂.
    conversion_factor : float, optional
        Factor for converting `price` into the model's internal emissions units,
        currently USD / tonne carbon. Optional: a default value is retrieved from
        :mod:`iam_units`.
    drate_parameter : str; one of "drate" or "interestrate"
        Name of the parameter to use for the growth rate of the carbon price.
    """
    years = ScenarioInfo(scen).Y
    filters = dict(year=years)
    # Default: since the mass of the species is in the denominator, take the inverse
    conversion_factor = conversion_factor or 1.0 / convert_gwp(
        "AR5GWP100", "1 t", "CO2", "C"
    )

    # Duration of periods
    dp = scen.par("duration_period", filters=filters).set_index("year")["value"]

    # Retrieve the discount rate
    if drate_parameter == "interestrate":
        # MESSAGE parameter with "year" dimension
        r = scen.par(drate_parameter, filters=filters).set_index("year")["value"]
    else:
        # MACRO parameter with "node" dimension
        drates = scen.par(drate_parameter).value.unique()
        if len(drates) > 1:
            log.warning(f"Using the first of multiple discount rates: drate={drates}")
        r = pd.Series([drates[0]] * len(years), index=pd.Index(years, name="year"))

    # Compute cumulative growth versus the first period
    r_cumulative = (r + 1).pow(dp.shift(-1)).cumprod().shift(1, fill_value=1.0)

    # Assemble the parameter data
    name = "tax_emission"
    data = make_df(
        name,
        value=(price * conversion_factor * r_cumulative),
        type_year=r_cumulative.index,
        node="World",
        type_emission="TCE",
        type_tec="all",
        unit="USD/tC",
    )

    with scen.transact("Added carbon price"):
        scen.add_par(name, data)
