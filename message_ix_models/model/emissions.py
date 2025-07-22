import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import genno
import pandas as pd
from genno import Key
from genno import operator as g
from iam_units import convert_gwp
from message_ix import Scenario, make_df

from message_ix_models import ScenarioInfo
from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.util import package_data_path

from .structure import get_codes

if TYPE_CHECKING:
    from genno.types import AnyQuantity

log = logging.getLogger(__name__)


@dataclass
class Options(BaseOptions):
    """Options for :class:`PRICE_EMISSION`."""

    #: Override parent class default of :any:`True`.
    aggregate: bool = False
    #: Override parent class default of :any:`True`.
    interpolate: bool = False

    #: Path containing CSV data files. For example:
    #:
    #: .. code-block:: python
    #:    package_data_path("transport", "R12", "price-emission")
    base_path: Path = field(default_factory=Path.cwd)

    #: Information about a scenario used to construct a file name. Specifically, the
    #: file name :file:`{scenario_info.path}.csv` is used. See
    #: :attr:`~.ScenarioInfo.path`.
    scenario_info: "ScenarioInfo" = field(default_factory=ScenarioInfo)


@register_source
class PRICE_EMISSION(ExoDataSource):
    """Provider of exogenous data for ``PRICE_EMISSION``."""

    Options = Options

    key = Key("PRICE_EMISSION:n-type_emission-type_tec-y:exo")

    def __init__(self, *args, **kwargs) -> None:
        opts = self.options = self.Options.from_args(
            f"{__name__}.PRICE_EMISSION", *args, **kwargs
        )

        self.path = opts.base_path.joinpath(f"{opts.scenario_info.path}.csv")
        if not self.path.exists():
            msg = f"No file in {self.path.parent} for {opts.scenario_info.url}"
            log.error(msg)
            raise ValueError(msg)

    def get(self) -> "AnyQuantity":
        from genno.operator import load_file
        from ixmp.report.common import RENAME_DIMS

        # Map e.g. "type_tec" → "type_tec", even if not in RENAME_DIMS
        dims = {d: d for d in self.key.dims} | RENAME_DIMS

        return load_file(self.path, dims=dims)


def get_emission_factors(units: Optional[str] = None) -> "AnyQuantity":
    """Return carbon emission factors.

    Values are from the file :file:`message_ix_models/data/ipcc/1996_v3_t1-2.csv`, in
    turn from `IPCC <https://www.ipcc-nggip.iges.or.jp/public/gl/guidelin/ch1wb1.pdf>`_
    (see Table 1-2 on page 1.6); these are the same that appear on the "Emissions from
    energy" page of the MESSAGEix-GLOBIOM documentation.

    The fuel dimension and names in the source are mapped to a :math:`c` ("commodity")
    dimension and labels from :ref:`commodity.yaml <commodity-yaml>`, using the
    ``ipcc-1996-name`` annotations appearing in the latter. A value for "methanol" that
    appears in the MESSAGEix-GLOBIOM docs table but not in the source is appended.

    Parameters
    ----------
    unit : str, optional
        Expression for units of the returned quantity. Tested values include:

        - “tC / TJ”, source units (default),
        - “t CO2 / TJ”, and
        - “t C / kWa”, internal units in MESSAGEix-GLOBIOM, for instance for
          "relation_activity" entries for emissions relations.

    Returns
    -------
    Quantity
        with 1 dimension (:math:`c`).
    """
    # Prepare information about commodities
    commodities = get_codes("commodity")
    relabel = {}  # Mapping from IPCC names/IDs to message_ix_models commodity ID
    select = []  # Select only the commodities needed
    for c in commodities:
        try:
            ipcc_name = str(c.get_annotation(id="ipcc-1996-name").text)
        except KeyError:
            continue
        else:
            relabel[ipcc_name] = c.id
            select.append(c.id)

    # Load data from file; relabel; and select only the values needed
    result = (
        g.load_file(package_data_path("ipcc", "1996_v3_t1-2.csv"), dims={"fuel": "c"})
        .pipe(g.relabel, dict(c=relabel))
        .pipe(g.select, dict(c=select))
    )

    # Manually insert a value for methanol
    result = g.concat(
        result,
        genno.Quantity(
            pd.Series(17.4, pd.Index(["methanol"], name="c")), units=result.units
        ),
    )

    result.attrs["species"] = "C"

    if units is not None:
        # Identify a GWP factor for target `units`, if any
        to_units, to_species = split_species(units)
        gwp_factor = convert_gwp(
            "AR5GWP100", (1.0, str(result.units)), "C", to_species
        ).magnitude
    else:
        gwp_factor, to_units = 1.0, result.units

    # Multiply by the GWP factor; let genno/pint handle other conversion
    return result.pipe(g.mul, genno.Quantity(gwp_factor)).pipe(
        g.convert_units, to_units
    )


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


def split_species(unit_expr: str) -> tuple[str, Optional[str]]:
    """Split `unit_expr` to an expression without a unit mention, and maybe species."""
    if match := re.fullmatch("(.*)(CO2|C)(.*)", unit_expr):
        return f"{match.group(1)}{match.group(3)}", match.group(2)
    else:
        return unit_expr, None
