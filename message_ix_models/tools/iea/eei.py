"""Handle data from the IEA Energy Efficiency Indicators (EEI)."""
# FIXME This file is currently excluded from coverage measurement. See
#       iiasa/message-ix-models#164

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

import genno
import numpy as np
import pandas as pd
import plotnine as p9
from genno import Key

from message_ix_models import Context
from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.util import cached, path_fallback

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import AnyQuantity

log = logging.getLogger(__name__)

REPLACE = dict(UNIT_MEASURE={"10^3 vkm/vehicle": "Mm / vehicle / year"})

UNITS = """
MEASURE    INDICATOR                               PRODUCT                                                 UNIT_MEASURE
   energy                                    __NA                                    Oil and oil products                  PJ
   energy                                    __NA                                                     Gas                  PJ
   energy                                    __NA                                  Coal and coal products                  PJ
   energy                                    __NA                                      Biofuels and waste                  PJ
   energy                                    __NA                                                    Heat                  PJ
   energy                                    __NA                                             Electricity                  PJ
   energy                                    __NA                                           Other sources                  PJ
   energy                                    __NA                                      Total final energy                  PJ
   energy                                    __NA                                 Of which: Solar thermal                  PJ
   energy                                    __NA                                          Motor gasoline                  PJ
   energy                                    __NA                               Diesel and light fuel oil                  PJ
   energy                                    __NA                                                     LPG                  PJ
   energy                                    __NA                                          Heavy fuel oil                  PJ
   energy                                    __NA                          Jet fuel and aviation gasoline                  PJ
     __NA                                    __NA                                              Population                10^6
     __NA                                    __NA                                     Services employment                10^6
     __NA                                    __NA                                      Occupied dwellings                10^6
     __NA                                    __NA                                  Residential floor area             10^9 m2
     __NA                                    __NA                                     Heating degree days                10^3
     __NA                                    __NA                                     Cooling degree days                10^3
     __NA                                    __NA                                                  Stocks       million units
     __NA                                    __NA                                             Value added   10^9 USD PPP 2015
     __NA                                    __NA                                       Cement production              10^6 t
     __NA                                    __NA                                        Steel production              10^6 t
     __NA                                    __NA                                    Passenger-kilometres            10^9 pkm
     __NA                                    __NA                                      Vehicle-kilometres            10^9 vkm
     __NA                                    __NA                                           Vehicle stock                10^6
     __NA                                    __NA                                        Tonne-kilometres            10^9 tkm
     __NA                                    __NA      Occupied dwellings of which heated by oil products                   %
     __NA                                    __NA               Occupied dwellings of which heated by gas                   %
     __NA                                    __NA          Occupied dwellings of which heated by biofuels                   %
     __NA                                    __NA  Occupied dwellings of which heated by district heating                   %
     __NA                                    __NA       Occupied dwellings of which heated by electricity                   %
     __NA                                    __NA                                     Services floor area             10^9 m2
     __NA                                    __NA                                              Peak power                 MWp
     __NA             Per capita energy intensity                                                    __NA              GJ/cap
     __NA         Per floor area energy intensity                                                    __NA               GJ/m2
     __NA      Per floor area TC energy intensity                                                    __NA               GJ/m2
     __NA           Per dwelling energy intensity                                                    __NA               GJ/dw
     __NA        Per dwelling TC energy intensity                                                    __NA               GJ/dw
     __NA     Per unit equipment energy intensity                                                    __NA             GJ/unit
     __NA        Per value added energy intensity                                                    __NA     MJ/USD PPP 2015
     __NA  Per services employee energy intensity                                                    __NA         GJ/employee
     __NA    Per physical output energy intensity                                                    __NA                GJ/t
     __NA                          Fuel intensity                                                    __NA      litres/100 vkm
     __NA         Passenger-kilometres per capita                                                    __NA        10^3 pkm/cap
     __NA   Passenger-kilometres energy intensity                                                    __NA              MJ/pkm
     __NA                   Passenger load factor                                                    __NA             pkm/vkm
     __NA           Vehicle-kilometres per capita                                                    __NA        10^3 vkm/cap
     __NA     Vehicle-kilometres energy intensity                                                    __NA              MJ/vkm
     __NA                             Vehicle use                                                    __NA    10^3 vkm/vehicle
     __NA             Tonne-kilometres per capita                                                    __NA        10^3 tkm/cap
     __NA       Tonne-kilometres energy intensity                                                    __NA              MJ/tkm
     __NA                     Freight load factor                                                    __NA             tkm/vkm
emissions                                    __NA                                   Total final emissions               MtCO2
     __NA             Per capita carbon intensity                                                    __NA            tCO2/cap
     __NA         Per floor area carbon intensity                                                    __NA             tCO2/m2
     __NA           Per dwelling carbon intensity                                                    __NA             tCO2/dw
     __NA     Per unit equipment carbon intensity                                                    __NA           tCO2/unit
     __NA        Per value added carbon intensity                                                    __NA  kgCO2/USD PPP 2015
     __NA  Per services employee carbon intensity                                                    __NA       tCO2/employee
     __NA    Per physical output carbon intensity                                                    __NA              tCO2/t
     __NA   Passenger-kilometres carbon intensity                                                    __NA           kgCO2/pkm
     __NA     Vehicle-kilometres carbon intensity                                                    __NA           kgCO2/vkm
     __NA       Tonne-kilometres carbon intensity                                                    __NA           kgCO2/tkm
"""  # noqa: E501

#: Mapping of weights to variables used as weights for weighted averaging.
#:
#: .. todo:: Replace with tests showing usage of :func:`wavg`.
WAVG_MAP = {
    "Fuel intensity": "vehicle-kilometres",
    "Passenger load factor": "vehicle-kilometres",
    "Vehicle use": "vehicle stock",
    "Vehicle-kilometres energy intensity": "vehicle-kilometres",
    "Freight load factor": "vehicle-kilometres",
    # Rest of variables are Weighted with population:
    # "Passenger-kilometres per capita"
    # "Per capita energy intensity"
    # "Vehicle-kilometres per capita"
    # "Tonne-kilometres per capita"
    # TODO The following should not be weighted with population
    # "Tonne-kilometres energy intensity": np.average,
    # "Passenger-kilometres energy intensity": np.average,
}


@register_source
class IEA_EEI(ExoDataSource):
    """Provider of exogenous data from the IEA Energy Efficiency Indicators source."""

    @dataclass
    class Options(BaseOptions):
        #: By default, do not aggregate.
        aggregate: bool = False

        #: By default, do not interpolate.
        interpolate: bool = False

        #: Name of a :class:`.Key` containing a mapping for
        #: :func:`genno.operator.broadcast_map`.
        broadcast_map: Optional["Key"] = None

        #: Add a task with the key "plot IEA_EEI debug" to generate diagnostic plot
        #: using :class:`.Plot`.
        plot: bool = False

    options: Options

    where = ["local", "private"]

    def __init__(self, *args, **kwargs) -> None:
        opt = self.options = self.Options.from_args(self, *args, **kwargs)
        self.path = path_fallback(
            "iea",
            "eei",
            "Energyefficiencyindicators_2020-extended.xlsx",
            where=self._where(),
        )

        # Prepare query
        self.query = f"INDICATOR == {opt.measure!r}"

        # Determine whether to perform a weighted average operation
        self.weights = None

        # Construct .key
        super().__init__()

    def get(self) -> "AnyQuantity":
        from genno.operator import unique_units_from_dim

        tmp = (
            iea_eei_data_raw(self.path)
            .query(self.query)
            .rename(columns={"TIME_PERIOD": "y"})
        )

        # Identify dimensions
        # - Not the "value" or measure columns.
        # - Not columns filled entirely with "__NA".
        dims = [
            c
            for c, s in tmp.items()
            if (c not in {"value", "INDICATOR"} and set(s.unique()) != {"__NA"})
        ]

        return genno.Quantity(tmp.set_index(dims)["value"]).pipe(
            unique_units_from_dim, dim="UNIT_MEASURE"
        )

    def transform(self, c: "Computer", base_key: "Key") -> "Key":
        k = super().transform(c, base_key)

        if self.options.broadcast_map:
            k_map = Key(self.options.broadcast_map)
            rename = {k_map.dims[1]: k_map.dims[0]}
            c.add(k[0], "broadcast_map", k, k_map, rename=rename)
            k = k[0]

        if self.weights:
            # TODO Add operations for computing a weighted mean
            raise NotImplementedError

        if self.options.plot:
            # Path for debug output
            context: "Context" = c.graph["context"]
            debug_path = context.get_local_path("debug")
            debug_path.mkdir(parents=True, exist_ok=True)
            c.configure(output_dir=debug_path)

            c.add(f"plot {type(self).__name__} debug", Plot, k)

        return k


class Plot(genno.compat.plotnine.Plot):
    """Diagnostic plot of processed data."""

    basename = "IEA_EEI-data"

    static = [
        p9.aes(x="Year", y="Value", color="region"),
        p9.geom_line(),
        p9.facet_wrap("Variable", scales="free_y"),
        p9.labs(x="Year", y="mode"),
        p9.theme(subplots_adjust={"wspace": 0.15}, figure_size=(11, 9)),
    ]

    def generate(self, data):
        for mode, group_df in data.groupby("Mode/vehicle type"):
            yield p9.ggplot(group_df) + self.static + p9.ggtitle(mode)


SECTOR_MEASURE_EXPR = re.compile(r"(?P<SECTOR>[^ -]+)[ -](?P<MEASURE0>.+)")
MEASURE_UNIT_EXPR = re.compile(r"(?P<MEASURE1>.+) \((?P<UNIT_MEASURE>.+)\)")


def extract_measure_and_units(df: pd.DataFrame) -> pd.DataFrame:
    # Identify the column containing a units expression: either "Indicator" or "Product"
    measure_unit_col = ({"Indicator", "Product"} & set(df.columns)).pop()
    # - Split the identified column to UNIT_MEASURE and either INDICATOR or PRODUCT.
    # - Concatenate with the other columns.
    return pd.concat(
        [
            df.drop(measure_unit_col, axis=1),
            df[measure_unit_col]
            .str.extract(MEASURE_UNIT_EXPR)
            .rename(columns={"MEASURE1": measure_unit_col.upper()}),
        ],
        axis=1,
    )


def melt(df: pd.DataFrame) -> pd.DataFrame:
    """Melt on any dimensions."""
    index_cols = set(df.columns) & {
        "Activity",
        "Country",
        "End use",
        "INDICATOR",
        "MEASURE",
        "Mode/vehicle type",
        "PRODUCT",
        "SECTOR",
        "Subsector",
        "UNIT_MEASURE",
    }
    return df.melt(id_vars=sorted(index_cols), var_name="TIME_PERIOD")


@cached
def iea_eei_data_raw(path, non_iso_3166: Literal["keep", "discard"] = "discard"):
    from message_ix_models.util.pycountry import iso_3166_alpha_3

    xf = pd.ExcelFile(path)

    dfs = []
    for sheet_name in xf.sheet_names:
        # Parse the sheet name
        match = SECTOR_MEASURE_EXPR.fullmatch(sheet_name)
        if match is None:
            continue

        # Preserve the sector and/or measure ID from the sheet name
        s, m = match.groups()
        assign: dict[str, str] = dict()
        if s not in ("Activity",):
            assign.update(SECTOR=s.lower())
        if m in ("Energy", "Emissions"):
            assign.update(MEASURE=m.lower())

        # - Read the sheet.
        # - Drop rows containing only null values.
        # - Right-strip whitespaces from columns containing strings.
        # - Assign sector and/or measure ID.
        # - Extract units.
        # - Melt from wide to long layout.
        # - Drop null values.
        df = (
            xf.parse(sheet_name, header=1, na_values="..")
            .dropna(how="all")
            .apply(lambda col: col.str.rstrip() if col.dtype == object else col)
            .assign(**assign)
            .pipe(extract_measure_and_units)
            # .replace(REPLACE)
            .pipe(melt)
            .dropna(subset="value")
        )
        assert not df.isna().any(axis=None)
        dfs.append(df)

    return (
        pd.concat(dfs)
        .fillna("__NA")
        .assign(n=lambda df: df["Country"].apply(iso_3166_alpha_3))
        .drop("Country", axis=1)
    )


def wavg(measure: str, df: pd.DataFrame, weight_data: pd.DataFrame) -> pd.DataFrame:
    """Perform masked & weighted average for `measure` in `df`, using `weight_data`.

    .. todo:: Replace this with usage of genno; add tests.

    :data:`.WAVG_MAP` is used to select a data from `weight_data` appropriate for
    weighting `measure`: either "population", "vehicle stock" or "vehicle-kilometres*.
    If the measure to be used for weights is all NaNs, then "population" is used as a
    fallback as weight.

    The weighted average is performed by grouping `df` on the "region", "year", and
    "Mode/vehicle type" dimensions, i.e. the values returned are averages weighted
    within these groups.

    Parameters
    ----------
    measure : str
        Name of measure contained in `df`.
    df : pandas.DataFrame
        Data to be aggregated.
    weight_data : pandas.DataFrame.
        Data source for weights.

    Returns
    -------
    pandas.DataFrame
    """
    # Choose the measure for weights using `WAVG_MAP`.
    weights = WAVG_MAP.get(measure, "population")
    if weight_data[weights]["value"].isna().all():
        # If variable to be used for weights is all NaNs, then use population as weights
        # since pop data is available in all cases
        weights = "population"

    # Align the data and the weights into a single data frame
    id_cols = ["region", "year", "Mode/vehicle type"]
    data = df.merge(
        weight_data[weights],
        on=list(filter(lambda c: c in weight_data[weights].columns, id_cols)),
    )

    units = data["units_x"].unique()
    assert 1 == len(units), units

    def _wavg(group):
        # Create masked arrays, masking NaNs from the weighted average computation:
        d = np.ma.masked_invalid(group["value_x"].values)
        w = np.ma.masked_invalid(group["value_y"].values)
        # Compute weighted average
        return np.ma.average(d, weights=w)

    # - Apply _wavg() to groups by `id_cols`.
    # - Return to a data frame.
    # - Re-insert "units" and "variable" columns.
    return (
        data.groupby(id_cols)
        .apply(_wavg)
        .rename("value")
        .reset_index()
        .assign(units=units[0], variable=measure)
    )
