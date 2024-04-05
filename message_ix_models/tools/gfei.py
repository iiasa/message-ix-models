"""Prepare fuel economy data from the GFEI model via GFEI_FE_by_Powertrain_2017.csv"""

import logging
from typing import TYPE_CHECKING

import genno
import plotnine as p9

from message_ix_models.tools.exo_data import ExoDataSource, register_source
from message_ix_models.util import path_fallback

if TYPE_CHECKING:
    from genno import Computer

    from message_ix_models import Context

log = logging.getLogger(__name__)

#: Technology map to MESSAGEix-Transport technologies.
#:
#: .. todo:: Store and use this elsewhere.
#:
#: - Flexfuel: Reference is ICEs vehicle using methanol/synthetic fossil liquids.
#: - Missing in the mapping: ``IGH_ghyb``, ``ICE_L_ptrp``.
#:
#: .. todo:: Deal with diesel (ATM assumed to be ICE using biofuels) and LPG powertrains
#:    (so far, it is ICE_lpg, not present in MESSAGEix-Transport). Also, category
#:    ``Hybrid`` should be extended to the list of three modes: "IAHe_ptrp",
#:    "IAHm_ptrp", "ICH_chyb".
TECH_MAP = {
    "Diesel": "ICAe_ffv",
    "Flexfuel": "ICAm_ptrp",
    "Hybrid": "ICH_chyb",
    "Petrol": "ICE_conv",
    "Electric": "ELC_100",
    "LPG": "ICE_lpg",
    "Plug-in": "PHEV_ptrp",
    "CNG": "ICE_nga",
    "Hydrogen": "HFC_ptrp",
    "Unspec.": None,
}


@register_source
class GFEI(ExoDataSource):
    """Read Global Fuel Economy Initiative data for 2017.

    Country-level data is retrieved from the underlying data of `"Figure 37. Fuel
    consumption range by type of powertrain and vehicle size, 2017"` in
    https://theicct.org/publications/gfei-tech-policy-drivers-2005-2017, in which:

    - Values correspond exclusively to new vehicle registrations in 2017.
    - Units are in Lge/100 km, converted into MJ/km.

    Data is processed into a :class:`pandas.DataFrame` with columns including modes,
    country names and their respective ISO codes, MESSAGEix region and units.

    Parameters
    ----------
    context : .Context
        Information about target Scenario.
    plot : bool, optional
        If ``True``, plots per mode will be generated in folder /debug.
    """

    id = "GFEI"

    def __init__(self, source, source_kw):
        if source != self.id:
            raise ValueError(source)

        self.aggregate = source_kw.pop("aggregate", False)
        self.plot = source_kw.pop("plot", False)

        self.raise_on_extra_kw(source_kw)

        # Set the name of the returned quantity
        self.name = "fuel economy"

        self.path = path_fallback(
            "transport", "GFEI_FE_by_Powertrain_2017.csv", where="private test"
        )
        if "test" in self.path.parts:
            log.warning(f"Reading random data from {self.path}")

    def __call__(self):
        import genno.operator

        # - Read the CSV file, rename columns.
        # - Assign the y value.
        # - Convert units.
        return (
            genno.operator.load_file(
                self.path, dims={"Country": "n", "FuelTypeReduced": "t"}
            )
            .pipe(lambda qty: qty.expand_dims(y=[2017]))
            .pipe(genno.operator.convert_units, "MJ / (vehicle km)")
        )

    def transform(self, c: "Computer", base_key: genno.Key) -> genno.Key:
        """Prepare `c` to transform raw data from `base_key`.

        Unlike the base class version, this implementation only adds the aggregation
        step if :attr:`.aggregate` is :any:`True`.
        """
        ks = genno.KeySeq(base_key)

        k = ks.base
        if self.aggregate:
            # Aggregate
            k = c.add(ks[1], "aggregate", k, "n::groups", keep=False)

        # TODO: missing to perform weighted average for countries in the same regions,
        #  based on vehicle stocks form get_eei_data(). This could be done by retrieving
        #  the "Activity" DataFrame from the returned dict, otherwise, I could be added
        #  as a genno calculation.

        if self.plot:
            # Path for debug output
            context: "Context" = c.graph["context"]
            debug_path = context.get_local_path("debug")
            debug_path.mkdir(parents=True, exist_ok=True)
            c.configure(output_dir=debug_path)

            c.add(f"plot {self.id} debug", Plot, k)

        return k


class Plot(genno.compat.plotnine.Plot):
    """Plot values from file."""

    basename = "GFEI-fuel-economy-t"

    static = [
        p9.aes(x="n", y="value"),
        p9.geom_col(stat="identity", position="dodge"),
        p9.theme(axis_text_x=p9.element_text(rotation=90, hjust=1)),
    ]

    def generate(self, data):
        for technology, group_df in data.groupby("t"):
            yield p9.ggplot(group_df) + self.static + p9.ggtitle(technology)
