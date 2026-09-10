import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import genno
import pandas as pd
from genno import Key

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.util import MappingAdapter, cached, package_data_path

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import AnyQuantity

log = logging.getLogger(__name__)

#: Valid combinations of the `scenario` and `variant` options to
#: :class:`.EmissionFactor`.
SCENARIO_VARIANT = {
    ("SSP1", "L"),
    ("SSP1", "M"),
    ("SSP1", "VLHO"),
    ("SSP1", "VLLO"),
    ("SSP2", "H"),
    ("SSP2", "L"),
    ("SSP2", "M"),
    ("SSP2", "VLHO"),
    ("SSP2", "VLLO"),
    ("SSP3", "H"),
    ("SSP3", "M"),
    ("SSP3", "ML"),
    ("SSP5", "H"),
    ("SSP5", "HLE"),
    ("SSP5", "L"),
    ("SSP5", "ML"),
}


@register_source
class EmissionFactor(ExoDataSource):
    """Source of exogenous ``emission_factor`` data from GAINS."""

    @dataclass
    class Options(BaseOptions):
        #: ID of the target node code list.
        nodes: str = ""

        #: Target scenario.
        scenario: str = ""

        #: Target scenario variant.
        variant: str = ""

        def __post_init__(self) -> None:
            if (self.scenario, self.variant) not in SCENARIO_VARIANT:
                raise ValueError(
                    f"invalid combination: scenario={self.scenario}, "
                    f"variant={self.variant}"
                )

    options: Options
    key = Key("emission_factor:e-n-t-y:gains")
    path: "Path"

    def __init__(self, *args, **kwargs) -> None:
        self.options = self.Options.from_args(self, *args, **kwargs)

        self.path = package_data_path("gains", "emission_factor.csv.xz")
        assert self.path.exists(), f"Not found: {self.path}"
        log.info(f"GAINS emission_factor data from {self.path}")

        self.key = self.key + self.options.scenario + self.options.variant

    def get(self) -> "AnyQuantity":
        # Read data or return from cache
        return gains_data_for_query(
            self.path,
            f"scenario == {self.options.scenario!r} and "
            f"variant == {self.options.variant!r}",
        )

    @property
    def adapter(self) -> MappingAdapter:
        """Construct and return an adapter from GAINS to MESSAGEix regions."""
        from message_ix_models.util.node import create_maps

        # Retrieve the mapping from R12 to GAINS node IDs. This has exactly 1 entry
        # for each of the R12 regions.
        map = create_maps(self.options.nodes, "GAINS")[0]

        # - Invert the mapping, so GAINS node IDs in the source file are mapped to R12
        #   regions, with every region mapped-to once.
        # - Create the adapter.
        return MappingAdapter({"n": tuple((v, k) for k, v in map.items())})

    def transform(self, c: "Computer", base_key: Key) -> Key:
        # TODO Avoid this cross-module import in general-purpose code
        from message_ix_models.model.transport.util import EXTRAPOLATE

        k = base_key

        # Interpolate on "y" dimension
        c.add(k[0], "interpolate", k, "y::coords", **EXTRAPOLATE)

        # 'n' dimension: map from GAINS to MESSAGEix regions on the 'n' dimension
        c.add(self.key, lambda qty: self.adapter(qty), k[0])

        return self.key


@cached
def gains_data_for_query(path: "Path", query: str) -> "AnyQuantity":
    """Read and cache data from `path`."""
    from ixmp.report.common import RENAME_DIMS

    return genno.Quantity(
        pd.read_csv(path, skipinitialspace=True, comment="#")
        .query(query)
        .rename(columns=RENAME_DIMS | dict(scenario="s", variant="v", EMF30_DET="t"))
        .set_index(["e", "n", "t", "y"])["value"],
        units="kt / PJ",
    )
