"""Handle data from the Global Energy Assessment (GEA)."""

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.tools.iamc import iamc_like_data_for_query
from message_ix_models.util import package_data_path, path_fallback

if TYPE_CHECKING:
    from genno import Computer, Key
    from genno.types import AnyQuantity
    from sdmx.model.common import Code

log = logging.getLogger(__name__)


@register_source
class GEA(ExoDataSource):
    """Provider of exogenous data from the GEA data source.

    Per :attr:`Options.measure`, see the source data for details.
    """

    @dataclass
    class Options(BaseOptions):
        #: By default, do not aggregate.
        aggregate: bool = False

        #: By default, do not interpolate.
        interpolate: bool = False

        #: Model name.
        model: str = ""

        #: Scenario name.
        scenario: str = ""

    options: Options

    where = ["private"]

    def __init__(self, *args, **kwargs) -> None:
        opt = self.options = self.Options.from_args(self, *args, **kwargs)

        # Set .key
        super().__init__()

        # Check for a valid (model, scenario) combination
        check = (opt.model, opt.scenario)
        if check not in get_model_scenario():
            log.error(f"No data for (model, scenario) = {check!r}")
            raise ValueError(check)

        # Identify input data path
        self.path = path_fallback(
            "gea", "GEADB_ARCHIVE_20171108.zip", where=self._where()
        )

        # Assemble query
        self.query = (
            f"MODEL == {opt.model!r}" if opt.model else "True"
        ) + f" and SCENARIO == {opt.scenario!r} and VARIABLE == {opt.measure!r}"

    def get(self) -> "AnyQuantity":
        return iamc_like_data_for_query(
            self.path,
            self.query,
            archive_member=self.path.with_suffix(".csv").name,
            non_iso_3166="keep",
        )

    def transform(self, c: "Computer", base_key: "Key") -> "Key":
        """Prepare `c` to transform raw data from `base_key`.

        Compared to :meth:`.ExoDataSource.transform`, this version:

        - Does not perform interpolation.
        """
        k = super().transform(c, base_key)

        # TODO Incorporate the following
        def adapt_nodes(nodes: list["Code"]) -> dict[str, str]:
            """Convert `nodes` with IDs e.g. “R11_AFR” to a mapping.

            From e.g. “AFR” to “R11_AFR”.
            """
            return {r.split("_")[-1]: r for r in map(str, nodes)}

        return k


@lru_cache
def get_model_scenario() -> set[tuple[str, str]]:
    """Return a set of valid GEA (model name, scenario name) combinations.

    These are read from :file:`data/gea/model-scenario.json`.

    .. todo:: Convert to :class:`~sdmx.model.common.Codelist`.
    """
    import json

    with open(package_data_path("gea", "model-scenario.json")) as f:
        return set((v["model"], v["scenario"]) for v in json.load(f))
