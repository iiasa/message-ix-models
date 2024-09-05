"""Handle data from the Global Energy Assessment (GEA)."""

import logging
from functools import lru_cache
from typing import TYPE_CHECKING

import genno

from message_ix_models.tools.exo_data import ExoDataSource, register_source
from message_ix_models.tools.iamc import iamc_like_data_for_query
from message_ix_models.util import package_data_path, path_fallback

if TYPE_CHECKING:
    from sdmx.model.common import Code

    from message_ix_models import Computer

log = logging.getLogger(__name__)


@register_source
class GEA(ExoDataSource):
    """Provider of exogenous data from the GEA data source.

    To use data from this source, call :func:`.exo_data.prepare_computer` with the
    arguments:

    - `source`: "GEA".
    - `source_kw` including:

      - `model`, `scenario`: model name and scenario name. See
        :func:`.get_model_scenario`.
      - `measure`: See the source data for details.
      - `aggregate`, `interpolate`: see :meth:`.ExoDataSource.transform`.
    """

    id = "GEA"

    #: By default, do not aggregate.
    aggregate = False

    #: By default, do not interpolate.
    interpolate = False

    def __init__(self, source, source_kw):
        if source != self.id:
            raise ValueError(source)

        # Pieces for query
        model = source_kw.pop("model", None)
        scenario = source_kw.pop("scenario", None)
        self.measure = variable = source_kw.pop("measure")

        # Check for a valid (model, scenario) combination
        check = (model, scenario)
        if check not in get_model_scenario():
            log.error(f"No data for (model, scenario) = {check!r}")
            raise ValueError(check)

        self.raise_on_extra_kw(source_kw)

        # Identify input data path
        self.path = path_fallback(
            "gea", "GEADB_ARCHIVE_20171108.zip", where="private test"
        )
        if "test" in self.path.parts:
            log.warning(f"Reading random data from {self.path}")

        # Assemble query
        self.query = " and ".join(
            [
                f"MODEL == {model!r}" if model else "True",
                f"SCENARIO == {scenario!r}",
                f"VARIABLE == {variable!r}",
            ]
        )

    def __call__(self):
        return iamc_like_data_for_query(
            self.path,
            self.query,
            archive_member=self.path.with_suffix(".csv").name,
            non_iso_3166="keep",
        )

    def transform(self, c: "Computer", base_key: genno.Key) -> genno.Key:
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
    """
    import json

    with open(package_data_path("gea", "model-scenario.json")) as f:
        return set((v["model"], v["scenario"]) for v in json.load(f))
