import logging
from functools import lru_cache
from typing import TYPE_CHECKING, Dict, List, Set, Tuple

import genno

from message_ix_models.tools.exo_data import (
    ExoDataSource,
    iamc_like_data_for_query,
    register_source,
)
from message_ix_models.util import package_data_path, private_data_path

if TYPE_CHECKING:
    from sdmx.model.common import Code

    from message_ix_models import Computer

log = logging.getLogger(__name__)


@register_source
class GEA(ExoDataSource):
    """Return Global Energy Assessment data.

    The data is stored in :file:`data/gea/` using a snapshot from the GEA database as
    of 2017-11-08.

    Parameters
    ----------
    query : str, optional
        Used with :meth:`pandas.DataFrame.query` to limit the returned values.
    """

    id = "GEA"

    def __init__(self, source, source_kw):
        if source != self.id:
            raise ValueError(source)

        # Pieces for query
        model = source_kw.pop("model", None)
        scenario = source_kw.pop("scenario", None)
        variable = source_kw.pop("measure")

        # Check for a valid (model, scenario) combination
        check = (model, scenario)
        if check not in get_model_scenario():
            log.error(f"No data for (model, scenario) = {check!r}")
            raise ValueError(check)

        # For transform()
        self.aggregate = source_kw.pop("aggregate", False)

        self.raise_on_extra_kw(source_kw)

        # Identify input data path
        self.path = private_data_path("gea", "GEADB_ARCHIVE_20171108.zip")
        if not self.path.exists():
            log.error(f"Not found: {self.path}")
            raise ValueError(self.path)

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

        Unlike the base class version, this implementation only adds the aggregation
        step if :attr:`.aggregate` is :any:`True`.
        """
        ks = genno.KeySeq(base_key)

        k = ks.base
        if self.aggregate:
            # Aggregate
            k = c.add(ks[1], "aggregate", k, "n::groups", keep=False)

        # TODO Incorporate the following
        def adapt_nodes(nodes: List["Code"]) -> Dict[str, str]:
            """Convert `nodes` with IDs e.g. “R11_AFR” to a mapping.

            From e.g. “AFR” to “R11_AFR”.
            """
            return {r.split("_")[-1]: r for r in map(str, nodes)}

        return k


@lru_cache
def get_model_scenario() -> Set[Tuple[str, str]]:
    """Return a mapping from (full) GEA scenario names to (short) labels/IDs.

    These are read from :file:`data/gea/scenario.yaml`.
    """
    import json

    with open(package_data_path("gea", "model-scenario.json")) as f:
        return set((v["model"], v["scenario"]) for v in json.load(f))
