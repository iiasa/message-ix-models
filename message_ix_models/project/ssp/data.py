import logging
from copy import copy

from message_ix_models.tools.exo_data import (
    ExoDataSource,
    iamc_like_data_for_query,
    register_source,
)
from message_ix_models.util import (
    HAS_MESSAGE_DATA,
    package_data_path,
    private_data_path,
)

log = logging.getLogger(__name__)


@register_source
class SSPUpdate(ExoDataSource):
    """Provider of exogenous data from the SSP Update database."""

    id = "SSP update"

    def __init__(self, source, source_kw):
        s = "ICONICS:SSP(2024)."
        if not source.startswith(s):
            raise ValueError(source)

        *parts, self.ssp_number = source.partition(s)

        # Map the `measure` keyword to a string appearing in the data
        _kw = copy(source_kw)
        self.measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[_kw.pop("measure")]

        # Store the model ID, if any
        self.model = _kw.pop("model", None)

        if len(_kw):
            raise ValueError(_kw)

    def __call__(self):
        # Assemble a query string
        query = " and ".join(
            [
                f"Scenario == 'SSP{self.ssp_number} - Review Phase 1'",
                f"Variable == '{self.measure}'",
                f"Model == '{self.model}'" if self.model else "True",
            ]
        )

        parts = ("ssp", "SSP-Review-Phase-1.csv.gz")
        if HAS_MESSAGE_DATA:
            path = private_data_path(*parts)
        else:
            path = package_data_path("test", *parts)
            log.warning(f"Reading random data from {path}")

        return iamc_like_data_for_query(path, query)
