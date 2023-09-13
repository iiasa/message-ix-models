from message_ix_models.tools.exo_data import (
    ExoDataSource,
    iamc_like_data_for_query,
    register_source,
)
from message_ix_models.util import private_data_path


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
        self.measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[source_kw.pop("measure")]

        # Store the model ID, if any
        self.model = source_kw.pop("model", None)

        if len(source_kw):
            raise ValueError(source_kw)

    def __call__(self):
        # Assemble a query string
        query = " and ".join(
            [
                f"Scenario == 'SSP{self.ssp_number} - Review Phase 1'",
                f"Variable == '{self.measure}'",
                f"Model == '{self.model}'" if self.model else "True",
            ]
        )

        path = private_data_path("ssp", "SSP-Review-Phase-1.csv.gz")
        assert path.exists(), "TODO handle the case where message_data is not insalled"

        return iamc_like_data_for_query(path, query)
