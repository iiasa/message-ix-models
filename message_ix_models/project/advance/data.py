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

__all__ = [
    "ADVANCE",
]

log = logging.getLogger(__name__)


@register_source
class ADVANCE(ExoDataSource):
    """Provider of exogenous data from the ADVANCE project database.

    To use data from this source, call :func:`.exo_data.prepare_computer` with the
    arguments:

    - `source`: "ADVANCE"
    - `source_kw` including:

      - "model": one of:

          - MESSAGE
          - (possibly others)

      - "measure": A value from the VARIABLE code list.

      - "scenario": one of:

          - ADV3TRAr2_Base
          - (possibly others)

    Example
    -------
    >>> keys = prepare_computer(
    ...     context,
    ...     computer,
    ...     source="ADVANCE",
    ...     source_kw=dict(
    ...         measure="Transport|Service demand|Road|Freight",
    ...         model="MESSAGE",
    ...         scenario="ADV3TRAr2_Base",
    ...     ),
    ... )
    >>> result = computer.get(keys[0])
    """

    id = "ADVANCE"

    def __init__(self, source, source_kw):
        if not source == self.id:
            raise ValueError(source)

        # Map the `measure` keyword to a string appearing in the data
        _kw = copy(source_kw)
        measure = _kw.pop("measure")
        self.variable = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }.get(measure, measure)

        # Store the model and scenario ID
        self.model = _kw.pop("model", None)
        self.scenario = _kw.pop("scenario", None)

        if len(_kw):
            raise ValueError(_kw)

    def __call__(self):
        # Assemble a query string
        query = " and ".join(
            [
                f"SCENARIO == {self.scenario!r}",
                f"VARIABLE == {self.variable!r}",
                f"MODEL == {self.model!r}" if self.model else "True",
            ]
        )
        log.debug(query)

        # Expected location of the ADVANCE WP2 data snapshot.
        parts = ("advance", "advance_compare_20171018-134445.csv.zip")
        if HAS_MESSAGE_DATA:
            path = private_data_path(*parts)
        else:
            path = package_data_path("test", *parts)
            log.warning(f"Reading random data from {path}")

        return iamc_like_data_for_query(
            path,
            query,
            archive_member="advance_compare_20171018-134445.csv",
            non_iso_3166="keep",
        )
