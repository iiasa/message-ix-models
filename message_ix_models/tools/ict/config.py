from dataclasses import dataclass
from typing import Literal


@dataclass
class Config:
    """Configuration for :mod:`.ict`."""

    #: Last year of demand projection.
    #: if last year > 2050 an additional extrapolation is applied, which extends the
    #: projections beyond the original scope.
    lastyear: int = 2100

    #: Node code list / spatial resolution for which to project demand.
    #: This should correspond to the target scenario to which data is to be added.
    node: Literal["R11", "R12"] = "R12"

    #: Scenario for which to project ICT energy demand.
    #: "Mean" implies the arithmetic mean of Low and High.
    #: "Central" implies a benchmarking between Low and High,
    #:  based on the IEA's 2035 Base projection.
    scenario: Literal["Low", "Central", "Mean", "High"] = "Mean"

    #: Ratio of telecommunication infrastructure to data center energy demand
    tele_comm_ratio: float = 0.78

    #: SSP for which to project ICT energy demand.
    ssp: Literal["SSP1", "SSP2", "SSP3", "SSP4", "SSP5"] = "SSP2"

    def __post_init__(self):
        self.tele_comm_ratio = self.tc_ratio()

    def tc_ratio(self) -> float:
        ratios: dict[str, float] = {
            "High": 0.91,
            "Low": 0.78,
            "Mean": 0.78,
            "Central": 0.78,
        }
        return ratios.get(self.scenario, 0.78)

    def check(self):
        """Validate settings."""
        valid_nodes = {"R11", "R12"}
        if self.node not in valid_nodes:
            raise NotImplementedError(
                f"Cost projections for {self.node!r}; use one of {valid_nodes}"
            )
