from dataclasses import dataclass, field
from typing import Literal, Optional

from message_ix_models import ScenarioInfo


@dataclass
class Config:
    """Configuration for :mod:`.costs`.

    On creation:

    - If not given, :attr:`.ref_region` is set based on :attr:`.node` using, for
      instance, :py:`ref_region="R12_NAM"` for :py:`node="R12"`.
    """

    #: Base year for projected costs.
    #: This is the first year for which cost reductions/decay are calculated.
    #: If the base year is greater than y0 (first model year),
    #: then the costs are assumed to be the same from y0 to base_year.
    base_year: int = 2025

    #: Year of convergence; used when :attr:`.method` is "convergence". This is the year
    #: by which costs in all regions should converge to the reference region's costs.
    #: See :func:`.create_projections_converge`.
    convergence_year: int = 2050

    #: Final year for projections. Note that the default is the same as the final
    #: model year of 2110 commonly used in MESSAGEix-GLOBIOM (:doc:`/pkg-data/year`).
    final_year: int = 2110

    #: Rate of exponential growth (positive values) or decrease of fixed operating and
    #: maintenance costs over time. The default of 0 implies no change over time.
    #: If the rate is 0.025, for example, that implies exponential growth at a
    #: rate of 2.5% per year; or :py:`(1 + 0.025) ** N` for a period of length N.
    fom_rate: float = 0

    #: Format of output from :func:`.create_cost_projections`. One of:
    #:
    #: - "iamc": IAMC time series data structure.
    #: - "message": :mod:`message_ix` parameter data.
    format: Literal["iamc", "message"] = "message"

    #: Node code list / spatial resolution for which to project costs.
    #: This should correspond to the target scenario to which data is to be added.
    node: Literal["R11", "R12", "R20"] = "R12"

    #: Method for projecting costs in non-reference regions. One of:
    #:
    #: - "constant": uses :func:`.create_projections_constant`.
    #: - "convergence": uses :func:`.create_projections_converge`.
    #: - "gdp": uses :func:`.create_projections_gdp`.
    method: Literal["constant", "convergence", "gdp"] = "gdp"

    #: Model variant for which to project costs.
    module: Literal["energy", "materials", "cooling"] = "energy"

    #: Use vintages.
    #:
    #: If True, for each vintage, the fixed O&M costs will be calculated as a
    #: ratio of the investment costs, that decreases by the rate of :attr:`fom_rate`.
    #: If False, the fix_cost is the ratio of the investment cost for each year_act.
    #: In this case, the fix_cost is the same for all vintages of the same year_act.
    use_vintages: bool = False

    #: .. todo:: Document the meaning of this setting.
    pre_last_year_rate: float = 0.01

    #: Reference region. If not given, :py:`"{node}_NAM"`` for a given :attr:`.node`.
    #: This default **must** be overridden if there is no such node.
    ref_region: Optional[str] = None

    #: Set of SSPs referenced by :attr:`scenario`. One of:
    #:
    #: - "original": :obj:`SSP_2017`
    #: - "updated": :obj:`SSP_2024`
    #: - "all": both of the above.
    scenario_version: Literal["original", "updated", "all"] = "updated"

    #: Scenario(s) for which to project costs. "all" implies the set of all the other
    #: values, meaning that costs are projected for all scenarios.
    scenario: Literal["all", "LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"] = "all"

    # Internal: Scenario Info object used for y0, Y, seq_years
    _info: ScenarioInfo = field(default_factory=ScenarioInfo)

    def __post_init__(self):
        from message_ix_models.model.structure import get_codes

        if self.ref_region is None:
            self.ref_region = f"{self.node}_NAM"

        # Default periods 'B'
        self._info.year_from_codes(get_codes("year/B"))

    @property
    def y0(self) -> int:
        """The first model period."""
        return self._info.y0

    @property
    def Y(self) -> list[int]:
        """List of model periods."""
        return self._info.Y

    @property
    def seq_years(self) -> list[int]:
        """Similar to :attr:`Y`.

        This list of periods differs in that it:

        1. Excludes periods after :attr:`.final_year`.
        2. Includes 5-year periods even when these are not in :attr:`.Y`.
        """
        return list(range(self.y0, self.final_year + 1, 5))

    def check(self):
        """Validate settings."""
        valid_nodes = {"R11", "R12", "R20"}
        if self.node not in valid_nodes:
            raise NotImplementedError(
                f"Cost projections for {self.node!r}; use one of {valid_nodes}"
            )
