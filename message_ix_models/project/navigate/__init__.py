"""NAVIGATE project."""
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, Generator, List, Literal, Mapping, Optional, Union

import ixmp
import yaml
from message_ix_models.util import as_codes
from sdmx.model.v21 import Annotation, Code

from message_data.projects.engage.workflow import PolicyConfig

log = logging.getLogger(__name__)

# Shorthand for use in `CLIMATE_POLICY`
_kw: Mapping = dict(
    reserve_margin=False,
    solve=dict(model="MESSAGE", solve_options=dict(barcrossalg=2)),
)

#: Mapping of climate policy labels to :class:`.engage.workflow.PolicyConfig` objects.
#:
#: - Some of the ``budget`` values were originally from
#:   :file:`message_data/projects/engage/config.yaml`, but have been updated using the
#:   results of diagnostic Ctax runs and the ENGAGE budget calculator
#:   :file:`message_data/projects/engage/doc/v4.1.7_T4.5_budget_calculation_r3.1.xlsx`.
#: - The values for ``low_dem_scen`` have no effect in the NAVIGATE workflow as
#:   currently implemented. In the :file:`engage/config.yaml`, they appear to form a
#:   "waterfall", with each successively lower budget referencing the previous, which
#:   seems to preclude running lower budgets without first running every larger budget.
CLIMATE_POLICY = {
    pc.label: pc
    for pc in (
        # No climate policy → no ENGAGE workflow steps
        PolicyConfig("NPi", steps=[], **_kw),
        # Only step 1. This item does not appear in the official NAVIGATE scenarios
        # list, but is used in EXTRA_SCENARIOS below.
        PolicyConfig("1000 Gt", steps=[1], budget=2449, **_kw),
        # All steps 1–3
        # Originally from an item labelled "1000" in engage/config.yaml
        PolicyConfig(
            "20C",
            # budget=2700,  # for T3.5, initial WP6 with 1150 Gt target
            budget=1931,  # for WP6 with 900 Gt target, using "check-budget"
            **_kw,
        ),
        # Originally from an item labelled "600" in engage/config.yaml. Current values
        # calculated based on Ctax runs with price of 1000 USD / t CO₂.
        PolicyConfig(
            "15C",
            # Achievable for non-ref demand-side scenarios aiming for 650 Gt:
            # budget=1357,
            # Achievable for -ref demand-side scenario aiming for 850 Gt:
            budget=1840,
            **_kw,
        ),
        # The following do not appear in the official NAVIGATE scenarios list, but are
        # used in EXTRA_SCENARIOS below.
        # From an item labelled "1600" in engage/config.yaml
        PolicyConfig("1600 Gt", steps=[1], budget=4162, **_kw),
        # From an item labelled "2000" in engage/config.yaml
        PolicyConfig("2000 Gt", steps=[1], budget=5320, **_kw),
    )
}

#: Special "20C" policy for Task 6.2:
#:
#: - Only ENGAGE step 3; i.e. not step 1 or 2.
#: - tax_emission_scenario changes the behaviour of engage.step_3: CO2 prices are
#:   retrieved from the indicated scenario.
CLIMATE_POLICY["20C T6.2"] = PolicyConfig(
    "20C",
    steps=[3],
    tax_emission_scenario=dict(scenario="NPi-Default_ENGAGE_20C_step-2"),
    **_kw,
)


# Common annotations for `EXTRA_SCENARIOS`
_A = {
    "act+tec": Annotation(id="navigate_T35_policy", text="act+tec"),
    "all": Annotation(id="navigate_T35_policy", text="act+ele+tec"),
    "ele": Annotation(id="navigate_T35_policy", text="ele"),
    "ref": Annotation(id="navigate_T35_policy", text=""),
    "T3.5": Annotation(id="navigate_task", text="T3.5"),
    "T6.1": Annotation(id="navigate_task", text="T6.1"),
    "advanced": Annotation(id="navigate_WP6_production", text="advanced"),
    "default": Annotation(id="navigate_WP6_production", text="default"),
}


def _anno(names: str, climate_policy) -> Dict[Literal["annotations"], List[Annotation]]:
    """Return the annotations given by `names` from :data:`_A`.

    Shorthand function used to prepare :data:`EXTRA_SCENARIOS`.
    """
    # Collect predefined annotations from _A
    result = [_A.get(name) for name in names.split()]

    # Add an annotation with the value of the `climate_policy` argument
    result.append(Annotation(id="navigate_climate_policy", text=climate_policy))

    # Return as a keyword argument
    return dict(annotations=result)


#: Extra scenario IDs not appearing in the authoritative NAVIGATE list per the workflow
#: repository.
#:
#: - ``NAV_Dem-15C-ref`` and ``NAV_Dem-20C-ref`` were added to the NAVIGATE list in
#:   iiasa/navigate-workflow#43, but with invalid "navigate_T35_policy" annotations.
#:   Retained here pending iiasa/navigate-workflow#47. cf. corresponding pin to an
#:   earlier commit in :file:`pytest.yaml` workflow.
#: - ``NAV_Dem-(* Gt|Ctax)-*``: diagnostic scenarios
#: - ``PC-*``: no-policy scenarios corresponding to WP6 scenarios.
EXTRA_SCENARIOS = [
    Code(id="NAV_Dem-1000 Gt-ref", **_anno("T3.5 ref", "1000 Gt")),
    Code(id="NAV_Dem-1600 Gt-ref", **_anno("T3.5 ref", "1600 Gt")),
    Code(id="NAV_Dem-2000 Gt-ref", **_anno("T3.5 ref", "2000 Gt")),
    Code(id="NAV_Dem-Ctax-ref", **_anno("T3.5 ref", "Ctax")),
    Code(id="NAV_Dem-Ctax-all", **_anno("T3.5 all", "Ctax")),
    Code(id="PC-NPi-Default", **_anno("T6.1 ref default", "NPi")),
    Code(id="PC-NPi-AdvPE", **_anno("T6.1 ele advanced", "NPi")),
    Code(id="PC-NPi-LowCE", **_anno("T6.1 act+tec default", "NPi")),
    Code(id="PC-NPi-AllEn", **_anno("T6.1 all advanced", "NPi")),
]


@lru_cache()
def _read() -> List[Code]:
    """Read the codes from the NAVIGATE workflow directory."""
    workflow_dir = Path(ixmp.config.get("navigate workflow dir")).expanduser().resolve()

    # Use the particular path scheme of Scenario Explorer config repositories
    path = workflow_dir.joinpath("definitions", "scenario", "scenarios.yaml")
    log.info(f"Read scenarios from {path}")
    with open(path) as f:
        _content = yaml.safe_load(f)

    # Transform into a form intelligible by as_codes()
    content: Dict[str, Union[str, Code]] = {}
    for item in _content:
        if isinstance(item, str):
            content[item] = Code(id=item, name=item)
        else:
            content.update(item)

    # Transform to a list of SDMX codes
    return as_codes(content)


def iter_scenario_codes(
    context, filters: Optional[Mapping] = None
) -> Generator[Code, None, None]:
    """Iterate over the scenarios defined in the ``navigate-workflow`` repository.

    Parameters
    ----------
    filters : dict (str -> any), optional
        Only codes with annotations that match these filters are returned.
    """
    filters = filters or dict()

    codes = _read()

    # Yield codes whose annotations match `filters`
    for code in codes + EXTRA_SCENARIOS:
        match = True
        for key, value in filters.items():
            try:
                if str(code.get_annotation(id=key).text) not in value:
                    match = False
                    break
            except KeyError:
                match = False
                break
        if match:
            yield code


@dataclass
class Config:
    """Configuration for NAVIGATE."""

    #: Other scenario from which to copy historical time series data for reporting.
    copy_ts: Dict = field(default_factory=dict)

    #: Target data structure for submission prep
    dsd: Literal["iiasa-ece", "navigate"] = "navigate"

    #: Single target scenario for :mod:`.navigate.workflow.generate_workflow`
    scenario: Optional[str] = None

    #: :data:`True` to use MESSAGEix-Transport (:mod:`.model.transport`) alongside
    #: MESSAGEix-Buildings and MESSAGEix-Materials.
    transport: bool = True
