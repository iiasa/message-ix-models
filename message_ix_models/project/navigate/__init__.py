"""NAVIGATE project."""
import logging
from functools import lru_cache
from pathlib import Path
from typing import Generator, List, Mapping, Optional

import yaml
from message_ix_models.util import as_codes
from sdmx.model import Annotation, Code

from message_data.projects.engage.workflow import PolicyConfig

log = logging.getLogger(__name__)

#: Mapping of climate policy labels to :class:`.engage.workflow.PolicyConfig` objects.
#:
#: Some of the values are from engage/config.yaml.
#:
#: .. todo:: The low_dem_scen values appear to form a "waterfall", with each
#:    successively lower budget referencing the previous. Investigate how to run only
#:    some budgets without the previous ones.
_kw = dict(
    reserve_margin=False,
    solve=dict(model="MESSAGE", solve_options=dict(barcrossalg=2)),
)
CLIMATE_POLICY = {
    pc.label: pc
    for pc in (
        # No climate policy → no ENGAGE workflow steps
        PolicyConfig("NPi", steps=[], **_kw),
        # Only step 1. This item does not appear in the official NAVIGATE scenarios
        # list, but is used in EXTRA_SCENARIOS below.
        PolicyConfig(
            "1000 Gt",
            steps=[1],
            budget=2449,
            low_dem_scen="EN_NPi2020_1200_step1",
            **_kw,
        ),
        # All steps 1–3
        # From an item labelled "1000" in engage/config.yaml
        PolicyConfig("20C", budget=2449, low_dem_scen="EN_NPi2020_1200_step1", **_kw),
        # From an item labelled "600" in engage/config.yaml
        PolicyConfig("15C", budget=1288, low_dem_scen="EN_NPi2020_700_step1", **_kw),
        #
        # The following do not appear in the official NAVIGATE scenarios list, but are
        # used in EXTRA_SCENARIOS below.
        # From an item labelled "1600" in engage/config.yaml
        PolicyConfig(
            "1600 Gt",
            steps=[1],
            budget=4162,
            low_dem_scen="EN_NPi2020_1800_step1",
            **_kw,
        ),
        # From an item labelled "2000" in engage/config.yaml
        PolicyConfig(
            "2000 Gt",
            steps=[1],
            budget=5320,
            low_dem_scen="EN_NPi2020_2500_step1",
            **_kw,
        ),
    )
}
# Placeholder
CLIMATE_POLICY["Ctax"] = None


# Common annotations for EXTRA_SCENARIOS
_A = [
    Annotation(id="navigate-T3.5-policy", text=""),
    Annotation(id="navigate-task", text="T3.5"),
]

#: Extra scenario IDs not appearing in the authoritative NAVIGATE list per the workflow
#: repository. These each have the same 3 annotations.
EXTRA_SCENARIOS = [
    Code(
        id="NAV_Dem-20C-ref",
        annotations=[Annotation(id="navigate-climate-policy", text="20C")] + _A,
    ),
    Code(
        id="NAV_Dem-1000 Gt-ref",
        annotations=[Annotation(id="navigate-climate-policy", text="1000 Gt")] + _A,
    ),
    Code(
        id="NAV_Dem-1600 Gt-ref",
        annotations=[Annotation(id="navigate-climate-policy", text="1600 Gt")] + _A,
    ),
    Code(
        id="NAV_Dem-2000 Gt-ref",
        annotations=[Annotation(id="navigate-climate-policy", text="2000 Gt")] + _A,
    ),
    Code(
        id="NAV_Dem-Ctax-ref",
        annotations=[Annotation(id="navigate-climate-policy", text="Ctax")] + _A,
    ),
]


@lru_cache()
def _read() -> List[Code]:
    """Read the codes from the NAVIGATE workflow directory."""
    # FIXME read this path from config
    workflow_dir = Path("~/vc/iiasa/navigate-workflow").expanduser()

    # Use the particular path scheme of Scenario Explorer config repositories
    path = workflow_dir.joinpath("definitions", "scenario", "scenarios.yaml")
    log.info(f"Read scenarios from {path}")
    with open(path) as f:
        _content = yaml.safe_load(f)

    # Transform into a form intelligible by as_codes()
    content = {}
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
                if str(code.get_annotation(id=key).text) != value:
                    match = False
                    break
            except KeyError:
                match = False
                break
        if match:
            yield code
