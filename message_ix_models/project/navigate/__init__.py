"""NAVIGATE project."""
import logging
from pathlib import Path
from typing import Generator, Mapping, Optional

import yaml
from message_ix_models.util import as_codes
from sdmx.model import Annotation, Code

log = logging.getLogger(__name__)

EXTRA_SCENARIOS = [
    Code(
        id="NAV_Dem-20C-ref",
        annotations=[
            Annotation(id="navigate-climate-policy", text="20C"),
            Annotation(id="navigate-T3.5-policy", text=""),
            Annotation(id="navigate-task", text="T3.5"),
        ],
    ),
]


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
    codes = as_codes(content)

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
