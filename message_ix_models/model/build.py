import logging
from typing import Callable, Dict, Mapping

import pandas as pd
from ixmp.utils import maybe_check_out, maybe_commit
from message_ix import Scenario
from sdmx.model import Code

from message_ix_models.util import add_par_data, strip_par_data
from message_ix_models.util.scenarioinfo import ScenarioInfo

log = logging.getLogger(__name__)


def apply_spec(
    scenario: Scenario,
    spec: Mapping[str, ScenarioInfo],
    data: Callable = None,
    **options,
):
    """Apply `spec` to `scenario`.

    Parameters
    ----------
    spec
        A 'specification': :class:`dict` with 'require', 'remove', and 'add' keys and
        :class:`.ScenarioInfo` objects as values.
    data : callable, optional
        Function to add data to `scenario`. `data` can either manipulate the scenario
        directly, or return a :class:`dict` compatible with :func:`.add_par_data`.

    Other parameters
    ----------------
    dry_run : bool
        Don't modify `scenario`; only show what would be done. Default :obj:`False`.
        Exceptions will still be raised if the elements from ``spec['required']`` are
        missing; this serves as a check that the scenario has the required features for
        applying the spec.
    fast : bool
        Do not remove existing parameter data; increases speed on large scenarios.
    quiet : bool
        Only show log messages at level ``ERROR`` and higher. If :obj:`False` (default),
        show log messages at level ``DEBUG`` and higher.
    message : str
        Commit message.

    See also
    --------
    .add_par_data
    .strip_par_data
    .Code
    .ScenarioInfo
    """
    dry_run = options.get("dry_run", False)

    log.setLevel(logging.ERROR if options.get("quiet", False) else logging.DEBUG)

    if not dry_run:
        try:
            scenario.remove_solution()
        except ValueError:
            pass
        maybe_check_out(scenario)

    dump: Dict[str, pd.DataFrame] = {}  # Removed data

    for set_name in scenario.set_list():
        # Check whether this set is mentioned at all in the spec
        if 0 == sum(map(lambda info: len(info.set[set_name]), spec.values())):
            # Not mentioned; don't do anything
            continue

        log.info(f"Set {repr(set_name)}")

        # Base contents of the set
        base_set = scenario.set(set_name)
        # Unpack a multi-dimensional/indexed set to a list of tuples
        base = (
            list(base_set.itertuples(index=False))
            if isinstance(base_set, pd.DataFrame)
            else base_set.tolist()
        )

        log.info(f"  {len(base)} elements")
        # log.debug(', '.join(map(repr, base)))  # All elements; verbose

        # Check for required elements
        require = spec["require"].set[set_name]
        log.info(f"  Check {len(require)} required elements")

        # Raise an exception about the first missing element
        missing = list(filter(lambda e: e not in base, require))
        if len(missing):
            log.error(f"  {len(missing)} elements not found: {repr(missing)}")
            raise ValueError

        # Remove elements and associated parameter values
        remove = spec["remove"].set[set_name]
        for element in remove:
            msg = f"{repr(element)} and associated parameter elements"

            if options.get("fast", False):
                log.info(f"  Skip removing {msg} (fast=True)")
                continue

            log.info(f"  Remove {msg}")
            strip_par_data(scenario, set_name, element, dry_run=dry_run, dump=dump)

        # Add elements
        add = [] if dry_run else spec["add"].set[set_name]
        for element in add:
            scenario.add_set(
                set_name,
                element.id if isinstance(element, Code) else element,
            )

        if len(add):
            log.info(f"  Add {len(add)} element(s)")
            log.debug("  " + ", ".join(map(repr, add)))

        log.info("  ---")

    N_removed = sum(len(d) for d in dump.values())
    log.info(f"{N_removed} parameter elements removed")

    # Add units to the Platform before adding data
    for unit in spec["add"].set["unit"]:
        unit = unit if isinstance(unit, Code) else Code(id=unit, name=unit)
        log.info(f"Add unit {repr(unit)}")
        scenario.platform.add_unit(unit.id, comment=str(unit.name))

    # Add data
    if callable(data):
        result = data(scenario, dry_run=dry_run)
        if result:
            # `data` function returned some data; use add_par_data()
            add_par_data(scenario, result, dry_run=dry_run)

    # Finalize
    log.info("Commit results.")
    maybe_commit(
        scenario,
        condition=not dry_run,
        message=options.get("message", f"{__name__}.apply_spec()"),
    )
