"""Prepare base models from snapshot data."""

import logging
from pathlib import Path

import pandas as pd
from message_ix import Scenario
from message_ix.models import MACRO
from tqdm import tqdm

from message_ix_models import Spec
from message_ix_models.util import minimum_version
from message_ix_models.util.pooch import SOURCE, fetch

from .build import apply_spec
from .structure import get_codes

log = logging.getLogger(__name__)


def unpack(path: Path) -> Path:
    """Unpack :ref:`ixmp-format Excel file <ixmp:excel-data-format>` at `path`.

    The file is unpacked into a directory with the same name stem as the file (that is,
    without the :file:`.xlsx` suffix). In this directory are created:

    - One :file:`.csv.gz` file for each MESSAGE and/or MACRO parameter.
    - One file :file:`sets.xlsx` with only the :mod:`ixmp` sets, and no parameter data.

    If the files exist, they are not updated.
    To force re-unpacking, delete the files.

    Returns
    -------
    Path
        Path to the directory containing the unpacked files.
    """
    assert path.suffix == ".xlsx"
    base = path.with_suffix("")
    base.mkdir(exist_ok=True)

    # Get item name -> ixmp type mapping as a pd.Series
    xf = pd.ExcelFile(path, engine="openpyxl")
    name_type = xf.parse("ix_type_mapping")

    # Copied exactly from ixmp.backend.io
    def parse_item_sheets(name):
        """Read data for item *name*, possibly across multiple sheets."""
        dfs = [xf.parse(name)]

        # Collect data from repeated sheets due to max_row limit
        for x in filter(lambda n: n.startswith(name + "("), xf.sheet_names):
            dfs.append(xf.parse(x))  # pragma: no cover

        # Concatenate once and return
        return pd.concat(dfs, axis=0, ignore_index=True)

    sets_path = base.joinpath("sets.xlsx")
    sets_path.unlink(missing_ok=True)

    with pd.ExcelWriter(sets_path, engine="openpyxl") as ew:
        for _, (name, ix_type) in tqdm(name_type.iterrows()):
            item_path = base.joinpath(f"{name}.csv.gz")
            if item_path.exists():
                continue

            df = parse_item_sheets(name)

            if ix_type == "set":
                df.to_excel(ew, sheet_name=name, index=False)
            else:  # pragma: no cover
                df.to_csv(item_path, index=False)

        name_type.query("ix_type == 'set'").to_excel(ew, sheet_name="ix_type_mapping")

    return base


def read_excel(scenario: Scenario, path: Path) -> None:
    """Similar to :meth:`.Scenario.read_excel`, but using :func:`unpack`."""
    base = unpack(path)

    scenario.read_excel(base.joinpath("sets.xlsx"))

    parameters = set(scenario.par_list())

    with scenario.transact(f"Read snapshot data from {path}"):
        for p in base.glob("*.csv.gz"):
            name = p.name.split(".")[0]

            if name not in parameters:
                continue  # Variable or equation data: don't read

            data = pd.read_csv(p)

            # Correct units
            if name == "inv_cost":
                data.replace({"unit": {"USD_2005/t ": "USD_2005/t"}}, inplace=True)

            scenario.add_par(name, data)


@minimum_version("message_ix 3.5")
def load(scenario: Scenario, snapshot_id: int) -> None:
    """Fetch and load snapshot with ID `snapshot_id` into `scenario`.

    See also
    --------
    SNAPSHOTS
    """
    snapshot_name = f"snapshot-{snapshot_id}"
    path, *_ = fetch(**SOURCE[snapshot_name], extra_cache_path=snapshot_name)

    if snapshot_id == 0:
        # Needs correction of units
        # Add units
        spec = Spec()
        spec.add.set["unit"] = get_codes("unit/snapshot")
        apply_spec(scenario, spec)

        # Initialize MACRO items
        with scenario.transact("Prepare scenario for snapshot data"):
            MACRO.initialize(scenario)

        read_excel(scenario, path)
    elif snapshot_id == 1:
        # Should contain only valid units
        # Initialize MACRO items
        with scenario.transact("Prepare scenario for snapshot data"):
            MACRO.initialize(scenario)

        scenario.read_excel(path, add_units=True, init_items=True)
