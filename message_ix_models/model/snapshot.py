"""Prepare base models from snapshot data."""
import logging
from pathlib import Path

import click
import pandas as pd
from message_ix import Scenario
from message_ix.models import MACRO
from tqdm import tqdm

from message_ix_models import Spec
from message_ix_models.util.pooch import fetch

from .build import apply_spec
from .structure import get_codes

log = logging.getLogger(__name__)

#: Available snapshots.
SNAPSHOTS = {
    0: dict(
        base_url="doi:10.5281/zenodo.5793870",
        registry={
            "MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline.xlsx": (
                "md5:222193405c25c3c29cc21cbae5e035f4"
            ),
        },
    )
}


def _unpack(path: Path) -> Path:
    """Unpack ixmp-format Excel file at `path`."""
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
            dfs.append(xf.parse(x))

        # Concatenate once and return
        return pd.concat(dfs, axis=0)

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
            else:
                df.to_csv(item_path, index=False)

        name_type.query("ix_type == 'set'").to_excel(ew, sheet_name="ix_type_mapping")

    return base


def _read_excel(scenario: Scenario, path: Path) -> None:
    base = _unpack(path)

    scenario.read_excel(base.joinpath("sets.xlsx"))
    with scenario.transact(f"Read snapshot from {path}"):
        for p in base.glob("*.csv.gz"):
            name = p.name.split(".")[0]
            data = pd.read_csv(p)

            # Correct units
            if name == "inv_cost":
                data.replace({"unit": {"USD_2005/t ": "USD_2005/t"}}, inplace=True)

            scenario.add_par(name, data)


def load(scenario: Scenario, snapshot_id: int) -> None:
    """Load snapshot with ID `snapshot_id` into `scenario`.

    See also
    --------
    SNAPSHOTS
    """
    path = fetch(SNAPSHOTS[snapshot_id])

    # Add units
    spec = Spec()
    spec.add.set["unit"] = get_codes("unit/snapshot")
    apply_spec(scenario, spec)

    # Initialize MACRO items
    with scenario.transact("Prepare scenario for snapshot data"):
        MACRO.initialize(scenario)

    _read_excel(scenario, path)


@click.group("snapshot", help="__doc__")
def cli():
    pass


@cli.command("fetch")
@click.argument("id_", metavar="ID", type=int)
def fetch_cmd(id_):
    """Fetch snapshot ID from Zenodo."""
    fetch(SNAPSHOTS[id_], progressbar=True)
