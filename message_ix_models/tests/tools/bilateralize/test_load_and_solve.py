from contextlib import contextmanager
from typing import TYPE_CHECKING, cast

import pandas as pd

from message_ix_models.tools.bilateralize.load_and_solve import (
    ensure_balance_equality,
)
from message_ix_models.tools.bilateralize.utils import get_logger

if TYPE_CHECKING:
    import message_ix


class MockScenario:
    """Stand-in for :class:`message_ix.Scenario` recording set changes.

    Only implements the subset of the API used by
    :func:`.ensure_balance_equality`: :meth:`set`, :meth:`par`, :meth:`transact`,
    :meth:`remove_set`, and :meth:`add_set`.
    """

    def __init__(self, balance_equality: pd.DataFrame, output: pd.DataFrame) -> None:
        self._balance_equality = balance_equality
        self._output = output
        self.removed: list[tuple[str, pd.DataFrame]] = []
        self.added: list[tuple[str, pd.DataFrame]] = []

    def set(self, name: str) -> pd.DataFrame:
        assert name == "balance_equality"
        return self._balance_equality.copy()

    def par(self, name: str) -> pd.DataFrame:
        assert name == "output"
        return self._output.copy()

    @contextmanager
    def transact(self, message: str | None = None):
        yield

    def remove_set(self, name: str, data: pd.DataFrame) -> None:
        self.removed.append((name, data))

    def add_set(self, name: str, data: pd.DataFrame) -> None:
        self.added.append((name, data))


def test_ensure_balance_equality() -> None:
    balance_equality = pd.DataFrame(
        [
            ["LNG", "export"],
            ["LNG", "import"],
            ["coal", "primary"],
        ],
        columns=["commodity", "level"],
    )

    output = pd.DataFrame(
        [
            ["LNG_exp", "LNG", "shipped"],
            ["LNG_imp", "LNG", "shipped"],
            ["LNG_exp", "LNG", "primary"],
            ["crudeoil_piped_exp", "crudeoil", "piped"],
        ],
        columns=["technology", "commodity", "level"],
    )

    scen = MockScenario(balance_equality=balance_equality, output=output)
    log = get_logger(__name__)

    ensure_balance_equality(
        scen=cast("message_ix.Scenario", scen),
        log=log,
        covered_tec=["LNG", "crudeoil"],
    )

    # Only the import/export rows are removed; the unrelated "primary" row is not
    assert len(scen.removed) == 1
    removed_name, removed_df = scen.removed[0]
    assert removed_name == "balance_equality"
    pd.testing.assert_frame_equal(
        removed_df.reset_index(drop=True),
        balance_equality[
            balance_equality["level"].isin(["export", "import"])
        ].reset_index(drop=True),
    )

    # One balance_equality set is added per covered technology
    assert len(scen.added) == 2
    added_by_name = {name: df for name, df in scen.added}
    assert set(added_by_name) == {"balance_equality"}

    added_dfs = [df for _, df in scen.added]

    # LNG: only the "shipped" row survives (the "primary" row is filtered out,
    # and the two "shipped" rows across LNG_exp/LNG_imp collapse to one)
    lng_df = next(df for df in added_dfs if (df["commodity"] == "LNG").all())
    assert lng_df[["commodity", "level"]].values.tolist() == [["LNG", "shipped"]]

    # crudeoil: the single "piped" row survives unchanged
    crudeoil_df = next(df for df in added_dfs if (df["commodity"] == "crudeoil").all())
    assert crudeoil_df[["commodity", "level"]].values.tolist() == [
        ["crudeoil", "piped"]
    ]
