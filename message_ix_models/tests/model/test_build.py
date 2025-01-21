import logging
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from ixmp.testing import assert_logs
from message_ix import make_df
from message_ix.testing import make_dantzig

from message_ix_models import Spec
from message_ix_models.model.build import apply_spec

if TYPE_CHECKING:
    from message_ix import Scenario


@pytest.fixture
def scenario(test_context):
    mp = test_context.get_platform()
    yield make_dantzig(mp)


@pytest.fixture(scope="function")
def spec() -> Generator[Spec, None, None]:
    """An empty Spec."""
    yield Spec()


def test_apply_spec0(caplog, scenario: "Scenario", spec: Spec):
    """Require missing element raises ValueError."""
    spec["require"].set["node"].append("vienna")

    with pytest.raises(ValueError):
        apply_spec(scenario, spec)

    assert (
        "message_ix_models.model.build",
        logging.ERROR,
        "  1 elements not found: ['vienna']",
    ) in caplog.record_tuples


def test_apply_spec1(caplog, scenario: "Scenario", spec: Spec):
    """Add data using the data= argument."""

    def add_data_func(scenario, dry_run):
        return dict(
            demand=make_df(
                "demand",
                commodity="cases",
                level="consumption",
                node="chicago",
                time="year",
                unit="case",
                value=301.0,
                year=1963,
            )
        )

    apply_spec(scenario, spec, data=add_data_func, dry_run=True)

    # Messages are logged about additions
    assert_logs(caplog, "1 rows in 'demand'")
    # …but no change because `dry_run`
    assert not any(301.0 == scenario.par("demand")["value"])

    caplog.clear()

    apply_spec(scenario, spec, data=add_data_func)

    # Messages are logged about additions
    assert_logs(caplog, "1 rows in 'demand'")
    # Value was actually changed
    assert 1 == sum(301.0 == scenario.par("demand")["value"])


def test_apply_spec2(caplog, scenario: "Scenario", spec: Spec):
    """Remove an element, with fast=True."""
    spec["remove"].set["node"] = ["new-york", "not-a-node"]

    apply_spec(scenario, spec, fast=True)

    # Messages are logged about removals
    assert_logs(
        caplog,
        (
            "Remove 'new-york' from set 'node'",
            "Remove 'not-a-node' from set 'node'",
            "  …not found",
        ),
    )


def test_apply_spec3(caplog, scenario: "Scenario", spec: Spec):
    """Actually remove data."""
    spec["remove"].set["node"] = ["new-york"]

    apply_spec(scenario, spec)

    # Messages are logged about removals
    assert_logs(
        caplog,
        (
            "Remove data with node='new-york'",
            "  1 rows in 'demand'",
            "  2 rows in 'output'",
            "  3 rows total",
        ),
    )


def test_apply_spec4(request, caplog, scenario: "Scenario", spec: Spec):
    """Test that platform region IDs are added as necessary."""

    # Existing region code list on `scenario.platform`
    regions_pre = scenario.platform.regions()

    # Add a unique node ID
    node = f"{request.node.name} {len(regions_pre)}"
    spec.add.set["node"] = [node]

    # Also add a node ID that already exists as a region ID on `scenario.platform`
    spec.add.set["node"].append(regions_pre["region"].iloc[0])

    # Function runs
    apply_spec(scenario, spec)

    # `scenario.platform` gains a region ID corresponding to the new node ID
    assert node in scenario.platform.regions()["region"].tolist()

    # Nothing logged for the already-existing region ID
    assert not any("already defined" in message for message in caplog.messages)
