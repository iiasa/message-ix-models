import numpy as np

from message_ix_models.tools.costs.learning import get_cost_reduction_data


def test_get_cost_reduction_data():
    res = get_cost_reduction_data()

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "message_technology",
                    "technology_type",
                    "scenario",
                    "cost_reduction",
                ]
            ).any()
        )
        is True
    )

    # Check that the max cost reduction is less than 1
    assert res.cost_reduction.max() < 1
