import numpy as np

from message_ix_models.tools.costs.learning import get_cost_reduction_data


def test_get_cost_reduction_data():
    res = get_cost_reduction_data()

    # Check the manually assigned GEA values for gas_ppl is correct
    assert np.all(
        res.loc[res.message_technology == "gas_ppl"][["GEAL", "GEAM", "GEAH"]].values
        == [0.2, 0.29, 0.38]
    )

    # Check that SSP columns are in the dataframe
    assert (
        bool(
            res.columns.isin(
                [
                    "SSP1_learning",
                    "SSP1_cost_reduction",
                    "SSP2_learning",
                    "SSP2_cost_reduction",
                    "SSP3_learning",
                    "SSP3_cost_reduction",
                    "SSP4_learning",
                    "SSP4_cost_reduction",
                    "SSP5_learning",
                    "SSP5_cost_reduction",
                ]
            ).any()
        )
        is True
    )

    # Check the SSP5 cost reduction rate for geo_hpl is 0.18
    assert (
        res.loc[res.message_technology == "geo_hpl"][["SSP5_cost_reduction"]].values
        == 0.18
    )
