import pytest
from message_ix import make_df

from message_ix_models.model.material.data_other_industry import gen_other_ind_demands


@pytest.mark.parametrize("ssp", ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"])
def test_gen_other_ind_demands(ssp) -> None:
    df = gen_other_ind_demands(ssp)
    for v in df.values():
        assert not v.isna().any(axis=None)
        assert (make_df("demand").columns == v.columns).all()
