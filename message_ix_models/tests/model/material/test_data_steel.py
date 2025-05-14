from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_steel import (
    gen_dri_act_bound,
    gen_dri_coal_model,
)


def test_gen_dri_coal_model():
    info = ScenarioInfo()
    info.set["node"] = ["node0", "node1"]
    info.set["year"] = [2020, 2025]
    par_dict = gen_dri_coal_model(info)
    for k, v in par_dict.items():
        assert not v.isna().any(axis=None)  # Completely full


def test_gen_dri_act_bound():
    par_dict = gen_dri_act_bound()
    for k, v in par_dict.items():
        assert not v.isna().any(axis=None)  # Completely full
