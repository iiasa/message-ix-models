import pytest

from message_ix_models.testing import bare_res
from message_ix_models.tools.ict import build
from message_ix_models.tools.ict.build import make_spec
from message_ix_models.tools.ict.config import Config


def test_make_spec() -> None:
    s = make_spec()
    assert len(s.add.set["technology"]) == 2


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize(
    "regions, years, relations, ict_scen",
    [
        pytest.param("R12", "B", "B", "Mean"),
        pytest.param("R12", "B", "B", "Central"),
        pytest.param("R12", "B", "B", "Low"),
        pytest.param("R12", "B", "B", "High"),
    ],
)
def test_build(request, tmp_path, test_context, regions, years, relations, ict_scen):
    ctx = test_context
    ctx.update(regions=regions, years=years, ssp="SSP2", relations=relations)
    ctx.ict = Config(scenario=ict_scen)
    scenario = bare_res(request, ctx)
    with scenario.transact():
        scenario.add_set("mode", "M1")
    build(ctx, scenario)
    assert len(scenario.par("input")) > 0
