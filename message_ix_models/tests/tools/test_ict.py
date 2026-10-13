import pytest

from message_ix_models.testing import bare_res
from message_ix_models.tools.ict import build
from message_ix_models.tools.ict.data_center import read_hist_data
from message_ix_models.tools.ict.digtrans import read_country_data


def test_iea() -> None:
    from message_ix_models.tools.ict.iea import read_iea_data

    result = read_iea_data()
    # TODO: test result
    assert 1


def test_digitrans() -> None:
    result = read_country_data("SSP2", full=True)
    # TODO: test result
    assert 1


def test_data_center() -> None:
    result = read_hist_data()
    # TODO: test result
    assert 1


def test_make_spec() -> None:
    s = build.make_spec()
    # TODO: test spec
    print()


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize(
    "regions, years, relations, ict_scen",
    [
        pytest.param("R12", "B", "B", "Central"),
        pytest.param("R12", "B", "B", "Low"),
        pytest.param("R12", "B", "B", "Medium"),
        pytest.param("R12", "B", "B", "High"),
    ],
)
def test_build(request, tmp_path, test_context, regions, years, relations, ict_scen):
    ctx = test_context
    ctx.update(regions=regions, years=years, ssp="SSP2", relations=relations)
    scenario = bare_res(request, ctx)
    with scenario.transact():
        scenario.add_set("mode", "M1")
    build(ctx, scenario, ict_scen)
    assert len(scenario.par("input")) > 0
