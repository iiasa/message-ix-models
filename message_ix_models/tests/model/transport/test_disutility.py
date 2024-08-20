from ixmp.testing import assert_logs

import message_ix_models.model.transport.build
from message_ix_models.model.transport import testing


@message_ix_models.model.transport.build.get_computer.minimum_version
def test_disutility(
    caplog, tmp_path, test_context, regions="R12", years="B", options={}
):
    test_context.dry_run = True

    c, info = testing.configure_build(
        test_context, tmp_path=tmp_path, regions=regions, years=years, options=options
    )

    # Calculation succeeds
    key = "disutility::ixmp"
    # print(c.describe(key))
    result = c.get(key)
    # print(f"{result = }")

    # Result contains data for the "input" parameter only
    df = result.pop("input")
    assert 0 == len(result)
    # print(f"{df = }")

    # No missing values
    assert not df.isna().any(axis=None)
    # All model years are included
    assert set(c.get("y::model")) == set(df.year_vtg.unique())

    # Data are added to the scenario
    N = 12 * 27 * 12 * 14  # n × cg × t × y
    with assert_logs(caplog, f"{N} rows in 'input'"):
        c.get("add message_ix_models.model.transport.disutility")
