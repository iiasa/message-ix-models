import pytest

from message_ix_models.model import Config


class TestConfig:
    @pytest.mark.parametrize(
        "values",
        [
            dict(regions="R11", years="A"),
            pytest.param(
                dict(regions="R99", years="A"),
                marks=pytest.mark.xfail(
                    raises=ValueError, reason="regions='R99' not among […]"
                ),
            ),
            pytest.param(
                dict(regions="R11", years="C"),
                marks=pytest.mark.xfail(
                    raises=ValueError, reason="regions='C' not among ['A', 'B']"
                ),
            ),
        ],
    )
    def test_check(self, values):
        c = Config(**values)
        c.check()
