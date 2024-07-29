import pytest

from message_ix_models.model.transport.migrate import import_all


@pytest.mark.xfail(reason="Don't actually attempt to run the code.")
def test_null():
    import_all()
