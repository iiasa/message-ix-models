import pytest

from message_ix_models.model.transport.callback import main


@pytest.mark.xfail(reason="Don't actually attempt to run the code.")
def test_callback():
    main()
