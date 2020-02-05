import pytest

from message_data.model.transport.callback import main


@pytest.mark.xfail(reason="Don't actually attempt to run the code.")
def test_callback():
    main()
