import pytest


class TestMessageDataFinder:
    @pytest.mark.parametrize("has_message_data", (False, True))
    def test_exception(self, monkeypatch, has_message_data: bool) -> None:
        """:meth:`.MessageDataFinder.find_spec` fails transparently."""
        import message_ix_models.util.common

        monkeypatch.setattr(
            message_ix_models.util.common, "HAS_MESSAGE_DATA", has_message_data
        )
        try:
            import message_ix_models.model.transport.not_.a.submodule  # noqa: F401
        except ImportError as e:
            # The exception message mentions the original import name, not message_data
            assert "named 'message_ix_models.model.transport" in repr(e)
