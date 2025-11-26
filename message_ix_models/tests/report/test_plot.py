from message_ix_models.report.plot import LabelFirst


class TestLabelFirst:
    def test_call(self) -> None:
        labeler = LabelFirst("foo {}")

        assert ["foo 0", "1", "2", "3"] == list(map(labeler, range(4)))
