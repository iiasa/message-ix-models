import logging

from sdmx.model import Annotation, Code

from message_ix_models.util.sdmx import eval_anno


def test_eval_anno(caplog):
    c = Code()

    assert None is eval_anno(c, "foo")

    c.annotations.append(Annotation(id="foo", text="bar baz"))

    with caplog.at_level(logging.DEBUG, logger="message_ix_models"):
        assert "bar baz" == eval_anno(c, "foo")

    assert [
        "Could not eval('bar baz'): invalid syntax (<string>, line 1)"
    ] == caplog.messages

    c.annotations.append(Annotation(id="qux", text="3 + 4"))

    assert 7 == eval_anno(c, id="qux")
