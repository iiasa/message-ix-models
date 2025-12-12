import pytest

from message_ix_models.util.genno import append


def test_append() -> None:
    from genno import Computer

    c = Computer()

    # Append to list
    c.add("key", ["foo", "bar"])
    # Function runs without error
    append(c, "key", "baz")
    # Task is extended
    assert ["foo", "bar", "baz"] == c.graph["key"]
    # Task runs
    assert ["foo", "bar", "baz"] == c.get("key")

    # Append to tuple
    c.add("key", lambda *args: " ".join(args), "foo", "bar")
    # Function runs without error
    append(c, "key", "baz")
    # Task is extended
    assert ("foo", "bar", "baz") == c.graph["key"][1:]
    # Task runs
    assert "foo bar baz" == c.get("key")

    # Not supported target type
    c.graph["key"] = object()
    with pytest.raises(TypeError):
        append(c, "key", "baz")
