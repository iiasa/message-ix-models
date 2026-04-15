from dataclasses import dataclass, field
from pathlib import Path

import pytest

from message_ix_models.util.config import ConfigHelper


@dataclass
class C1(ConfigHelper):
    """A class which inherits from ConfigHelper."""

    foo_1: int = 1
    foo_2: str = ""
    foo_3: bool = True


@dataclass
class C2:
    """NOT a ConfigHelper subclass."""

    baz_1: int = 1
    baz_2: int = 2


@dataclass
class C3(ConfigHelper):
    """A class with a plain attribute and 2 instances of classes."""

    bar_1: float = 0.01
    subconfig_a: C1 = field(default_factory=C1)
    subconfig_b: C2 = field(default_factory=C2)


class TestConfigHelper:
    @pytest.fixture
    def c(self) -> C1:
        return C1(foo_1=99, foo_2="bar", foo_3=False)

    @pytest.fixture
    def c2(self) -> C3:
        result = C3(bar_1=3.14, subconfig_a=C1(foo_1=99, foo_2="bar", foo_3=False))
        result.subconfig_b.baz_1 = 2
        result.subconfig_b.baz_2 = 1
        return result

    def test_canonical_name(self) -> None:
        assert "foo_1" == C1._canonical_name("foo 1")
        assert "foo_2" == C1._canonical_name("foo-2")
        assert "foo_3" == C1._canonical_name("foo_3")
        assert None is C1._canonical_name("foo 4")

    def test_from_dict(self, c: C1) -> None:
        values = {"foo 1": 99, "foo-2": "bar", "foo_3": False}
        assert c == C1.from_dict({"foo 1": 99, "foo-2": "bar", "foo_3": False})

        values.update(foo_4=3.14)
        with pytest.raises(ValueError):
            C1.from_dict(values)

    def test_read_file(
        self,
        caplog: pytest.LogCaptureFixture,
        tmp_path: Path,
        c: C1,
        c2: C3,
    ) -> None:
        # Write a YAML snippet to file
        yaml_path = tmp_path.joinpath("config.yaml")
        yaml_path.write_text(
            """
foo 1: 99

foo-2: bar

foo_3: false

foo_4: 3.14
            """
        )

        obj1 = C1()
        # Method runs
        with pytest.raises(ValueError, match="no attribute for file section 'foo_4'"):
            obj1.read_file(yaml_path)

        yaml_path.write_text(
            """
bar_1: 3.14
subconfig a:
  foo 1: 99
  foo-2: bar
  foo_3: false
subconfig-b:  # No name manipulation for subkeys here
  baz_1: 2
  baz_2: 1
            """
        )

        obj2 = C3()
        # Method runs
        obj2.read_file(yaml_path)
        # Values are read
        assert c2 == obj2, obj2

        json_path = tmp_path.joinpath("config.json")
        json_path.write_text(
            """{
"foo 1": 99,
"foo-2": "bar",
"foo_3": false
}           """
        )

        obj3 = C1()
        # Method runs
        obj3.read_file(json_path)
        # Values are read
        assert c == obj3, obj3

        obj4 = C1()
        with pytest.raises(NotImplementedError):
            obj4.read_file(yaml_path.with_suffix(".xlsx"))

    def test_replace(self, c: C1) -> None:
        result = c.replace(foo_2="baz")
        assert result is not c
        assert "baz" == result.foo_2

    def test_update(self, c: C1) -> None:
        """:meth:`.ConfigHelper.update` raises AttributeError."""
        with pytest.raises(AttributeError):
            c.update(foo_4="")
