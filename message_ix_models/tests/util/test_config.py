from dataclasses import dataclass, field
from typing import Type

import pytest

from message_ix_models.util.config import ConfigHelper


class TestConfigHelper:
    @pytest.fixture
    def cls(self) -> Type:
        """A class which inherits from ConfigHelper."""

        @dataclass
        class Config(ConfigHelper):
            foo_1: int = 1
            foo_2: str = ""
            foo_3: bool = True

        return Config

    @pytest.fixture
    def cls2(self, cls) -> Type:
        """A class with an attribute."""

        @dataclass
        class Config3:
            """NOT a ConfigHelper subclass."""

            baz_1: int = 1
            baz_2: int = 2

        @dataclass
        class Config2(ConfigHelper):
            bar_1: float = 0.01
            subconfig_a: cls = field(default_factory=cls)  # type: ignore [valid-type]
            subconfig_b: Config3 = field(default_factory=Config3)

        return Config2

    @pytest.fixture
    def c(self, cls):
        return cls(foo_1=99, foo_2="bar", foo_3=False)

    @pytest.fixture
    def c2(self, cls, cls2):
        result = cls2(bar_1=3.14, subconfig_a=cls(foo_1=99, foo_2="bar", foo_3=False))
        result.subconfig_b.baz_1 = 2
        result.subconfig_b.baz_2 = 1
        return result

    def test_canonical_name(self, cls):
        assert "foo_1" == cls._canonical_name("foo 1")
        assert "foo_2" == cls._canonical_name("foo-2")
        assert "foo_3" == cls._canonical_name("foo_3")
        assert None is cls._canonical_name("foo 4")

    def test_from_dict(self, cls, c):
        values = {"foo 1": 99, "foo-2": "bar", "foo_3": False}
        assert c == cls.from_dict({"foo 1": 99, "foo-2": "bar", "foo_3": False})

        values.update(foo_4=3.14)
        with pytest.raises(ValueError):
            cls.from_dict(values)

    def test_read_file(self, caplog, tmp_path, cls, cls2, c, c2):
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

        obj1 = cls()
        # Method runs
        obj1.read_file(yaml_path, fail=False)
        # Values are read
        assert c == obj1, obj1
        # Messages are logged
        assert [
            "Config has no attribute for file section 'foo_4'; ignored"
        ] == caplog.messages
        caplog.clear()

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

        obj2 = cls2()
        # Method runs
        obj2.read_file(yaml_path, fail=False)
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

        obj3 = cls()
        # Method runs
        obj3.read_file(json_path)
        # Values are read
        assert c == obj3, obj3

        obj4 = cls()
        with pytest.raises(NotImplementedError):
            obj4.read_file(yaml_path.with_suffix(".xlsx"))

    def test_replace(self, c):
        result = c.replace(foo_2="baz")
        assert result is not c
        assert "baz" == result.foo_2
