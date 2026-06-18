from message_ix_models.util._re import Substitutions


class TestSubstitutions:
    def test_init_call(self) -> None:
        s0 = Substitutions(("foo", "bar"), ("bar", "baz"), ("zz+", "z"))
        result = s0("afooz")
        assert "abaz" == result

    def test_add(self) -> None:
        s0 = Substitutions(("foo", "bar"), ("bar", "baz"))
        s1 = s0 + ("zz+", "z")
        result = s1("afooz")
        assert "abaz" == result
