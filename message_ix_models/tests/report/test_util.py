import pytest
from genno import Key
from message_ix import Reporter

from message_ix_models.report.key import all_iamc
from message_ix_models.report.util import IAMCConversion

M = pytest.mark.xfail(raises=AssertionError)

foo = Key("foo:nl-nd-yv-ya-a-b")


class TestIAMCConversion:
    @pytest.fixture
    def rep(self) -> Reporter:
        r = Reporter()
        r.add(all_iamc, "concat")
        return r

    @pytest.mark.parametrize(
        "var_parts, d_region, d_year",
        [
            (["Foo", "nd", "ya", "a", "b"], "nl", "yv"),
            (["Foo", "nd", "yv", "a", "b"], "nl", "ya"),
            (["Foo", "nl", "ya", "a", "b"], "nd", "yv"),
            (["Foo", "nl", "yv", "a", "b"], "nd", "ya"),
            #
            # Invalid arguments
            # No dimension mapped to "region"
            pytest.param(["Foo", "nd", "nl", "yv", "a", "b"], "", "", marks=M),
            # No dimension mapped to "year"
            pytest.param(["Foo", "nd", "ya", "yv", "a", "b"], "", "", marks=M),
            # Both of the above combined
            pytest.param(["Foo", "nd", "nl", "ya", "yv", "a", "b"], "", "", marks=M),
        ],
    )
    def test_add_tasks0(
        self, rep: Reporter, var_parts: list[str], d_region: str, d_year: str
    ) -> None:
        """Test behaviour of :meth:`.IAMCConversion.add_tasks`.

        See https://github.com/iiasa/message-ix-models/issues/454.
        """
        # IAMCConversion can be instantiated with these keyword arguments
        c = IAMCConversion(
            base=foo, var_parts=var_parts, unit="kg", sums=["a", "b", "a-b"]
        )

        keys_pre = set(rep.graph)
        c.add_tasks(rep)

        # Set of all added keys
        keys_post = set(rep.graph)
        added = keys_post - keys_pre
        assert 4 <= len(added)

        # Task for iamc::all has 4 input keys: base quantity + 3 sums
        task_all = rep.graph[all_iamc]
        assert 5 == len(task_all)

        for k in task_all[1:]:
            # Retrieve the final key added by add_tasks(), and the task it refers to
            task = rep.graph[k]

            # Description of the same task
            desc = rep.describe(k)

            # The task is connected to the original key
            assert f"- {foo!s}" in desc, desc

            # Retrieve the 'rename' keyword argument passed to partial(as_pyam, …)
            rename = task[0].keywords["rename"]

            # `d_region` is mapped to the IAMC `region` dimension
            assert "region" == rename[d_region]

            # `d_year` is mapped to the IAMC `year` dimension
            assert "year" == rename[d_year]

            # No dimensions referenced in "var_parts" appear in `rename`
            assert not (set(rename) & set(var_parts))
