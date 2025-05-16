import re
from typing import TYPE_CHECKING, Literal, Optional

import pytest

from message_ix_models import testing
from message_ix_models.project.navigate import T35_POLICY, Config
from message_ix_models.project.navigate.report import _scenario_name
from message_ix_models.project.navigate.workflow import add_macro, generate

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from message_ix_models import Context


def test_generate_workflow(test_context: "Context") -> None:
    # Use default configuration
    test_context.navigate = Config()

    # Same as in test_generate_workflow_cli
    wf = generate(test_context)
    wf.truncate("M T3.5 built")

    # Check the pre-requisite steps of some workflow steps. This is the 2nd entry in the
    # dask task tuple in wf.graph.
    assert wf.graph["MT NPi-ref solved"][2] == "MT NPi-ref built"
    assert wf.graph["MT NPi-ref built"][2] == "M T3.5 + GLOBIOM"

    # Workflow is truncated at "M T3.5 built"
    assert wf.graph["M T3.5 built"][2] is None
    assert wf.graph["M T3.5 built"][0].scenario_info == dict(
        model="MESSAGEix-Materials", scenario="baseline_DEFAULT_NAVIGATE"
    )

    for s in ("act", "all", "ele", "ref", "tec"):
        # Depends on the corresponding solved BMT model + a following step
        assert wf.graph[f"NPi-{s} reported"][2] == f"NPi-{s} 2025 limit computed"

        # Scenario name as expected
        assert wf.graph[f"BMT NPi-{s} solved"][0].scenario_info == dict(
            model="MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)", scenario=f"NPi-{s}"
        )


# Chunks of text to look for in the --dry-run output. The text cannot be matched exactly
# because the order of traversing the graph is non-deterministic, i.e. which step
# displays "MT solved" and its subtree may vary.
_context = r"'context' \(above\)"
BLOCKS = [
    "Truncate workflow at 'M T3.5 built'",
    rf"""
(\s+)- 'MT NPi-ref solved':
  \1- <Step solve\(\)>
  \1- {_context}
  \1- 'MT NPi-ref built':
    \1- <Step main\(\) -> MESSAGEix-GLOBIOM 1.1-MT-R12 \(NAVIGATE\)/NPi-ref>
    \1- {_context}""",
    rf"""
(\s+)- 'M T3.5 built':
  \1- <Step load -> MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE>
  \1- {_context}
  \1- None""",
]


@testing.MARK[1]
def test_generate_workflow_cli(test_context, mix_models_cli) -> None:
    """Test :func:`.navigate.workflow.generate` and associated CLI."""

    # CLI command to run
    cmd = ["navigate", "run", "--from=M T3.5 built", "--dry-run", "all reported"]
    result = mix_models_cli.invoke(cmd)
    assert 0 == result.exit_code, result.output

    # Workflow has the expected scenarios in it
    for b in BLOCKS:
        if re.search(b, result.output, flags=re.DOTALL):
            continue
        else:  # pragma: no cover
            print(result.output)
            assert False, f"No match for: {b!r}"


@pytest.mark.parametrize(
    "dsd, input, expected",
    (
        # Task 3.5
        ("navigate", "NPi-ref", "NAV_Dem-NPi-ref"),
        ("navigate", "NPi-ref+MACRO", "NAV_Dem-NPi-ref"),
        ("navigate", "20C-act+MACRO_ENGAGE_20C_step-3+B", "NAV_Dem-20C-act_u"),
        ("navigate", "20C-ref_ENGAGE_20C_step-3+B", "NAV_Dem-20C-ref"),
        ("navigate", "20C-tec_ENGAGE_20C_step-3+B", "NAV_Dem-20C-tec_u"),
        # Prior to 2023-08-21 "NPi" would appear in the scenario name even for policy
        # scenarios; the 3 below are earlier forms of the 3 above
        # ("navigate", "NPi-act+MACRO_ENGAGE_20C_step-3+B", "NAV_Dem-20C-act_u"),
        # ("navigate", "NPi-ref_ENGAGE_20C_step-3+B", "NAV_Dem-20C-ref"),
        # ("navigate", "NPi-tec_ENGAGE_20C_step-3+B", "NAV_Dem-20C-tec_u"),
        ("navigate", "20C-tec_u ENGAGE_20C_step-3+B", "NAV_Dem-20C-tec_u"),
        # Work package 6
        ("navigate", "NPi-Default", "PC-NPi-Default"),
        ("navigate", "2C-AllEn ENGAGE_20C_step-3+B", "PEP-2C-AllEn"),
        ("navigate", "15C-LowCE ENGAGE_15C_step-3+B", "PC-15C-LowCE"),
        # Others
        ("iiasa-ece", "Ctax-ref", "NAV_Dem-Ctax-ref"),
        ("iiasa-ece", "Ctax-ref+B", "NAV_Dem-Ctax-ref"),
        ("navigate", "baseline", None),
    ),
)
def test_scenario_name(
    test_context: "Context",
    dsd: Literal["iiasa-ece", "navigate"],
    input: str,
    expected: Optional[str],
) -> None:
    test_context.setdefault("navigate", Config(dsd=dsd))
    assert expected == _scenario_name(test_context, input)


@pytest.mark.skipif(testing.GHA, reason="Crashes pytest on GitHub Actions runners")
@pytest.mark.xfail(reason="Bare RES lacks detail sufficient for add_macro()")
def test_add_macro(request: "FixtureRequest", test_context: "Context") -> None:
    test_context.regions = "R12"
    scenario = testing.bare_res(request, test_context, solved=True)
    add_macro(test_context, scenario)


class TestT35_POLICY:
    @pytest.mark.parametrize(
        "value, expected",
        (
            ("", T35_POLICY.REF),
            ("act", T35_POLICY.ACT),
            ("ele", T35_POLICY.ELE),
            ("tec", T35_POLICY.TEC),
            ("act+ele+tec", T35_POLICY.ACT | T35_POLICY.ELE | T35_POLICY.TEC),
            pytest.param(
                "foo+act+tec", None, marks=pytest.mark.xfail(raises=ValueError)
            ),
        ),
    )
    def test_parse(self, value: str, expected: Optional[int]) -> None:
        assert expected is T35_POLICY.parse(value)
