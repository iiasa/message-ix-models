import re
from typing import TYPE_CHECKING

from sdmx.model import common, v21

from message_ix_models.model.transport.workflow import SOLVE_CONFIG
from message_ix_models.model.workflow import from_codelist, step_0
from message_ix_models.testing import bare_res
from message_ix_models.tools import (
    add_AFOLU_CO2_accounting,
    add_alternative_TCE_accounting,
)
from message_ix_models.util.sdmx import StructureFactory

if TYPE_CHECKING:
    from pytest import FixtureRequest, LogCaptureFixture

    from message_ix_models import Context


class CL_SCENARIO_TEST(StructureFactory):
    """A scenario code list for testing."""

    urn = "IIASA_ECE:CL_SCENARIO_TEST"
    version = "1.0.0"
    base_url = ""

    @classmethod
    def create(cls) -> "common.Codelist":
        from sdmx.model import common

        from message_ix_models.util.sdmx import read

        IIASA_ECE = read("IIASA_ECE:AGENCIES")["IIASA_ECE"]
        cl: "common.Codelist" = common.Codelist(
            id=cls.urn.partition(":")[-1],
            maintainer=IIASA_ECE,
            version="1.0.0",
            is_external_reference=False,
            is_final=True,
        )

        anno: list["common.BaseAnnotation"] = [
            v21.Annotation(id="base-scenario-URL", text=repr(cls.base_url))
        ]

        for id_ in "FOO BAR BAZ".split():
            cl.append(common.Code(id=id_, annotations=anno))

        return cl


def test_from_codelist(
    caplog: "LogCaptureFixture", request: "FixtureRequest", test_context: "Context"
) -> None:
    test_context.model.regions = "R12"
    scenario = bare_res(request, test_context, solved=False)

    # Use the `scenario` as the base for a new Workflow
    CL_SCENARIO_TEST.base_url = f"ixmp://{scenario.platform.name}/{scenario.url}"

    # Use specific, non-default solve config
    test_context.solve = SOLVE_CONFIG

    # from_codelist() runs without error
    wf = from_codelist(test_context, CL_SCENARIO_TEST)

    # Specific CPLEX options from SOLVE_CONFIG are used in the solve step
    wf.get("FOO solved")

    assert any(re.match("^Use CPLEX options.*'iis': 1", msg) for msg in caplog.messages)


def test_step_0(request: "FixtureRequest", test_context: "Context") -> None:
    """Test :func:`.model.workflow.step_0`."""
    test_context.model.regions = "R12"
    scenario = bare_res(request, test_context, solved=False)

    # Add to `scenario` minimal data/structure needed by tools to be used
    add_AFOLU_CO2_accounting.test_data(scenario)
    add_alternative_TCE_accounting.test_data(scenario)

    step_0(test_context, scenario)

    # TODO Add assertions about modified structure & data
