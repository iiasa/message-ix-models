from message_ix_models import Context
from message_ix_models.model.workflow import from_codelist
from message_ix_models.project.circeular.workflow import generate


@from_codelist.minimum_version
def test_generate(test_context: Context) -> None:
    wf = generate(test_context)
    # TODO Expand with additional assertions
    del wf
