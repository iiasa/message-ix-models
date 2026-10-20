import pytest

from message_ix_models.model.transport.slurm import Template


@pytest.mark.parametrize("source", ("github", "gitlab", "foo"))
def test_template(source: str) -> None:
    # Format succeeds with no arguments
    argv, body = Template().format(source=source)

    # print(body.decode())  # DEBUG Show the script

    # Template is formatted including clone URLs
    assert b"STATIC_DATA_URL" in body
    assert b"MESSAGE_MODELS_URL" in body
