from message_ix_models.project.ssp import generate


def test_generate(test_context):
    generate(test_context)


def test_cli(mix_models_cli):
    mix_models_cli.assert_exit_0(["ssp", "gen-structures"])
