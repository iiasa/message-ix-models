from message_ix_models.project.ssp import generate


def test_generate(tmp_path, test_context):
    generate(test_context, base_dir=tmp_path)

    assert 3 == len(list(tmp_path.glob("*.xml")))


def test_cli(mix_models_cli):
    mix_models_cli.assert_exit_0(["ssp", "gen-structures", "--dry-run"])
