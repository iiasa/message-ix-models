def test_create_bare(mix_models_cli):
    """The ``res create-bare`` CLI command can be invoked."""
    result = mix_models_cli.invoke(["res", "create-bare"])

    assert 0 == result.exit_code, result.output
