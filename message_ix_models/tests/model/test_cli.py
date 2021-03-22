def test_create_bare(mix_models_cli):
    """The ``res create-bare`` CLI command can be invoked."""
    # --regions is not a required option, but we give it anyway to test the CLI code
    # that handles it
    result = mix_models_cli.invoke(["res", "create-bare", "--regions=R11"])

    assert 0 == result.exit_code, result.output
