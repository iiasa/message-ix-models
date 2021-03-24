def test_create_bare(mix_models_cli):
    """The ``res create-bare`` CLI command can be invoked."""
    # --regions is not a required option, but we give it anyway to test the CLI code
    # that handles it
    mix_models_cli.assert_exit_0(["res", "create-bare", "--regions=R11"])
