import pytest

_sd = ["--style=directives"]


@pytest.mark.parametrize(
    "sbatch_opts, env",
    (
        # All dry-run
        (_sd + ["--username=u", "--venv=/path/to/venv"], {}),  # Use CLI options
        (_sd, {"USER": "u", "VIRTUAL_ENV": "/path/to/venv"}),  # Options from env vars
        (_sd + ["--remote", "--username=u", "--venv=/path/to/venv"], {}),
        # With --go fails because the default config references a non-existent host
        pytest.param(
            _sd + ["--remote", "--username=u", "--venv=/path/to/venv", "--go"],
            {},
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ),
)
def test_cli(monkeypatch, tmp_path, mix_models_cli, sbatch_opts, env) -> None:
    cmd = (
        ["sbatch"]
        + sbatch_opts
        + ["--", "--opt0=0", "foo", "--opt1=1", "bar", "--opt2=2", "baz"]
    )

    # Set a temporary, fixed $HOME directory that exists
    home = str(tmp_path)
    monkeypatch.setitem(mix_models_cli.env, "HOME", home)

    for k, v in env.items():
        monkeypatch.setitem(mix_models_cli.env, k, v)

    # Command completes without error
    result = mix_models_cli.assert_exit_0(cmd)

    # Template is rendered correctly
    for line in (
        "#SBATCH --mail-user=u@iiasa.ac.at",
        f"#SBATCH --output={home}/slurm/solve_%J.out",
        "source /path/to/venv/bin/activate",
        "export IXMP_DATA=/path/to/venv/share/ixmp",
        "mix-models --opt0=0 foo --opt1=1 bar --opt2=2 baz",
    ):
        assert f"\n{line}\n" in result.output
