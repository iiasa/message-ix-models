from pathlib import PurePosixPath as Path

from message_ix_models.util.slurm import JinjaTemplate


class Template(JinjaTemplate):
    """Template for use with :program:`mix-models sbatch`.

    Example usage::

      mix-models sbatch -m model.transport --go \
        -v branch=branch-to-run -v model_extra=debug \
        -- \
        --from="" "SSP2 reported" -- go

    The script performs the following actions, tailored for the IIASA 'UnICC' cluster
    and SLURM:

    1. Load necessary modules.
    2. Use :program:`uv` to install :mod:`message_ix_models` and dependencies.
    3. Run :program:`mix-models transport run --go` with additional arguments.
    4. Copy output files (e.g. reporting) to a shared drive.
    """

    argv = [
        "sbatch",
        "--job-name={job_name}",
        "--mem=64G",
        "--output=/home/{username}/slurm/{job_name}-%J.out",
        "--time=2:00:00",
    ]

    default_parameters = {
        "branch": "main",
        "job_name": "MixT",
        "model_extra": "dev",
        "nodes": "R12",
        "platform_dest": "message-jdbc-pg",
        "platform_src": "ixmp-dev",
        "python_version": "3.14",
        "source": "gitlab",
    }

    file = Path("transport", "slurm.sh")

    def __init__(self) -> None:
        # Disable the parent class' __init__() that requires a file= parameter
        pass
