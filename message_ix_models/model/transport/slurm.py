from pathlib import Path

from message_ix_models.util.slurm import JinjaTemplate


class Template(JinjaTemplate):
    """Template for use with :program:`mix-models sbatch`.

    Example usage::

      mix-models sbatch -m model.transport --remote --username=foo --go -- \
        "--base=auto --fast --nodes=R12 --model-extra='debug' --from='' 'SSP2 reported'"

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
        "job_name": "MixT",
        "branch": "main",
        "model_extra": "dev",
        "python_version": "3.14",
        "source": "gitlab",
    }

    file = Path("transport", "slurm.sh")

    def __init__(self) -> None:
        pass
