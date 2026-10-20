from message_ix_models.util import package_data_path
from message_ix_models.util.slurm import Template

#: Template for use with :program:`mix-models sbatch`. Example usage::
#:
#:   mix-models sbatch -m model.transport --remote --username=foo --go -- \
#:     "--base=auto --fast --nodes=R12 --model-extra='debug' --from='' 'SSP2 reported'"
#:
#: The script performs the following actions, tailored for the IIASA 'UniCC' cluster and
#: SLURM:
#:
#: 1. Load necessary modules.
#: 2. Use :program:`uv` to install :mod:`message_ix_models` and dependencies.
#: 3. Run :program:`mix-models transport run --go` with additional arguments.
#: 4. Copy output files (e.g. reporting) to a shared drive.
TEMPLATE = Template(
    "sbatch",
    "--job-name={J}",
    "--mem=40G",
    "--output=/home/{username}/slurm/message-%J.out",
    "--time=2:00:00",
    package_data_path("transport", "slurm.sh").read_text(),
)
