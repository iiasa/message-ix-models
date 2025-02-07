"""Utilities for the `SLURM Workload Manager <https://slurm.schedmd.com/>`_.

To use, transform a desired, ordinary invocation of the :program:`mix-models` CLI:

.. code-block:: bash
   :caption: Command 1

   mix-models --opt_a=0 -b 2 command --opt_c=2 subcommand --opt_d=3 arg0 arg1

…into something like:

.. code-block:: bash
   :caption: Command 2

   mix-models sbatch --go \\
     --username=example_user \\
     --venv=/home/example_user/venv/py3.13_demo \\
     -- \\
     --opt_a=0 -b 2 command --opt_c=2 subcommand --opt_d=3 arg0 arg1

In particular:

- The inserted ``--`` separates the command ``sbatch`` from the options and arguments to
  be used to invoke :program:`mix-models` on the worker node.
  This command will result in exactly Command 1 being invoked at the end of the script
  :data:`TEMPLATE`.
- The options :program:`--username` and  :program:`--venv` are also passed into the
  template. As the name implies, they are optional. The values are read from the
  ``$USER`` and ``$VIRTUAL_ENV`` environment variables, respectively, wherever Command 2
  is invoked.
- Without the option :program:`--go`, the batch script is only printed out.
  Add this option to actually call sbatch.

See also:

- `sbatch <https://slurm.schedmd.com/sbatch.html>`_ manual page.
- :doc:`/howto/unicc`.
- :doc:`/distrib/`.
"""

import os
from collections.abc import Sequence
from subprocess import PIPE, STDOUT, run
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    import subprocess

#: Template for an sbatch script. Currently, the same as suggested by
#: :doc:`/distrib/unicc`.
#:
#: .. todo:: Read this content in separate pieces from the user's configuration and
#:    assemble.
TEMPLATE = """#!/bin/bash
#SBATCH --time=1:00:00
#SBATCH --mem=32G
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user={username}@iiasa.ac.at
#SBATCH -o {home_path}/out/solve_%J.out
#SBATCH -e {home_path}/err/solve_%J.err

module purge
source /opt/apps/lmod/8.7/init/bash
module load Python/3.11.5-GCCcore-13.2.0
module load Java

echo "Activate environment and set IXMP_DATA"
source {env_path}/bin/activate
export IXMP_DATA={env_path}/share/ixmp

echo "Invoke message-ix-models"
mix-models {args}
"""


def invoke_sbatch(
    username: str, venv_path: str, args: Sequence[str], *, dry_run: bool = False
) -> "subprocess.CompletedProcess":
    """Invoke :program:`sbatch` using :func:`subprocess.run`.

    :data:`TEMPLATE` is formatted using the arguments and passed directly to
    :program:`sbatch` on standard input.
    """

    # Prepare the script using the template and variables
    stdin = TEMPLATE.format(
        username=username,
        home_path=os.environ["HOME"],
        env_path=venv_path,
        args=" ".join(args),
    ).encode()

    if dry_run:
        cmd = "echo"
        print(f"Will invoke `sbatch` with standard input:\n\n{stdin.decode()}")
    else:  # pragma: no cover
        cmd = "sbatch"

    return run(cmd, input=stdin, stdout=PIPE, stderr=STDOUT)


@click.command("sbatch")
@click.option("--username", "-u", envvar="USER", help="User name.")
@click.option("--venv", "-e", envvar="VIRTUAL_ENV", help="Path to virtual environment.")
@click.option("--go", is_flag=True, help="Actually invoke.")
@click.argument("args", nargs=-1)
@click.pass_obj
def cli(context, username, venv, go, args):
    """Submit `mix-models ARGS` to a SLURM queue."""
    result = invoke_sbatch(username, venv, args, dry_run=not go)

    assert result.returncode == 0, result
