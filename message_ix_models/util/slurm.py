"""Utilities for the `SLURM Workload Manager <https://slurm.schedmd.com/>`_.

To use, start with a desired, ordinary invocation of the :program:`mix-models` CLI:

.. code-block:: bash
   :caption: Command 1

   mix-models --opt_a=0 -b 2 command --opt_c=2 subcommand --opt_d=3 arg0 arg1

Then modify this into something like:

.. code-block:: bash
   :caption: Command 2

   mix-models sbatch --go \\
     --username=example_user \\
     --venv=/home/example_user/venv/py3.13_demo \\
     -- \\
     --opt_a=0 -b 2 command --opt_c=2 subcommand --opt_d=3 arg0 arg1

In particular:

- The inserted ``--`` separates the command ``sbatch`` from the options and arguments to
  be used to invoke :program:`mix-models` on the SLURM worker node.
  This command will result in exactly Command 1 being invoked at the end of the script
  :data:`DEFAULT`.
- The options :program:`--username` and  :program:`--venv` are also passed into the
  template. As the name implies, they are optional. The values are read from the
  ``$USER`` and ``$VIRTUAL_ENV`` environment variables, respectively, wherever Command 2
  is invoked.
- Without the option :program:`--go`, the batch script is only printed out. When the
  option is added, sbatch is actually invoked.

See also:

- `sbatch <https://slurm.schedmd.com/sbatch.html>`_ manual page.
- :doc:`/howto/unicc`.
- :doc:`/distrib/`.
"""

import os
from importlib import import_module
from subprocess import PIPE, STDOUT, run
from typing import Any

import click
import ixmp


class Template(list):
    """A template for a Slurm invocation.

    A list of :class:`str` in which:

    - The *first* entry should be the name or path of a program, for instance "sbatch".
    - The *final* entry should be a script body to be passed as standard input to the
      program.
    - Any intermediate entries should be command-line arguments to the program.
    """

    def __init__(self, *args: str) -> None:
        super().__init__(args)

    @classmethod
    def from_module(cls, module_name: str) -> "Template":
        """Return the first instance of Template found in `module_name`."""
        mod = import_module(f"message_ix_models.{module_name}")
        for name in dir(mod):
            obj = getattr(mod, name)
            if isinstance(obj, cls):
                return obj
        raise ImportError(  # pragma: no cover
            f"No instance of {cls} in module {mod.__name__!r}"
        )

    def format(
        self, as_directives: bool = False, **parameters: Any
    ) -> tuple[list[str], bytes]:
        """Prepare arguments and standard input from the template and `parameters`.

        Parameters
        ----------
        as_directives
            If :any:`True`, options to :program:`sbatch` are formatted as ``#SBATCH``
            lines, immediately after the first line of the template body. If
            :any:`False` (default), they are returned as part of the arguments.

        Returns
        -------
        tuple
            with 2 elements:

            1. A list of command arguments, length 1 or greater. The first is the name
               or path of an program, for instance "sbatch". The remainder are
               parameters or options to the same program.
            2. :class:`bytes` with the formatted script body to be passed as standard
               input to the program.
        """
        kw = (
            dict(
                home_path=os.environ["HOME"],
                mix_models_args=" ".join(parameters.pop("mix_models_args", [])),
            )
            | parameters
        )

        args = [arg.format(**kw) for arg in self[:-1]]
        body = self[-1].format(**kw)

        if as_directives:
            lines = body.splitlines()
            lines = lines[:1] + [f"#SBATCH {arg}" for arg in args[1:]] + lines[1:]
            body = "\n".join(lines)
            args = args[:1]

        return args, body.encode()


#: Default template for :func:`cli`. Currently, the same as suggested by
#: :doc:`/distrib/unicc`.
DEFAULT = Template(
    "sbatch",
    "--time=1:00:00",
    "--mem=32G",
    "--mail-type=BEGIN,END,FAIL",
    "--mail-user={username}@iiasa.ac.at",
    "--output={home_path}/slurm/solve_%J.out",
    "--error={home_path}/slurm/solve_%J.err",
    """#!/bin/bash

module purge
source /opt/apps/lmod/8.7/init/bash
module load Python/3.11.5-GCCcore-13.2.0
module load Java

echo "Activate environment and set IXMP_DATA"
source {env_path}/bin/activate
export IXMP_DATA={env_path}/share/ixmp

echo "Invoke message-ix-models"
mix-models {mix_models_args}
""",
)


@click.command("sbatch")
@click.option("--module", "-m", default="util.slurm")
@click.option("--remote", is_flag=True, help="Prepend args to call sbatch over ssh.")
@click.option("--go", is_flag=True, help="Actually invoke.")
@click.option(
    "--style",
    type=click.Choice(["o", "opts", "d", "directives"]),
    default="opts",
    help="Style of passing sbatch options.",
)
@click.option("--username", "-u", envvar="USER", help="User name.")
@click.option("--venv", "-e", envvar="VIRTUAL_ENV", help="Path to virtual environment.")
@click.option("vars", "-v", multiple=True, help="Set VAR=VALUE for templating.")
@click.argument("mix_models_args", nargs=-1, metavar="args")
@click.pass_context
def cli(
    click_ctx,
    module: str,
    remote: bool,
    style: str,
    username: str,
    venv: str,
    vars: tuple[str],
    go: bool,
    mix_models_args: list[str],
) -> None:
    """Submit `mix-models ARGS` to a SLURM queue."""

    # Parameters
    # 1. Directly from CLI options and arguments
    # 2. From 0 or more -v
    parameters = dict(
        env_path=venv, mix_models_args=mix_models_args, username=username
    ) | {k: v for k, _, v in map(lambda kv: kv.partition("="), vars)}

    # Prepare CLI args and script body
    args, body = Template.from_module(module).format(
        as_directives=style.startswith("d"), **parameters
    )

    if remote:
        # Prepend args to run the command through SSH
        args = ixmp.config.get("slurm remote args").split("\0") + args

    if go:
        result = run(args, input=body, stdout=PIPE, stderr=STDOUT)

        print(
            f"Command returned exit code {result.returncode}:",
            result.stdout.decode(),
            sep="\n",
        )
        # Propagate exit/return code to the `mix-models sbatch` call
        click_ctx.exit(result.returncode)
    else:
        print(
            "\n  ".join([f"Will invoke `{args[0]}` with arguments:"] + args[1:]),
            "…and standard input:",
            body.decode(),
            sep="\n\n",
        )
