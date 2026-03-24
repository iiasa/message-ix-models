Set up the MESSAGEix ecosystem
*******************************

Install the full MESSAGEix modelling stack — from system dependencies to a verified, working environment.
Commands below are written for a Unix shell (Bash/Zsh on Linux, macOS, or WSL).

.. admonition:: For Windows users

   The core workflow — clone, install with ``uv``, configure, verify — is the same on Windows.
   The differences:

   - **uv installer**: ``powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`` instead of ``curl | sh``.
   - **venv activation**: ``<workdir>\env\message\Scripts\activate`` instead of ``source <workdir>/env/message/bin/activate``.
   - **Shell config**: Windows has no ``~/.bash_profile``. Set environment variables via System Properties or your PowerShell ``$PROFILE``.
   - **GAMS**: use the Windows installer from `gams.com <https://www.gams.com/download/>`_ and add the install directory to your ``PATH``.

   Cluster-specific sections (module loading, compute-node caveats) do not apply.

.. contents::
   :local:
   :backlinks: none

Prerequisite knowledge
======================

This guide assumes:

- Access to the target machine (personal machine, cloud instance, or cluster).
- Familiarity with a terminal/shell (Bash or Zsh), basic command-line operations, and Git.

For background on any of these, see the `MIT Missing Semester <https://missing.csail.mit.edu/>`_.

.. admonition:: On IIASA UniCC

   Access requires an IIASA account.
   File a ticket with ICT requesting UnICC access — see the `Scientific Computing intranet page <https://iiasahub.sharepoint.com/sites/ict/SitePages/Scientific-Computing.aspx>`_.
   In the request, specify:

   - Which existing shared project folders (``/projects/``) you need access to, or whether a new one is needed.
   - Which P: drive folders you need accessible from within the cluster.
   - SSH and SSHFS access, if you want to connect or mount the cluster filesystem from your local machine.

   The home directory defaults to 5--10 GB.
   Request additional storage upfront — 50 GB or more if you plan to work with Git LFS files.

Ensure your `git identity <https://docs.github.com/en/get-started/getting-started-with-git/set-up-git#setting-up-git>`_ is configured (``git config --global user.name`` and ``user.email``).

The following external documentation is referenced throughout:

- `MESSAGEix installation <https://docs.messageix.org/en/stable/install.html>`_ and `advanced installation <https://docs.messageix.org/en/latest/install-adv.html>`_
- `GAMS installation on Unix <https://www.gams.com/latest/docs/UG_UNIX_INSTALL.html>`_
- `uv documentation <https://docs.astral.sh/uv/>`_

System dependencies
===================

The MESSAGEix stack requires three system-level dependencies:

- **Python** (3.10+)
- **Java** (a JRE or JDK — required by the :mod:`ixmp` JDBC backend)
- **GAMS** (with a valid license that includes the CPLEX solver)

On a personal machine, install these through your system package manager or their respective installers.

On a cluster with an `environment module system <https://modules.readthedocs.io/>`_, these are typically provided as loadable modules.

.. admonition:: On IIASA UniCC

   Load the required modules and persist them across sessions by adding the following to :file:`~/.bash_profile`:

   .. code-block:: bash

      module purge
      source /opt/apps/lmod/8.7/init/bash
      module load Python/3.12.3-GCCcore-13.3.0  # or another 3.10+ version available on your cluster
      module load Java
      module load gams
      module load git-lfs

   ``module purge`` clears inherited modules so the environment is reproducible.
   The ``source`` line initialises the `Lmod <https://lmod.readthedocs.io/>`_ module system — this is specific to UniCC's Lmod installation path.

.. _bootstrap-shell-config:

Verify that the dependencies are available:

.. code-block:: bash

   python3 --version   # 3.10+
   java -version        # a JRE or JDK
   gams                 # GAMS version and license info

On a cluster, these commands **should** work after the module loads in :file:`~/.bash_profile` (Bash) or :file:`~/.zprofile` (Zsh) take effect.
Run ``source ~/.bash_profile`` to apply changes to the current session.

GAMS license
------------

GAMS requires a license file (``gamslice.txt``) that includes the CPLEX solver.
The file **must** be placed in the GAMS system directory — the path shown in the ``gams`` output as ``System Directory``.

If GAMS is provided as a system module, the license is typically pre-configured.
Otherwise, obtain a license file from your institution and copy it into the system directory:

.. code-block:: bash

   cp /path/to/gamslice.txt "$(gams | grep 'System Directory' | awk '{print $NF}')"

Verify the license is valid and includes CPLEX:

.. code-block:: bash

   cd /tmp && gams trnsport

The output **should** end with ``Status: Normal completion``.

.. _bootstrap-layout:

Directory layout
================

All paths **must** be on a filesystem visible to both the login node and compute nodes.
Refer to your cluster's documentation for which filesystems are shared.

On clusters where the home directory has limited quota, place repositories and the virtual environment on a larger network drive.
Keep only job scripts and lightweight configuration in ``$HOME``.

**Work drive** (network drive or project directory with adequate space):

.. code-block:: text

   <workdir>/
   ├── env/
   │   └── message/           # virtual environment
   └── repos/
       ├── ixmp/
       ├── message_ix/
       ├── message-ix-models/
       └── message_data/

**Home directory** (lightweight, always accessible):

.. code-block:: text

   $HOME/
   ├── .bash_profile           # shell configuration
   ├── .local/share/ixmp/
   │   └── config.json         # ixmp platform configuration
   └── job/
       ├── solve.sh            # SLURM job scripts
       ├── solve.py            # Python solve scripts
       └── out/                # job output logs

If your home directory has sufficient space, ``<workdir>`` can be ``$HOME``.

.. admonition:: On IIASA UniCC

   The home directory defaults to 5--10 GB — too small for the full MESSAGE stack with LFS files.
   Repositories and the virtual environment **should** go on a network drive instead:

   - **H: drive**: ``/hdrive/all_users/<username>/``
   - **P: drive / project directories**: ``/projects/<project_name>/``

   Request access to the relevant network drives when requesting UnICC access from ICT.
   SLURM compute nodes can access these drives when provisioned — specify the paths you need in your ICT request.

   ``$HOME`` is appropriate for job scripts, shell configuration, and output logs.

.. _bootstrap-venv:

Virtual environment
===================

Use `uv <https://docs.astral.sh/uv/>`_ for environment and package management.
Install it first:

.. code-block:: bash

   curl -LsSf https://astral.sh/uv/install.sh | sh

Create a virtual environment.
All four packages will be installed into this single environment.

.. code-block:: bash

   uv venv <workdir>/env/message --python $(which python3)
   source <workdir>/env/message/bin/activate

.. important::

   The ``--python $(which python3)`` flag is critical on clusters.
   It ensures the venv links to the module-loaded Python rather than a system default.
   Without this, scripts may work on the login node but fail on compute nodes where the system Python is not available.

.. _bootstrap-install:

Install the MESSAGEix stack
===========================

Clone repositories
------------------

Clone all four repositories.
If working on forks, substitute your fork URL and add the upstream remote (``git remote add upstream git@github.com:iiasa/<repo>.git``).

.. code-block:: bash

   git clone git@github.com:iiasa/ixmp.git <workdir>/repos/ixmp
   git clone git@github.com:iiasa/message_ix.git <workdir>/repos/message_ix
   git clone git@github.com:iiasa/message-ix-models.git <workdir>/repos/message-ix-models
   git clone git@github.com:iiasa/message_data.git <workdir>/repos/message_data

.. note::

   ``message_data`` is a private repository.
   Access requires membership in the ``iiasa`` GitHub organisation.

Install packages
----------------

Install all packages as editable into the virtual environment.
The ``-p`` flag targets the venv directly — no activation needed for installation.
Install in dependency order — :mod:`ixmp` first, then :mod:`message_ix`, then :mod:`message_ix_models`, then :mod:`message_data`:

.. code-block:: bash

   uv pip install -p <workdir>/env/message -e <workdir>/repos/ixmp[docs,tests,tutorial]
   uv pip install -p <workdir>/env/message -e <workdir>/repos/message_ix[docs,report,tests,tutorial]
   uv pip install -p <workdir>/env/message -e <workdir>/repos/message-ix-models[docs,tests]
   uv pip install -p <workdir>/env/message -e <workdir>/repos/message_data[tests]

To use the installed tools, activate the environment first:

.. code-block:: bash

   source <workdir>/env/message/bin/activate

Git LFS
-------

:mod:`message_ix_models` and :mod:`message_data` use `Git LFS <https://git-lfs.com/>`_ for large files.
If you need these files (data inputs, test fixtures), install and pull LFS in each repository:

.. code-block:: bash

   cd <workdir>/repos/message-ix-models && git lfs install && git lfs pull
   cd <workdir>/repos/message_data && git lfs install && git lfs pull

This consumes significant storage.
Skip if you do not need the large files for your work.

.. admonition:: On IIASA UniCC

   Git LFS is available via ``module load git-lfs`` (included in the :file:`~/.bash_profile` above).
   Request at least 50 GB of home directory storage from ICT if you plan to pull LFS files for both repositories.

Platform configuration
======================

:mod:`ixmp` stores platform definitions (database connection details) in a configuration file at :file:`{$IXMP_DATA}/config.json`.
By default, ``IXMP_DATA`` is :file:`~/.local/share/ixmp/`.
A local HyperSQL platform is created automatically on first use.

For access to the shared ``ixmp_dev`` database (IIASA staff and collaborators), refer to the :mod:`ixmp` documentation on `platforms and backends <https://docs.messageix.org/projects/ixmp/en/latest/api-backend.html>`_ and :ref:`ixmp:configuration`.

To inspect your current configuration:

.. code-block:: bash

   mix-models config show

The output includes the path to :file:`config.json` and all registered platforms.

.. note::

   On a cluster, batch jobs need access to this configuration.
   If the default path (``~/.local/share/ixmp/``) is accessible from compute nodes, no extra setup is needed.
   Otherwise, set the ``IXMP_DATA`` environment variable in your job script to point to a location that contains :file:`config.json`:

   .. code-block:: bash

      export IXMP_DATA=/path/to/ixmp/config/dir

Verify the installation
=======================

.. code-block:: bash

   message-ix show-versions

This **should** print version numbers for :mod:`ixmp`, :mod:`message_ix`, :mod:`message_ix_models`, GAMS, Java, and Python.
If any component is missing or shows an error, revisit the relevant section above.
