Set up the full MESSAGEix-GLOBIOM stack
***************************************

- Start with a complete sentence and use complete sentences throughout.
- Collect links to install instructions for individual package.
- Define all acronyms on first use.
- Use intersphinx for links to other Sphinx documentation.
- Use :program: Sphinx role.
- Deduplicate text that appears in 2+ places.
- Use semantic line breaks per code style.

This guide explains HOWTO set up a full MESSAGEix-GLOBIOM ‘stack’. [1]_
Some packages in this stack have individual installation documentation:
:doc:`message-ix-models </install>`,
message_ix (:doc:`quick <message-ix:install>` and :doc:`advanced <message-ix:install-adv>`),
and :doc:`ixmp <ixmp:install>`.
This guide complements with additional information needed for a functioning whole,
focusing on a specific sub-set of cases.

.. [1] Per `Wikipedia <https://en.wikipedia.org/wiki/Solution_stack>`_,
   “a set of software components needed to create a complete platform
   such that no additional software is needed to support applications.”

.. contents::
   :local:
   :backlinks: none

Prerequisites
=============

Knowledge and skills
--------------------

See also :doc:`message-ix:prereqs` in the :mod:`message_ix` documentation.
This guide assumes you have both the “Basic usage” and “Advanced usage” scientific computing skills,
in particular familiarity with…

   - using a terminal/shell/command-line such as Bash or Zsh.
   - the Git version control system.

That page links to some learning resources.
Some additional resources relevant to this guide are:

- `MIT Missing Semester <https://missing.csail.mit.edu/>`_.
- `GAMS installation on Unix <https://www.gams.com/latest/docs/UG_UNIX_INSTALL.html>`_
- `uv documentation <https://docs.astral.sh/uv/>`_

.. todo:: Include or update the above links in :doc:`message-ix:prereqs`.

.. note:: If any of the following instructions are confusing,
   that could indicate one of two things:

   1. You do not have the pre-requisite knowledge and skills mentioned above.
      In this case, make a note of the specific item(s),
      and schedule time to learn them.
      You can proceed with the rest of the HOWTO,
      but may encounter further obstacles that are hard to overcome.
   2. This HOWTO is incomplete or erroneous.
      Please ask for help.
      Once the missing or misleading information is identified,
      then open an issue or pull request to add or correct it.

A computer
----------

You also need **access to a target machine**,
a computer on which you will install the stack.
This could be one of:

1. Access to a shared system or server, in particular the Unified IIASA Compute Cluster (UnICC).
2. A personal machine, for instance a laptop or desktop owned by or issued to you.
3. A ‘cloud’ server or virtual machine, for instance from a commercial provider.

For all three, a Unix-like system is assumed:
Linux, macOS, or the Windows Subsystem for Linux (WSL).
(1) in particular is a Linux system.
The following box includes some caveats for Windows users.

.. admonition:: For Windows users

   The core workflow—clone, install with :program:`uv`, configure, verify—is the same on Windows.
   We recommend :program:`uv` over :program:`conda` for environment management.
   The differences:

   - **uv installer**: ``powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`` instead of ``curl | sh``.
   - **Java**: :program:`conda` depends on a JDK, so Java is available implicitly in conda environments.
     With :program:`uv`, install a JDK separately;
     follow the link below under “System dependencies”.
   - **venv activation**: ``<workdir>\env\message\Scripts\activate`` instead of ``source <workdir>/env/message/bin/activate``.
   - **Shell config**: Windows has no ``~/.bash_profile``.
     Set environment variables via System Properties or your PowerShell ``$PROFILE``.
   - **GAMS**: use the GAMS Windows installer.
     Follow the specific instructions in the message_ix :doc:`message-ix:install-adv`
     to add the installation directory to your ``PATH`` environment variable.

   Cluster-specific sections (module loading, compute-node caveats) do not apply.

Access to UnICC
---------------

In the specific case of UnICC, access requires an IIASA account.
To obtain this, first read the
`Scientific Computing page <https://iiasahub.sharepoint.com/sites/ict/SitePages/Scientific-Computing.aspx>`_
on the IIASA intranet (not public),
in particular the file :file:`Slurm User Guide.pdf` (download or bookmark this file).
Per the section “Requesting access” in that document,
file a ticket with ICT to request access.
Include all the information mentioned in the User Guide: 

- Which existing shared project folders (subdirectories of :file:`/projects/`) you need access to,
  or whether a new one is needed.
  If you are working on a project and don't know if any such folders exist,
  ask your collaborators, or explicitly write ‘none’.
- Which P: drive folders you need accessible from within the cluster.
- SSH access, if you want to connect or mount the cluster filesystem from your local machine.
- If you plan to clone Git LFS files to your home directory,
  request additional home directory space, for instance 50 GB.
  The instructions below place these files on a network drive,
  so this is optional.

GitHub account and :mod:`message_data` access
---------------------------------------------

:mod:`message_data` is in a private repository,
only accessible to IIASA ECE staff and authorized collaborators.
Access is granted via membership in teams under the `@iiasa GitHub organisation <https://github.com/iiasa>`_;
this in turn requires a GitHub account.

- Create an account.
- Request access from a colleague.

System dependencies
===================


The MESSAGEix stack requires three system-level dependencies.
The following links go to the message_ix advanced installation guide,
which has further detail about each one.

- :ref:`Python <message-ix:install-python>`.
  Currently a minimum of version 3.10 is required.
  Newer Python versions can bring performance improvements;
  use the most recent version you can install.
- :ref:`Java <message-ix:install-java>`.
  Either a JRE or JDK is required by the :class:`ixmp.JDBCBackend`.
- :ref:`GAMS <message-ix:install-gams>`.
  A valid license is required; see below.

On a personal machine,
install these through your system package manager or their respective installers.

On a cluster with an `environment module system <https://modules.readthedocs.io/>`_,
these are typically provided as loadable modules.
The modules **may** be loaded manually (:program:`module load <name>`),
but **should** be added to a profile file such as :file:`~/.bash_profile` (Bash) or :file:`~.zprofile` (Zsh).
This file will be automatically run for every new shell.

.. code-block:: bash

   # Clear any loaded modules so the following have a deterministic effect
   module purge

   # Initialize the Lmod system. This path is specific to UnICC.
   source /opt/apps/lmod/8.7/init/bash

   # Python use this or any other 3.10+ version available
   module load Python/3.12.3-GCCcore-13.3.0
   module load Java
   module load gams
   module load git-lfs  # For Git LFS; see below

For these to take effect, either log out and in again,
or run :program:`source ~/.bash_profile` or similar.

.. _bootstrap-shell-config:

Verify that the dependencies are available:

.. code-block:: bash

   python3 --version   # 3.10+
   java -version       # a JRE or JDK
   gams                # GAMS version and license info

Install a GAMS license
----------------------

GAMS requires a license file (:file:`gamslice.txt`)
that includes the CPLEX solver used by MESSAGE.
The file **must** be placed in the GAMS system directory.
The path shown in the :program:`gams` output as ``System Directory``.

If GAMS is provided as a system module,
the license is typically pre-configured.
Otherwise,
obtain a license file from your institution and copy it into the system directory:

.. code-block:: bash

   cp /path/to/gamslice.txt "$(gams | grep 'System Directory' | awk '{print $NF}')"

Verify the license is valid and includes CPLEX:

.. code-block:: bash

   cd /tmp && gams trnsport

The output **should** end with ``Status: Normal completion``.

Configure Git
-------------

If you will be creating and pushing commits from Git repositories on the target machine,
ensure your `git identity <https://docs.github.com/en/get-started/getting-started-with-git/set-up-git#setting-up-git>`_ is configured:

.. code-block:: bash

   git config --global user.name "Your Full Name"
   git config --global user.email your.name@iiasa.ac.at

.. _bootstrap-layout:

Choose a directory layout
=========================

This guide uses two conceptual directories:
a **work directory** and a **home directory**.
Each contains subdirectories and files as follows:

Work directory
   The shorthand :file:`<workdir>` is used to refer to this directory.
   This directory contains repositories and a virtual environment.
   These can be larger, more than 5 GB.

   .. code-block:: text
   
      <workdir>/
      ├── env/
      │   └── message/            # Virtual environment containing the stack
      └── repos/
          ├── ixmp/
          ├── message_ix/
          ├── message-ix-models/
          └── message_data/

Home directory
   The shorthand ``$HOME`` is used to refer to this directory.
   This directory contains configuration for the shell,
   for ixmp, and job scripts and logs.
   These are collectively lightweight.

   .. code-block:: text
   
      $HOME/
      ├── .bash_profile           # Module configuration; see above
      ├── .local/share/ixmp/
      │   └── config.json         # ixmp platform configuration
      └── job/
          ├── solve.sh            # SLURM job scripts
          ├── solve.py            # Python solve scripts
          └── out/                # job output logs

These two directories **may** be the same:
in other words, :file:`<workdir>` and ``$HOME`` may be the same directory.
Or, they may be different directories on the same filesystem, [2]_
or on separate filesystems.

.. [2] This term can refer to individual volumes on the same physical disk,
   entire disks,
   or network filesystems that can be on a different computer and may span over
   2 or more physical disks.

On a personal machine or cloud instance,
the same filesystem or directory is a sound choice,
so long as it is sufficiently large.

On a cluster:

- Both directories **must** be on filesystems that are mounted to both the login node and compute nodes.
  Refer to your cluster's documentation for which filesystems are configured this way.
  If this is not the case,
  code or commands may work on the login node
  yet fail when jobs are submitted to compute nodes.
- Where ``$HOME`` is on a drive with limited space
  (for example, the standard quota of 5 or 10 GB on UnICC),
  place :file:`<workdir>` on a larger, network drive.
  On UnICC, this could be:

  - H: drive: :file:`/hdrive/all_users/<username>/`
  - P: drive/project directories: :file:`/projects/<project_name>/`.
    The available directories will include those mentioned in your ICT access request.
    You can also open additional requests to be granted access to more directories.

  Both of these meet the above criterion:
  they are mounted on both the login and compute nodes.

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

The :program:`--python` argument ensures that the environment uses the specific Python executable provided via the module system,
rather than (a) a system-wide Python that may not be available on compute nodes,
or (b) downloading and installing a different Python version managed by :program:`uv`.

.. _bootstrap-install:

Install the MESSAGEix stack
===========================

Clone repositories
------------------

Clone all four repositories.
If working on forks,
substitute your fork URL and add the upstream remote,
for instance :program:`git remote add upstream git@github.com:iiasa/<repo>.git`.

.. code-block:: bash

   git clone git@github.com:iiasa/ixmp.git <workdir>/repos/ixmp
   git clone git@github.com:iiasa/message_ix.git <workdir>/repos/message_ix
   git clone git@github.com:iiasa/message-ix-models.git <workdir>/repos/message-ix-models
   git clone git@github.com:iiasa/message_data.git <workdir>/repos/message_data

Pull Git LFS files
------------------

:mod:`message_ix_models` and :mod:`message_data` use `Git LFS <https://git-lfs.com/>`_ for large files,
including input data and data for testing.
If you do not need these files for your work,
you can skip this section.
If you need these files and are on UnICC
you **must** either:

- Place :file:`<workdir>` on a large, network drive, as discussed above, or
- Request a larger home directory quota (at least 50 GB) from IIASA ICT.

The above :file:`~/.bash_profile` lines load the module for Git LFS.
On other machines, follow its install instructions via the above link.
Then ‘install’ Git LFS configuration in each repository and pull the files:

.. code-block:: bash

   cd <workdir>/repos/message-ix-models && git lfs install && git lfs pull
   cd <workdir>/repos/message_data && git lfs install && git lfs pull

Install packages
----------------

Install all packages into the virtual environment in dependency order:

.. code-block:: bash

   uv pip install -p <workdir>/env/message -e <workdir>/repos/ixmp[docs,tests,tutorial]
   uv pip install -p <workdir>/env/message -e <workdir>/repos/message_ix[docs,report,tests,tutorial]
   uv pip install -p <workdir>/env/message -e <workdir>/repos/message-ix-models[docs,tests]
   uv pip install -p <workdir>/env/message -e <workdir>/repos/message_data[tests]

- The :program:`-e` option selects an editable install:
  this means that any modifications you make to files in the Git working trees will have effect when the code is run.
  It also avoids creating a copy of the repository files;
  the copy can be large if Git LFS is used.
- The :program:`-p` option targets the environment without needing to activate it.

To use the installed tools, activate the environment:

.. code-block:: bash

   source <workdir>/env/message/bin/activate

Configure :class:`ixmp.Platform`
================================

:mod:`ixmp` stores platform definitions (database connection details) in a configuration file at :file:`$IXMP_DATA/config.json`.
A local HyperSQL platform is created automatically on first use.

For access to the shared ``ixmp_dev`` database (IIASA staff and collaborators),
refer to the :mod:`ixmp` documentation on :doc:`platforms and backends <ixmp:api-backend>` and :ref:`ixmp:configuration`.

To inspect your current configuration:

.. code-block:: bash

   mix-models config show

The output includes the path to :file:`config.json` and all registered platforms.

If the ``IXMP_DATA`` environment variable is not set, it defaults to :file:`$HOME/.local/share/ixmp`.
On UnICC,
this default directory is accessible from compute nodes and no extra setup is needed.
Otherwise,
set ``IXMP_DATA`` in your job script or :file:`~/.bash_profile`
to point to a location that contains :file:`config.json`:

.. code-block:: bash

   export IXMP_DATA=/path/to/ixmp/config/dir

Verify the installation
=======================

.. code-block:: bash

   message-ix show-versions

This **should** print version numbers for :mod:`ixmp`,
:mod:`message_ix`,
:mod:`message_ix_models`,
GAMS,
Java,
and Python.
If any component is missing or shows an error,
revisit the relevant section above.
