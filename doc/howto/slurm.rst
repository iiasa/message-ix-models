Run MESSAGEix solves via SLURM
*******************************

Submit, monitor, and debug MESSAGEix scenario solves on a SLURM cluster.
This guide assumes a working MESSAGEix environment on the cluster's compute nodes (see :doc:`bootstrap`).

.. contents::
   :local:
   :backlinks: none

Prerequisite knowledge
======================

- A working MESSAGEix environment (:doc:`bootstrap`)
- Basic shell scripting

The following SLURM documentation is referenced throughout.
Consult these for the full set of options — this guide covers only what is needed for MESSAGEix:

- `sbatch <https://slurm.schedmd.com/sbatch.html>`_ — submit batch scripts
- `squeue <https://slurm.schedmd.com/squeue.html>`_ — view job queue
- `scontrol <https://slurm.schedmd.com/scontrol.html>`_ — inspect job details
- `scancel <https://slurm.schedmd.com/scancel.html>`_ — cancel jobs
- `sacct <https://slurm.schedmd.com/sacct.html>`_ — query completed job history

Shared filesystem
=================

SLURM job scripts run on compute nodes, not the login node where you submit them.
All paths in the script — the venv, repositories, output directory — **must** be on a filesystem visible to compute nodes.
On most clusters the home directory is network-mounted and shared; verify with your cluster documentation.

See :ref:`bootstrap-layout` for the recommended directory layout.

Anatomy of a job script
========================

A SLURM job script is a shell script with ``#SBATCH`` directives that declare resource requests.
The scheduler reads these directives, allocates resources, and runs the script body on a compute node.

.. code-block:: bash

   #!/bin/bash
   #SBATCH --time=3:00:00
   #SBATCH --mem=32G
   #SBATCH --cpus-per-task=4
   #SBATCH --mail-type=BEGIN,END,FAIL
   #SBATCH --mail-user=$USER@iiasa.ac.at
   #SBATCH --output=$HOME/slurm/solve_%J.out

   # --- Environment setup ---
   module purge
   source /opt/apps/lmod/8.7/init/bash
   module load Python/3.12.3-GCCcore-13.3.0
   module load Java
   module load gams
   module load git-lfs

   source <workdir>/env/message/bin/activate

   # --- Run ---
   python ~/job/solve.py

.. warning::

   Use ``$HOME`` in ``#SBATCH`` directives, **not** ``~``.
   SLURM does not expand tilde in directives — it creates a literal directory named ``~`` under your home.

The directives explained:

``--time=3:00:00``
   Wall-clock limit.
   A single MESSAGEix-GLOBIOM solve typically takes 10--60 minutes.
   Allow headroom, but do not request days — overprovisioning blocks other users.

``--mem=32G``
   Memory allocation.
   See :ref:`slurm-resource-sizing` for guidance.

``--cpus-per-task=4``
   CPU cores for CPLEX parallelism.
   See :ref:`slurm-resource-sizing` for guidance.

``--output=$HOME/slurm/solve_%J.out``
   Combined stdout and stderr.
   ``%J`` is replaced by the job ID.
   Create the output directory before first use: ``mkdir -p $HOME/slurm``.

   To separate stderr into its own file, add ``--error``:

   .. code-block:: bash

      #SBATCH --output=$HOME/slurm/solve_%J.out
      #SBATCH --error=$HOME/slurm/solve_%J.err

.. admonition:: On IIASA UniCC

   The ``module purge`` / ``source`` / ``module load`` block is specific to UniCC's Lmod setup.
   On other clusters, replace with the equivalent module commands for your system.
   The mail address pattern (``@iiasa.ac.at``) is also UniCC-specific.

.. _slurm-solve-script:

Example solve script
====================

A minimal Python script that loads and solves a scenario:

.. code-block:: python

   """Solve a MESSAGEix scenario on a cluster."""
   import message_ix

   mp = message_ix.Platform("ixmp_dev")
   scen = message_ix.Scenario(mp, "model_name", "scenario_name")

   scen.solve("MESSAGE")

   mp.close_db()

Replace ``model_name`` and ``scenario_name`` with actual values.
For MESSAGE-MACRO solves, use ``scen.solve("MESSAGE-MACRO")``.

Submit, monitor, and inspect
============================

Submit
------

.. code-block:: bash

   sbatch ~/job/solve.sh

On success, SLURM prints ``Submitted batch job <JOBID>``.

Monitor
-------

Check your active jobs:

.. code-block:: bash

   squeue -u $USER

The ``ST`` column shows job state: ``PD`` (pending — waiting for resources), ``R`` (running).

Cancel a job:

.. code-block:: bash

   scancel <JOBID>

Inspect a specific job:

.. code-block:: bash

   scontrol show jobid <JOBID>

Key fields: ``JobState``, ``ExitCode`` (``0:0`` = success), ``RunTime``, ``MaxRSS`` (peak memory).

After completion
----------------

Query finished jobs with resource usage:

.. code-block:: bash

   sacct -j <JOBID> --format=jobid,state,elapsed,maxrss,exitcode

Read the output log:

.. code-block:: bash

   less $HOME/slurm/solve_<JOBID>.out

If the job failed, ``ExitCode`` in ``sacct`` and the output log together identify the cause.
Common failures:

- ``ExitCode=1:0``: the Python script raised an exception — check the log.
- ``TIMEOUT``: wall-clock limit exceeded — increase ``--time``.
- ``OUT_OF_MEMORY``: increase ``--mem``.

.. _slurm-resource-sizing:

Resource sizing
===============

For a **single solve**, the following is a reasonable starting point:

.. list-table::
   :header-rows: 1

   * - Resource
     - Value
     - Rationale
   * - ``--cpus-per-task``
     - 4
     - Only CPLEX's barrier method is parallel; everything else is single-threaded. Diminishing returns beyond 4 for typical MESSAGE problems.
   * - ``--mem``
     - 32G
     - ~10 GB for a single solve. 64G+ if running :doc:`legacy reporting </api/report/legacy>` on multiple scenarios in sequence (memory accumulates).
   * - ``--time``
     - 3:00:00
     - Most solves complete in under 1 hour. Adjust based on experience with your scenario.

For **parallel solves** within a single job (multiple scenarios), divide available CPUs across solves:

.. code-block:: bash

   Total CPUs / number of parallel solves = threads per solve

For example, with ``--cpus-per-task=128`` and 8 scenarios, each solve gets 16 threads.

.. admonition:: On IIASA UniCC

   Compute nodes have 128 cores / 256 threads.
   The default memory allocation if ``--mem`` is not specified is 2 GB — too low for MESSAGE solves.
   Always specify ``--mem`` explicitly.

:program:`mix-models sbatch`
============================

:mod:`message_ix_models` provides a convenience command that generates and optionally submits SLURM job scripts:

.. code-block:: bash

   mix-models sbatch --go -- --url="ixmp://ixmp_dev/model/scenario" build

This wraps the ``mix-models`` invocation in a SBATCH script using a default template (:data:`~message_ix_models.util.slurm.DEFAULT`).

.. note::

   ``mix-models sbatch`` requires :mod:`message_ix_models` to be installed and available on the submission node.
   If your login/scheduling node does not have the MESSAGEix environment, use hand-written job scripts as above.

See :mod:`.util.slurm` for full documentation and template customisation.
