BMT workflow (:mod:`.model.bmt`)
********************************

The acronym **“BMT”** refers to a configuration of MESSAGEix-GLOBIOM
that combines all 3 of the
:doc:`/buildings/index` (**B**),
:doc:`MESSAGEix-Materials </material/index>` (**M**), and
:doc:`MESSAGEix-Transport </transport/index>` (**T**)
model variants.

In :func:`.bmt.workflow.generate`, each variant is added as follows:

- **Materials (M)** — load a baseline that already includes MESSAGEix-Materials
  (step ``M``); there is no separate materials build step yet.
- **Transport (T)** — build MESSAGEix-Transport on the cloned materials scenario
  via :func:`.transport.workflow.add_steps` and :func:`.transport.build.main`,
  using the ``transport`` section of :file:`data/bmt/config.yaml``.
- **Buildings (B)** — in step ``BMT built``, call :func:`.buildings.build.main`
  (imported in :mod:`.bmt.workflow` as ``build_B``) on the ``MT solved`` scenario.
- **Other BMT extensions** — add power-sector material intensity with
  :func:`.bmt.utils.build_PM` (``BMTX built``); later steps prepare MACRO
  calibration and solve with MESSAGE-MACRO.

The current module :mod:`.model.bmt` includes:

- :func:`.bmt.workflow.generate` —generates a :class:`.Workflow`
  that chains steps to build all 3 variants on a base scenario.
  See the function documentation for complete details.
- :mod:`.bmt.cli` —the :program:`mix-models bmt run` CLI subcommand
  used to invoke the workflow.
  For example::

    mix-models bmt run --from="base" "BMTX baseline macro reported" --dry-run

  See :program:`mix-models bmt run --help` for options.
- :mod:`.bmt.config` —handling for configuration,
  which is read from a file :file:`data/bmt/config.yaml`.
  See the module documentation for a description of the file format.

Code reference
==============

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   message_ix_models.model.bmt
