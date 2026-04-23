BMT workflow (:mod:`.model.bmt`)
********************************

The acronym **“BMT”** refers to a configuration of MESSAGEix-GLOBIOM
that combines all 3 of the
:doc:`/buildings/index` (**B**),
:doc:`MESSAGEix-Materials </material/index>` (**M**), and
:doc:`MESSAGEix-Transport </transport/index>` (**T**)
model variants.

The current module :mod:`.model.bmt` includes:

- :func:`.bmt.workflow.generate` —generates a :class:`.Workflow`
  that chains steps to build all 3 variants on a base scenario.
  See the function documentation for complete details.
- :mod:`.bmt.cli` —the :program:`mix-models bmt run` CLI subcommand
  used to invoke the workflow.
  For example::

    mix-models bmt run --from="base" "glasgow+" --dry-run

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
