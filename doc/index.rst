Tools for MESSAGEix-GLOBIOM models
**********************************

:mod:`message_ix_models` provides tools for research using the **MESSAGEix-GLOBIOM family of models** developed by the IIASA Energy, Climate, and Environment (ECE) Program and its collaborators.
This ‘family’ includes single-country and other models derived from the main, global model; all built in the `MESSAGEix framework <https://docs.messageix.org>`_ and on the `ix modeling platform (ixmp) <https://docs.messageix.org/ixmp/>`_.

Among other tasks, the tools allow modelers to:

- retrieve input data from various upstream sources,
- process/transform upstream data into model input parameters,
- create, populate, modify, and parametrize scenarios,
- conduct model runs,
- set up model *variants* with additional details or features, and
- report quantities computed from model outputs.

.. toctree::
   :maxdepth: 1
   :caption: User guide

   install
   data
   cli
   repro
   distrib
   bibliography

.. toctree::
   :maxdepth: 2
   :caption: API reference

   api/model
   api/model-bare
   api/model-build
   api/model-emissions
   api/model-snapshot
   api/disutility
   api/project
   api/tools
   api/util
   api/testing
   api/workflow

.. toctree::
   :maxdepth: 2
   :caption: Variants and projects

   global/index
   water/index

.. toctree::
   :maxdepth: 2
   :caption: Package data

   pkg-data/node
   pkg-data/relation
   pkg-data/year
   pkg-data/codelists
   pkg-data/iiasa-se

.. toctree::
   :maxdepth: 2
   :caption: Development

   develop
   whatsnew
   migrate
   releasing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
