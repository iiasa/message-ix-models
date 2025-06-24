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
   :maxdepth: 2
   :caption: User guide

   install
   data
   cli
   howto/index
   repro
   distrib
   develop
   whatsnew
   bibliography

API reference
=============

.. currentmodule:: message_ix_models

Commonly used classes may be imported directly from :mod:`message_ix_models`.

.. automodule:: message_ix_models

   .. autosummary::

      .Config
      .Context
      .ScenarioInfo
      .Spec
      .Workflow

- :doc:`api/model`
- :doc:`api/model-bare`
- :doc:`api/model-build`
- :doc:`api/model-emissions`
- :doc:`api/model-snapshot`
- :doc:`api/model-workflow`
- :doc:`api/disutility`
- :doc:`api/report/index`
- :doc:`api/tools`
- :doc:`api/data-sources`
- :doc:`api/workflow`
- :doc:`api/util`
- :doc:`api/testing`
- :doc:`api/tests`

.. toctree::
   :maxdepth: 2
   :caption: API reference
   :hidden:

   api/model
   api/model-bare
   api/model-build
   api/model-emissions
   api/model-snapshot
   api/model-workflow
   api/disutility
   api/report/index
   api/tools
   api/tools-costs
   api/tools-messagev
   api/data-sources
   api/workflow
   api/util
   api/testing
   api/tests


.. toctree::
   :maxdepth: 2
   :caption: Model variants

   global/index
   buildings/index
   material/index
   transport/index
   water/index

.. toctree::
   :maxdepth: 2
   :caption: Research projects

   project/advance
   project/alps
   project/carbon-direct
   project/cfr
   project/circeular
   project/ecemf
   project/edits
   project/elevate
   project/engage
   project/gea
   project/geidco
   project/genie
   project/guide
   project/hyway
   project/navigate
   project/newpathways
   project/nextgen-carbon
   project/ngfs
   project/prisma
   project/scenariomip
   project/shape
   project/sparccle
   project/ssp
   project/uptake

.. toctree::
   :maxdepth: 2
   :caption: Package data

   pkg-data/node
   pkg-data/relation
   pkg-data/year
   pkg-data/codelists
   pkg-data/iiasa-se

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
