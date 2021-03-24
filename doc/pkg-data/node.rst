.. _node-yaml:

Node code lists
***************

The codes in these lists denote **regions** and **countries**.

When loaded using :func:`.get_codes`, the :attr:`.Code.child` attribute is a list of child codes.
See the function documentation for how to retrieve these.

.. contents::
   :local:

Models with global scope
========================

.. _R32:

32-region aggregation (``R32``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/R32.yaml
   :language: yaml

.. _R14:

14-region aggregation (``R14``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/R14.yaml
   :language: yaml

.. _R11:

11-region aggregation (``R11``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/R11.yaml
   :language: yaml

.. _RCP:

5-region aggregation (``RCP``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/RCP.yaml
   :language: yaml


Others
======

These include models scoped to a single country or region, or a subset of all countries or regions.

.. _ISR:

Israel (``ISR``)
----------------

.. literalinclude:: ../../message_ix_models/data/node/ISR.yaml
   :language: yaml
