.. _node-yaml:

Node code lists
***************

The codes in these lists denote **regions** and **countries**.

When loaded using :func:`.get_codes`, the :attr:`.Code.child` attribute is a list of child codes.
See the function documentation for how to retrieve these.

.. seealso:: :obj:`.adapt_R11_R12`, :obj:`.adapt_R11_R14`, :func:`.identify_nodes`.

.. contents::
   :local:

Models with global scope
========================

.. _R32:

32-region aggregation (``R32``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/R32.yaml
   :language: yaml

.. _R20:

20-region aggregation (``R20``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/R20.yaml
   :language: yaml

.. _R17:

17-region aggregation (``R17``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/R17.yaml
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

.. _R12:

12-region aggregation (``R12``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/R12.yaml
   :language: yaml

.. _RCP:

5-region aggregation (``RCP``)
-------------------------------

.. literalinclude:: ../../message_ix_models/data/node/RCP.yaml
   :language: yaml


Others
======

These include models scoped to a single country or region, or a subset of all countries or regions, as well as code lists used in specific data sets from which :mod:`message_ix_models` handles data.

.. _ADVANCE-nodes:

ADVANCE project (``ADVANCE``)
-----------------------------

.. literalinclude:: ../../message_ix_models/data/node/ADVANCE.yaml
   :language: yaml

.. _ISR:

Israel (``ISR``)
----------------

.. literalinclude:: ../../message_ix_models/data/node/ISR.yaml
   :language: yaml

.. _ZMB:

Zambia (``ZMB``)
----------------

.. literalinclude:: ../../message_ix_models/data/node/ZMB.yaml
   :language: yaml
