Other code lists
****************

.. contents::
   :local:

.. _commodity-yaml:

Commodities (``commodity.yaml``)
================================

Each of these codes has the following annotations:

``level``
   Level where this commodity typically (not exclusively) occurs.
``unit``
   Units typically associated with this commodity.

.. literalinclude:: ../../message_ix_models/data/commodity.yaml
   :language: yaml

.. _level-yaml:

Levels (``level.yaml``)
=======================

This code list has no annotations and no hierarchy.

.. literalinclude:: ../../message_ix_models/data/level.yaml
   :language: yaml


.. _technology-yaml:

Technologies (``technology.yaml``)
==================================

.. warning:: This list is *only for reference*; particular MESSAGE-GLOBIOM scenarios may not contain all these technologies, or may contain other technologies not listed.

Each of these codes has the following annotations:

``sector``
   A categorization of the technology.
``input``
   (``commodity``, ``level``) for input to the technology.
``output``
   (``commodity``, ``level``) for output from the technology.
``vintaged``
   :obj:`True` if the technology is subject to vintaging.
``type``
   Same as ``output[1]``.


.. literalinclude:: ../../message_ix_models/data/technology.yaml
   :language: yaml

Others
======

.. literalinclude:: ../../message_ix_models/data/sdmx/ICONICS_SSP(2017).xml
   :language: xml

.. literalinclude:: ../../message_ix_models/data/sdmx/ICONICS_SSP(2024).xml
   :language: xml

.. literalinclude:: ../../message_ix_models/data/sdmx/IIASA_ECE_AGENCIES(0.1).xml
   :language: xml
