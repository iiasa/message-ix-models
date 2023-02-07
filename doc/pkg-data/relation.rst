.. _relation-yaml:

Relations (:file:`relation/*.yaml`)
***********************************

These lists provide codes (ID, optional name and description) for the ``relation`` :ref:`MESSAGEix set <message-ix:section_set_def>`.
See the :doc:`MESSAGEix documentation <message-ix:index>` sections on:

- :ref:`message-ix:section_of_generic_relations`.
- :ref:`message-ix:section_parameter_generic_relations`.

The codes in these lists have the following annotations:

``group``
   1 or more strings identifying (a) group(s) to which the relations belong(s).

``parameters``
   1 or more strings indicating relation parameters for which there should be entries corresponding to this relation ID.
   For example, "activity, lower" indicates there should be ``relation_activity`` and ``relation_lower`` values for this relation ID.

``technology-entry``
   .. todo:: Describe the meaning of this annotation.

.. contents::
   :local:

List ``A``
----------

.. literalinclude:: ../../message_ix_models/data/relation/A.yaml
   :language: yaml

List ``B``
----------

.. literalinclude:: ../../message_ix_models/data/relation/B.yaml
   :language: yaml

List ``CD-LINKS``
-----------------

.. literalinclude:: ../../message_ix_models/data/relation/CD-LINKS.yaml
   :language: yaml
