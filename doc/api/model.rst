.. currentmodule:: message_ix_models.model

Models and variants (:mod:`~message_ix_models.model`)
*****************************************************

Submodules described on this page:

.. contents::
   :local:
   :backlinks: none

Submodules described on separate pages:

- :doc:`/api/model-bare`
- :doc:`/api/model-build`
- :doc:`/api/disutility`
- :doc:`/api/model-emissions`
- :doc:`/api/model-snapshot`

.. automodule:: message_ix_models.model
   :members:

.. currentmodule:: message_ix_models.model.macro

:mod:`.model.macro`: MESSAGE-MACRO
==================================

.. automodule:: message_ix_models.model.macro
   :members:

:mod:`.model.structure`: Model structure information
====================================================

.. currentmodule:: message_ix_models.model.structure

.. automodule:: message_ix_models.model.structure
   :members:
   :exclude-members: get_codes

   .. autosummary::

      codelists
      generate_product
      generate_set_elements
      get_codes
      get_region_codes
      process_units_anno
      process_commodity_codes
      process_technology_codes

.. autofunction:: get_codes

   The available code lists are reproduced as part of this documentation.
   The returned code objects have annotations that vary by set.
   See:

   - :doc:`/pkg-data/codelists`
   - :doc:`/pkg-data/node`

   Also available is :file:`cd_links/unit.yaml`.
   This is a project-specific list of units appearing in CD-LINKS scenarios.

   **Example:**

   .. code-block:: python

      >>> from message_ix_models.model.structure import get_codes
      >>> codes = get_codes("node/R14")

      # Show the codes
      >>> codes
      [<Code ABW: Aruba>,
       <Code AFG: Afghanistan>,
       <Code AGO: Angola>,
       ...
       <Code ZWE: Zimbabwe>,
       <Code World: World>,
       ...
       <Code R11_PAS: Other Pacific Asia>,
       <Code R11_SAS: South Asia>,
       <Code R11_WEU: Western Europe>]

      # Retrieve one code matching a certain ID
      >>> world = codes[codes.index("World")]

      # Get its children's IDs strings, e.g. for a "node" dimension
      >>> [str(c) for c in world.child]
      ['R11_AFR',
       'R11_CPA',
       'R11_EEU',
       'R11_FSU',
       'R11_LAM',
       'R11_MEA',
       'R11_NAM',
       'R11_PAO',
       'R11_PAS',
       'R11_SAS',
       'R11_WEU']

      # Navigate from one ISO 3166-3 country code to its parent
      >>> AUT = codes[codes.index("AUT")]
      >>> AUT.parent
      <Code R11_WEU: Western Europe>

   .. seealso:: :obj:`.adapt_R11_R14`
