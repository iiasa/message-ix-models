Models and model variants
*************************

.. currentmodule:: message_ix_models.model

.. automodule:: message_ix_models.model
   :members:

:mod:`.model.structure`: Model structure information
====================================================

.. currentmodule:: message_ix_models.model.structure

.. automodule:: message_ix_models.model.structure

.. autofunction:: get_codes

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
