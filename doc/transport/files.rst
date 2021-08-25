Configuration files
*******************

Configuration (:file:`transport/R11/config.yaml`)
=================================================

Note that this is the configuration file for an R11 base model.
The configuration file contains some assumptions/settings that are specific to the node list used in the base model.
To use MESSAGEix-Transport on a base model with a different node list, there **must** be a corresponding file :file:`data/transport/{regions}/config.yaml`, where `regions` is the :attr:`.Context.regions` setting.

.. literalinclude:: ../../../../data/transport/R11/config.yaml
   :language: yaml

Technology (:file:`transport/technology.yaml`)
==============================================

Code list for the 'technology' dimension.

.. literalinclude:: ../../../../data/transport/technology.yaml
   :language: yaml


Other sets (:file:`transport/set.yaml`)
=======================================

Code lists for other dimensions.

.. literalinclude:: ../../../../data/transport/set.yaml
   :language: yaml
