Water Sector Linkage
*********************

:mod:`message_data.model.water` adds water usage and demand related representation to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Water”**. This work extends the water sector linkage described by Parkinson et al. (2019) :cite:`Parkinson2019`.



.. contents::
   :local:

Usage
=====



Code reference
==============

.. currentmodule:: message_data.model.water

.. automodule:: message_data.model.water
   :members:

Build and run
-------------
.. automodule:: message_data.model.water.build
   :members:


Data preparation
----------------

.. automodule:: message_data.model.water.data
   :members:

.. automodule:: message_data.model.water.data.water_for_ppl
   :members:

.. automodule:: message_data.model.water.data.waste_t_d
   :members:

.. automodule:: message_data.model.water.data.demands
   :members:


Utilities and CLI
-----------------

.. automodule:: message_data.model.water.utils
   :members:
   :exclude-members: read_config

.. automodule:: message_data.model.water.cli
   :members:


Data, metadata, and config files
================================

See also: :doc:`water/files`.

- :file:`data/water/`: data files from :file:`P:\ene.model\NEST' and other metadata used for defining water technologies.

  - :file:`technology.yaml`: metadata for the 'technology' dimension.
  - :file:`set.yaml`: metadata for other sets.


CLI usage
=========

Use the :doc:`CLI </cli>` command ``mix-data water`` to invoke the commands defined in :mod:`.water.cli`. Try:

.. code::

   Usage: mix-data water [OPTIONS] COMMAND [ARGS]...

     MESSAGE-water model.

   Options:
     --help  Show this message and exit.

   Commands:
     build    Prepare the model.
     clone    Clone base scenario to the local database.
     solve    Run the model.
