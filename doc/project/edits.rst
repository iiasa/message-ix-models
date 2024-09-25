.. currentmodule:: message_ix_models.project.edits

EDITS (:mod:`.project.edits`)
*****************************

“Energy Demand changes Induced by Technological and Social innovations (EDITS)”

Project information
===================

- Website: https://iiasa.ac.at/projects/edits
- Acknowledgement:

     [Work] was supported by funding from the Energy Demand changes Induced by Technological and Social innovations (EDITS) project, which is part of the initiative coordinated by the Research Institute of Innovative Technology for the Earth (RITE) and International Institute for Applied Systems Analysis (IIASA), and funded by the Ministry of Economy, Trade, and Industry (METI), Japan.

- The EDITS project funds many activities, only a few of which are related to :mod:`message_ix_models`.
  This code, data, and documentation concern only this subset; at the moment, specifically the **Model Complementarity Exercise** (**MCE**) activity related to transportation and involving the three models:

  - PASTA —operated by the International Transport Forum.
  - MESSAGEix-Transport —operated by IIASA.
  - DNE21+ —operated by RITE.

Usage
=====

The code requires:

1. A file named :file:`pasta.csv`.
   This file contains projected activity data from the PASTA ‘demand’ components.

   To place this file in the right location, run::

     $ mix-models config show

   The output should include lines like::

     Configuration path: /home/username/.local/share/ixmp/config.json

     {
       ...
       "message_local_data": "/home/username/path/to/local/data",
       ...
     }

   …where :file:`path/to/local/data` is some directory on your system containing :ref:`local data <local-data>`.
   If you do not have such a directory, you can create one (wherever you like) and run :program:`mix-models config set message_local_data /home/username/path/to/local/data` to store it in your configuration.

   The file :file:`pasta.csv` goes in a subdirectory named :file:`edits/`.

Then use the command line::

  $ mix-models edits --help
  Usage: mix-models edits [OPTIONS] COMMAND [ARGS]...

    EDITS project.

    https://docs.messageix.org/projects/models/en/latest/project/edits.html

  Options:
    --help  Show this message and exit.

  Commands:
    _debug  Development/debugging code.

Currently :program:`edits _debug` runs :func:`.read_pasta_data`.
See the function documentation.


Code reference
==============

.. automodule:: message_ix_models.project.edits
   :members:
