What's new
**********

.. Next release
.. ============

2021.3.3
========

- :pull:`8`: Migrate :mod:`.util.click`, :mod:`.util.logging`; expand documentation.
- :meth:`.Context.clone_to_dest` method replaces :func:`clone_to_dest` function.
- Build PDf documentation on ReadTheDocs.
- Allow CLI commands from both :mod:`message_ix_models` and :mod:`message_data` via :program:`mix-models`.
- Migrate :program:`mix-models techs` CLI command.

2021.2.28
=========

- :pull:`5`: Migrate :class:`.Context` class and :mod:`.testing` module from :mod:`message_data`.
- Add :func:`.load_private_data`, :func:`.package_data_path`, :func:`.private_data_path`.
- Document: :doc:`data` and :doc:`cli`.
- :pull:`6`: Update :doc:`node codelists <pkg-data/node>` to ensure they contain both current and former ISO 3166 codes for countries that have changed status.
  For instance, ANT dissolved into BES, CUW, and SXM in 2010; all four are included in R11_LAM so this list can be used to handle data from either before or after 2010.

2021.2.26
=========

- :pull:`2`: Add :func:`.get_codes` and related code lists.
- :pull:`3`: Add :class:`.MessageDataFinder` and document :doc:`migrate`.

2021.2.23
=========

Initial release.
