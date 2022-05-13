Tools for specific data sources
*******************************

IEA World Energy Balances
=========================

.. currentmodule:: message_ix_models.tools.iea_web

.. automodule:: message_ix_models.tools.iea_web
   :members:

   The raw data are in CSV or compressed CSV format.
   They have file names like:

   - :file:`cac5fa90-en.zip` —the complete, extended energy balances, ZIP compressed, containing a single file with a name like :file:`WBIG_2021-2021-1-EN-20211119T100005.csv`.

   - :file:`WBAL_12052022124930839.csv` —a subset or ‘highlights’

   The data have the following structure:

   =========== ======================
   Column name Example value
   =========== ======================
   UNIT [1]_                     KTOE
   Unit                          ktoe
   COUNTRY                        WLD
   Country                      World
   PRODUCT                       COAL
   Product     Coal and coal products
   FLOW                       INDPROD
   Flow                    Production
   TIME                          2012
   Time                          2012
   Value                    1234.5678
   Flag Codes                       M
   Flags       Missing value; data cannot exist
   =========== ======================

   .. [1] the column is sometimes labelled "MEASURE", but the contents appear to be the same.

Code lists
----------
The following files, in :file:`message_ix_models/data/iea/`, contain code lists extracted from the paired columns of the raw data.
The (longer, human-readable) names are not returned by :func:`.load_data`; only the (shorter) code IDs.

These can be used with other package utilities:

.. code-block:: python

   from message_ix_models.util import as_codes, load_package_data

   # a list of sdmx.model.Code objects
   cl = as_codes(load_package_data("iea", "product.yaml"))

   # …etc.


.. literalinclude:: ../../message_ix_models/data/iea/country.yaml
   :language: yaml
   :caption: COUNTRY / node (:file:`country.yaml`)

.. literalinclude:: ../../message_ix_models/data/iea/product.yaml
   :language: yaml
   :caption: PRODUCT / commodity (:file:`product.yaml`)

.. literalinclude:: ../../message_ix_models/data/iea/flag-codes.yaml
   :language: yaml
   :caption: FLAG (:file:`flag-codes.yaml`)

.. literalinclude:: ../../message_ix_models/data/iea/flow.yaml
   :language: yaml
   :caption: FLOW (:file:`flow.yaml`)
