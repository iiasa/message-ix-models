"""Tools for British Geological Survey Data.

.. todo::

   - Fetch data from https://www.bgs.ac.uk/mineralsuk/statistics/
     world-mineral-statistics/world-mineral-statistics-data-download/
     world-mineral-statistics-data/, in particular for commodity = [Alumina, Aluminum]
     and all periods/regions.

     This leads to query URLs like::

        https://www.bgs.ac.uk/wp-json/bgsfeed/v1/minerals/items
          ?year=1970-01-01T00:00:00.000Z/2023-01-01T00:00:00.000Z
          &country_trans=
          &bgs_commodity_trans=alumina%aluminium, primary
          &bgs_statistic_type_trans=Production
          &response=csv

   - Transform data as required by :mod:`.model.material`.
"""
