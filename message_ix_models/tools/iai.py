"""Tools for International Aluminum Institute data.

.. todo::

   - Fetch data from URLS like::

         https://international-aluminium.org/statistics/
         metallurgical-alumina-refining-fuel-consumption/
         ?publication=metallurgical-alumina-refining-fuel-consumption
         &filter=%7B%22row%22%3Anull%2C%22group%22%3Anull%2C%22multiGroup%22%3A%5B2%5D
         %2C%22dateRange%22%3A%22annually%22%2C%22monthFrom%22%3Anull%2C%22monthTo%22
         %3Anull%2C%22quarterFrom%22%3A1%2C%22quarterTo%22%3A4%2C%22yearFrom%22%3A2023
         %2C%22yearTo%22%3A2023%2C%22multiRow%22%3A%5B19%2C20%2C21%2C22%2C23%5D%2C
         %22columns%22%3A%5B49%2C50%2C51%2C52%2C53%2C54%2C55%5D%2C%22activeChartIndex
         %22%3A0%2C%22activeChartType%22%3A%22pie%22%7D

         https://international-aluminium.org/statistics/
         metallurgical-alumina-refining-energy-intensity/
         ?filter=%7B%22row%22%3A85%2C%22group%22%3Anull%2C%22multiGroup%22%3A%5B%5D%2C
         %22dateRange%22%3A%22annually%22%2C%22monthFrom%22%3Anull%2C%22monthTo%22
         %3Anull%2C%22quarterFrom%22%3A1%2C%22quarterTo%22%3A4%2C%22yearFrom%22%3A1985
         %2C%22yearTo%22%3A2023%2C%22multiRow%22%3A%5B%5D%2C%22columns%22%3A%5B%5D%2C
         %22activeChartIndex%22%3A0%2C%22activeChartType%22%3A%22line%22%7D

   - Transform data as required by :mod:`.model.material`.
"""
