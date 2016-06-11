.. _demand:

Demand
========
Baseline energy service demands are provided exogenously to MESSAGE, though they can be adjusted endogenously based on energy prices using the MESSAGE-MACRO link. There are seven energy 
service demands that are provided to MESSAGE, including:

1. Residential/commercial thermal
2. Residential/commercial specific
3. Industrial thermal
4. Industrial specific
5. Industrial feedstock (non-energy)
6. Transportation
7. Non-commercial biomass.

These demands are generated using a so-called scenario generator which is implemented in the script language `R <https://www.r-project.org/>`_. The scenario generator uses country-level 
historical data of GDP per capita (PPP) and final energy use as well as projections of GDP (PPP) and population to extrapolate the seven energy service demands into the future. The 
sources for the historical and projected datasets are the following:

1. Historical GDP (PPP) – World Bank (World Development Indicators 2012)
2. Historical Population – UN World Population Program (World Population Projection 2010)
3. Historical Final Energy – International Energy Agency Energy Balances (IEA 2012)
4. Projected GDP (PPP) – :cite:`2015 dellink_long-term_2015`, see Shared Socio-Economic Pathways database (`SSP scenarios <https://tntcat.iiasa.ac.at/SspDb/>`_)
5. Projected Population – KC and Lutz (2014 :cite:`kc_human_2014^), see Shared Socio-Economic Pathways database(`SSP scenarios <https://tntcat.iiasa.ac.at/SspDb/>`_)

Using the historical datasets, the scenario generator conducts regressions that describe the historical relationship between the independent variable (GDP (PPP) per capita) and several 
dependent variables, including total final energy intensity (MJ/2005USD) and the shares of final energy in several energy sectors (%). In the case of final energy intensity, the 
relationship is best modeled by a power function so both variables are log-transformed.  In the case of most sectoral shares, only the independent variable is log-transformed. The 
exception is the industrial share of final energy, which uses a hump-shaped function inspired by Schäfer (2005) :cite:`schafer_structural_2005`. This portion of the model provides the 
historical relationships between GDP per capita and the dependent variables for each of the eleven MESSAGE regions.

The historical data are also used in `quantile regressions <https://en.wikipedia.org/wiki/Quantile_regression>`_ to develop global trend lines that represent each percentile of the 
cumulative distribution function (CDF) of each regressed variable. Given the regional regressions and global trend lines, final energy intensity and sectoral shares can be extrapolated 
based on projected GDP per capita, or average income. Several user-defined inputs allow the user to tailor the extrapolations to individual socio-economic scenarios. In the case of final 
energy intensity (FEI), the extrapolation is produced for each region by defining the quantile at which FEI converges (e.g., the 20th percentile) and the income at which the convergence 
occurs.  For example, while final energy intensity converges quickly to the lowest quantile (0.001) in SSP1, it converges more slowly to a larger quantile (0.5 to 0.7 depending on the 
region) in SSP3. Convergence quantiles and incomes are provided for each SSP and region in the **Tables below**. The convergence quantile allows one to identify the magnitude 
of FEI while the convergence income establishes the rate at which the quantile is approached. For the sectoral shares, the user can specify the global quantile at which the extrapolation 
should converge, the income at which the extrapolation diverges from the regional regression line and turns parallel to the specified convergence quantile (i.e., how long the sectoral 
share follows the historical trajectory), and the income at which the extrapolation converges to the quantile. Given these input parameters, the user can extrapolate both FEI and sectoral shares.

The total final energy in each region is then calculated by multiplying the extrapolated final energy intensity by the projected GDP (PPP) in each time period. Next, the extrapolated 
shares are multiplied by the total final energy to identify final energy demand for each of the seven energy service demands used in MESSAGE. Finally, final energy is converted to useful 
energy in each region by using the average final-to-useful energy efficiencies reported by the IEA for each country.

**Tables from SI of SSP2 paper to be added**