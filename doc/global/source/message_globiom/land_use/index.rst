Land-use
=========

Land-use dynamics are modelled with the GLOBIOM (GLobal BIOsphere Management) model, which is a recursive-dynamic partial-equilibrium model (Havlík et al., 2011; Havlík et al., 2014). GLOBIOM represents the competition between different land-use based activities. It includes a bottom-up representation of the agricultural, forestry and bio-energy sector, which allows for the inclusion of detailed grid-cell information on biophysical constraints and technological costs, as well as a rich set of environmental parameters, incl. comprehensive AFOLU (agriculture, forestry and other land use) GHG emission accounts and irrigation water use. Its spatial equilibrium modelling approach represents bilateral trade based on cost competitiveness. For spatially explicit projections of the change in afforestation, deforestation, forest management, and their related CO2 emissions, GLOBIOM is coupled with the G4M (Global FORest Model) model (Kindermann et al., 2006; Kindermann et al., 2008; Gusti, 2010). The spatially explicit G4M model compares the income of managed forest (difference of wood price and harvesting costs, income by storing carbon in forests) with income by alternative land use on the same place, and decides on afforestation, deforestation or alternative management options. As outputs, G4M provides estimates of forest area change, carbon uptake and release by forests, and supply of biomass for bioenergy and timber.

:numref:`fig-biomass` shows the emulated biomass supply per biomass category as used in the SSP2 RCP2.6 scenario. The biomass categories are based on the type of biomass and the assumed CO2-equivalent price at which they become available.

.. _fig-biomass:
.. figure:: /_static/BiomassSupply.png
   :width: 600px

   Emulated biomass supply per biomass category as used in the SSP2 RCP2.6 scenario.

:numref:`fig-landem` shows a comparison of global land use emissions as emulated by the GLOBIOM emulator and finally with the fully coupled feedback run for the SSP2 reference baseline and the corresponding RCP4.5 and RCP2.6 scenarios.

.. _fig-landem:
.. figure:: /_static/LanduseEmissions.png
   :width: 600px

   Comparison of global land use emissions as emulated by the GLOBIOM emulator (dashed lines) and finally with the fully coupled feedback run (solid lines) for the SSP2 reference baseline (orange) and the corresponding RCP4.5 (yellow) and RCP2.6 (green) scenarios.
