What's new
**********

Next release
============


SSP_dev updates
----------------
Model development

Non-model changes:

- the --datafile option for the the material-ix build command was removed since it is not used anymore
- a material-ix calibrate command has been added that can be used to run the MACRO calibration step in isolation
- values from model parameter data that have a year_act and year_vtg columns greater than the technology lifetime are dropped since they are inflating the model size unnecessarily
- a module for commodity share constraints was created, but is not fully used in the build yet (TODO: think about adding that for the SSPs)

Demand

- the demand commodity level of methanol has been changed from "final_material" to "demand"
- an additional demand projection setting for aluminum, steel and cement has been introduced called "highest" which SSP5 scenarios are mapped to
- the 2025 demands are fixed to the projected SSP2 values for each SSP
- Aluminum base year demand has been updated based on IAI MFA output for 2020
- Cement base year demand has been updated based on GlobBulk Consulting data for 2020
- Steel base year demand has been updated based on worldsteel association data for 2020


Post processing

- the new industry reporter has been implemented in model/material/report, with mapping files in data/material/reporting

Model

General

- the power sector module has been deactivated for the SSP builds
- coal_i technology is share constrained in each region based on 2020 IEA statistics
- the low temperature share constraint for other industry is updated to reflect explicit modelling of heavy industry

TODO: compile low temperature literature and set up references

- the .tools.cost module has been updated to run in "gdp" mode when called by the materials build
- the .tools.cost module is called twice if --update_costs option is True in material-ix build command to be able to get the correct cost projections for the non-MESSAGEix-Materials industry technologies (e.g coal_i, sp_el_I etc.)
- the following utility functions were added/updated in utils.py:

    * for mapping country names with ISO 3166-1 alpha-3 codes with pycountry (TODO: consolidate with other pycountry wrappers in this repo)
    * adding a R12 column with region values mapped based on a iso column of the same dataframe
    * the MACRO calibration excel input file updater is now also updating the "demand_ref" tab based on the given scenario "demand" parameter values
    * the MACRO calibration excel input file updater can be run in "extrapolation" mode or just take values from the scenario directly


Aluminum

- the aluminum build reads SSP differentiated input data by reading from data/materials/aluminum/<SSPX>/aluminum_techno_economic.xlsx
- the historical capacity of smelters is calibrated until 2020 using the genisim dataset
- the historical activity of smelters is calibrated until 2020 using British Geological Survey data (Soderberg activity is calibrated assuming capacity shares computed from genisim dataset)
- the historical activity of smelters is calibrated until 2020 using British Geological Survey data
- the historical activity of alumina refining is calibrated until 2020 using British Geological Survey data
- a trade model for alumina is introduced to be able to calibrate refining and smelting activity
- the process heat fuel consumption is calibrated using IAI data
- the regional alumina refining process heat intensities are calibrated using IAI data

Ammonia

- no changes except for 2025 demand SSP2 fixing as mentioned under General changes
- the emission factor of biomass_NH3_ccs was fixed (had a unit issue)

Cement

- the cement build reads SSP differentiated input data by reading from data/materials/cement/<SSPX>/Global_cement_MESSAGE.xlsx
- selected regional CEMENT values of the residual_industry_2019.csv were updated since they seemed wrong when comparing the data to the IEA cement report (TODO: paste link to report here)
- the heat input for clinker CCS addons was lowered a lot (TODO: insert US NREL publiation)

Other industry

- the demands for the other industry are generated based on IEA historical data of the non-explicitly sectors (the new model still sits in a private repository)
- the sector furnaces were missing non CO2 emission factors of their original MESSAGE counterpart (e.g coal_i). Thus, they were copied from the originals and scaled with the input coefficients.
- the furnaces are now writing into the IndThermDemLink relation, that is required for the MESSAGE-GAINS linkage.

Methanol

- the model structure was slightly updated to simplify and correct the carbon emission balance accounting:
- the negative emission coefficients that represent the carbon stored in long lived products, were moved from meth_t_d technology to a new technology meth_ind_fs
- the carbon balance of MTO_petro was not correct. The process emisssions and the input/output parameters were updated based on new literature (TODO: compile literature and write a paragraph about it)

Petrochemicals

- with the changes in methanol the carbon acccounting was improved:
- the negative emission coefficients that represent the carbon stored in long lived products, were moved from steam_cracker_petro technology to the production_HVC technology
- since carbon capture in plastics was moved upstream, ethanol_to_ethylene_petro needed a positive emission factor to represent the combusted chemicals part produced with ethylene feedstock from ethanol

Steel

- the steel build reads SSP differentiated input data by reading from data/materials/steel/<SSPX>/Global_steel_MESSAGE.xlsx
- the maximum recycling relation was updated
- the minimum recycling relation was changed for SSP1
- the cokeoven got its own bottom up relation coefficient, to separate that from industrial emissions and move it to transformation emissions
- the cost changes done in the last PR (insert steel hydrogen PR link) were also transferred to the tools.cost module and custom reduction rates were assigned

subheading2
-----------



subheading3
-----------

