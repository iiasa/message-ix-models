
A comparison of the new GAMS-Message implementation and the current model
==========================

The following notation is used:

- **already implemented** in the new GAMS-Message model
- *on the to-do list* to be implemented in the new GAMS-Message model 
- :strike:`still imported into DB' for backwards compatibility (writing LDB files), but not to be used further`
- :underline:`new parameters' derived from other input parameters or added manually`

Parameters
----

This is a complete list of parameters imported into the database.

======================================= =============== =================================================================== =================
New name in DB                          section         index keys                                                          old Message name     
======================================= =============== =================================================================== =================
energy_stocks                           energyforms:    node|commodity|<value>                                              none
**demand**                              demand:         node|commodity|level|year|time                                      none
:strike:`main_output`                   systems:        node|technology|mode|node(O/D)|commodity|level|year|time|time(O/D)  moutp
:strike:`main_input`                    systems:        node|technology|mode|node(O/D)|commodity|level|<value>              minp
**output**                              systems:        node|technology|mode|node(O/D)|commodity|level|year|time|time(O/D)  moutp, outp
**input**                               systems:        node|technology|mode|node(O/D)|commodity|level|year|time|time(O/D)  minp, inp
**lifetime**                            systems:        node|technology|year                                                pll
**availability**                        systems:        node|technology|year|time                                           plf
**inv_cost**                            systems:        node|technology|year                                                inv
**fix_cost** [#costbyvintage]_          systems:        node|technology|year                                                fom
**var_cost** [#costbyvintage]_          systems:        node|technology|mode|year|time                                      vom
bounds_activity                         systems:        node|technology|mode|\*|year|time                                   bda
bounds_new_capacity                     systems:        node|technology|\*|year|time                                        bdc
market_penetration_activity             systems:        node|technology|mode|\*|\*|year|time                                mpa
market_penetration_new_capacity         systems:        node|technology|\*|\*|year|time                                     mpc
:underline:'initial_new_capacity        none            node|technology|year
:underline:'spillover_new_capacity      none            node|technology|year|node|technology|year
*constructiontime* [#construction]_     systems:        node|technology|year                                                ctime
:strike:'initial_cores'                 systems:        node|technology|\*|year|time                                        corin
:strike:'final_cores'                   systems:        node|technology|\*|year|time                                        corout
power_relation                          systems:        node|technology|mode|year|time                                      prel
con1a                                   systems:        node|technology|mode|\*|year                                        con1a
con2a                                   systems:        node|technology|mode|\*|year                                        con2a
conca                                   systems:        node|technology|\*|year|time                                        conca
con1c                                   systems:        node|technology|\*|year|time                                        con1c
cost_pseudotec                          variables:      node|pseudotechnology|year|time                                     cost
upper_bound                             variables:      node|pseudotechnology|year|time                                     upper
lower_bound                             variables:      node|pseudotechnology|year|time                                     lower
con1a_pseudotec                         variables:      node|pseudotechnology|\*|year                                       con1a
con2a_pseudotec                         variables:      node|pseudotechnology|\*|year                                       con2a
righthandside                           relations:      node|\*|\*|year|time                                                rhs
range                                   relations:      node|\*|year|time                                                   rng
availability                            relations:      node|\*|year|time                                                   plf
cost                                    relations:      node|\*|year|time                                                   cost
**resource_cost**                       resources:      node|commodity|grade|year                                           cost
**resource_remaining**                  resources:      node|commodity|grade|year                                           resrem
**resource_volume**                     resources:      node|commodity|grade|<value>                                        volume
:strike:'resource_baseyear_extraction'  resources:      node|commodity|grade|<value>                                        byrex
:underline:'emissionfactor'             none            node|technology|mode|emission|year                                  
:underline:'duration_period'            none            year                                                                
:underline:'duration_time'              none            time                                                                
======================================= =============== =================================================================== =================

.. [#costbyvintage] Do we want to include the possibility that technology fixed and variable costs change over time? 
.. [#construction] How explicit do we want to formulate the construction time in the capacity constraint?


Decision variables
----


Constraints in current Message version
----


Variables and constraints/equations in previous GAMS-Message version with technological learning 
----

These are the variables 

================================== =================================================== ============================================================
Variable name                      index keys                                          explanation
================================== =================================================== ============================================================
**CAP**                            period, node, technology, year                      technology capacities (by vintage)
CAP_TOTAL                          period, node, technology                            total technology capacity (summed over vintages)
**ACT**                            period, node, technology, mode, year                annual activities for regional technologies (by vintage)
ACT_SUBANNUAL                      period, node, technology, mode, year, season, time  subannual activities for regional technologies (by vintage)
ACT_TOTAL                          period, node, technology, mode                      total annual activities for regional technologies
*STOCK*                            period, node, commodity, level                      remaining stock of resources at beginning of period
COMMODITY_DEMAND                   period, node, commodity, level                      commodity demand on a certain level
COMMODITY_SUPPLY                   period, node, commodity, level                      commodity supply on a certain level
COMMODITY_SUBANNUAL_DEMAND         period, node, commodity, level, season, time        subannual commodity demand on a certain level
COMMODITY_SUBANNUAL_SUPPLY         period, node, commodity, level, season, time        subannual commodity supply on a certain level
TOTAL_COMMODITY_CONSUMPTION        period, commodity, level                            total commodity consumption
TOTAL_COMMODITY_PRODUCTION         period, commodity, level                            total commodity production
REGIONAL_COMMODITY_CONSUMPTION     period, node, commodity, level                      commodity consumption by region
REGIONAL_COMMODITY_PRODUCTION      period, node, commodity, level                      commodity production by region
SECTORAL_COMMODITY_CONSUMPTION     period, node, sector, commodity, level              commodity consumption by region/sector
SECTORAL_COMMODITY_PRODUCTION      period, node, sector, commodity, level              commodity production by region/sector
*TOTAL_EMISSION*                   period, emission                                    total emission output
REGIONAL_EMISSION                  period, node, emission                              emission output by region 
SECTORAL_EMISSION                  period, node, sector, emission                      emission output by region/sector 
**TOTAL_COST**                     period                                              total system costs by periods
REGIONAL_COST                      period, node                                        costs by region
SECTORAL_COST                      period, node, sector                                costs by region/sector
SECTORAL_COST_MESSAGE              period, node, sector                                costs by region/sector (MESSAGE accounting)
================================== =================================================== ============================================================

These are the constraints

============================================ ===================================================== ============================================================
Constraint name                              index keys                                            explanation
============================================ ===================================================== ============================================================
\* balance equations    
**EQ_COST_TOTAL**                            period                                                objective funtion by period
:strike:'EQ_COST_REGIONAL'                   period, node                                          objective function by period/region
:strike:'EQ_COST_SECTORAL'                   period, node, sector                                  objective function by period/region/sector
:strike:'EQ_COMMODITY_CONSUMPTION_TOTAL'     period, commodity, level                              commodity consumption
:strike:'EQ_COMMODITY_PRODUCTION_TOTAL'      period, commodity, level                              commodity production
:strike:'EQ_COMMODITY_CONSUMPTION_REGIONAL'  period, node, commodity, level                        commodity consumption by region
:strike:'EQ_COMMODITY_PRODUCTION_REGIONAL'   period, node, commodity, level                        commodity production by region
:strike:'EQ_COMMODITY_CONSUMPTION_SECTORAL'  period, node, sector, commodity, level 'commodity     consumption by region/sector
:strike:'EQ_COMMODITY_PRODUCTION_SECTORAL'   period, node, sector, commodity, level  'commodity    production by region/sector
:underline:'COMMODITY_BALANCE'
**EQ_EMISSION_TOTAL**                        period, emission                                      total emission output
EQ_EMISSION_REGIONAL                         period, node, emission                                emission output by region
EQ_EMISSION_SECTORAL                         period, node, sector, emission                        emission output by region/sector
\* technology specific equations
**EQ_MAX_AV**                                period, node, technology, year                        maximum technology availability
:strike:'EQ_MAV_SUBANNUAL_AV'                period, node, technology_load, year, season, time     maximum technology availability by time slice
:strike:'EQ_MAX_MODE_AV'                     period, node, technology, mode, year                  maximum technology availability by operation mode
EQ_ADDON_CAP                                 period, node, technology_addon                        add-on technology capacity constraint
EQ_ADDON_ACT                                 period, node, technology_addon, mode                  add-on technology activity constraint
EQ_COMMODITY_SUPPLY                          period, node, commodity, level                        supply of commodities
EQ_COMMODITY_DEMAND                          period, node, commodity, level                        demand for commodities
EQ_COMMODITY_BALANCE                         period, node, commodity, level                        supply > demand for commodities
EQ_COMMODITY_SUBANNUAL_SUPPLY                period, node, commodity, level, season, time          supply of commodities with subannual demand
EQ_COMMODITY_SUBANNUAL_BALANCE               period, node, commodity, level, season, time          supply > demand for commodities with subannual demand 
EQ_CAPACITY_TOTAL                            period, node, technology                              total regional capacities (summed over vintages
EQ_ACTIVITY_TOTAL                            period, node, technology, mode                        total regional activities (summed over vintages)
EQ_LOAD_ACT_SUM                              period, node, technology_load, mode, year             aggregation of subannual activities to annual activities
\* capacity adequacy 
EQ_CAPACITY_ANNUAL_ADEQUACY                  region, commodity, period                             capacity adequacy with annual time resolution
EQ_CAPACITY_SUBANNUAL_ADEQUACY               region, commodity, period, season, time               capacity adequacy with subannual time resolution
EQ_FLEXIBILITY_ANNUAL_ADEQUACY               region, commodity, period                             flexibility adequacy with annual time resolution
EQ_FLEXIBILITY_SUBANNUAL_ADEQUACY            region, commodity, period, season, time               flexibility adequacy with subannual time resolution
============================================ ===================================================== ============================================================





