A comparison of the new GAMS-Message implementation and the current model
==========================

The following notation is used:

- **already implemented** in the new GAMS-Message model
- *on the to-do list* to be implemented in the new GAMS-Message model 
- :strike:'still imported into DB' for backwards compatibility (writing LDB files), but not to be used further
- :underline:'new parameters' derived from other input parameters or added manually

Parameters
----

This is a complete list of parameters imported into the database.

======================================= =============== =========================================================== =================
New name in DB                          section         index keys                                                  old Message name     
======================================= =============== =========================================================== =================
energy_stocks                           energyforms:    node|commodity|<value>                                      none
**demand**                              demand:         node|commodity|level|year|time                              none
:strike:'main_output'                   systems:        node|technology|mode|node|commodity|level|year|time|time    moutp
:strike:'main_input'                    systems:        node|technology|mode|node|commodity|level|<value>           minp
**output**                              systems:        node|technology|mode|node|commodity|level|year|time|time    moutp, outp
**input**                               systems:        node|technology|mode|node|commodity|level|year|time|time    minp, inp
**lifetime**                            systems:        node|technology|year                                        pll
**availability**                        systems:        node|technology|year|time                                   plf
**inv_cost**                            systems:        node|technology|year                                        inv
**fix_cost**                            systems:        node|technology|year                                        fom
**var_cost**                            systems:        node|technology|mode|year|time                              vom
bounds_activity                         systems:        node|technology|mode|\*|year|time                           bda
bounds_new_capacity                     systems:        node|technology|\*|year|time                                bdc
market_penetration_activity             systems:        node|technology|mode|\*|\*|year|time                        mpa
Market_penetration_new_capacity         systems:        node|technology|\*|\*|year|time                             mpc
*construction_time*                     systems:        node|technology|year                                        ctime
initial_cores                           systems:        node|technology|\*|year|time                                corin
final_cores                             systems:        node|technology|\*|year|time                                corout
power_relation                          systems:        node|technology|mode|year|time                              prel
con1a                                   systems:        node|technology|mode|\*|year                                con1a
con2a                                   systems:        node|technology|mode|\*|year                                con2a
conca                                   systems:        node|technology|\*|year|time                                conca
con1c                                   systems:        node|technology|\*|year|time                                con1c
cost_pseudotec                          variables:      node|pseudotechnology|year|time                             cost
upper_bound                             variables:      node|pseudotechnology|year|time                             upper
lower_bound                             variables:      node|pseudotechnology|year|time                             lower
con1a_pseudotec                         variables:      node|pseudotechnology|\*|year                               con1a
con2a_pseudotec                         variables:      node|pseudotechnology|\*|year                               con2a
righthandside                           relations:      node|\*|\*|year|time                                        rhs
range                                   relations:      node|\*|year|time                                           rng
availability                            relations:      node|\*|year|time                                           plf
cost                                    relations:      node|\*|year|time                                           cost
**resource_cost**                       resources:      node|commodity|grade|year                                   cost
**resource_remaining**                  resources:      node|commodity|grade|year                                   resrem
**resource_volume**                     resources:      node|commodity|grade|<value>                                volume
:strike:'resource_baseyear_extraction'  resources:      node|commodity|grade|<value>                                byrex
:underline:'emissionfactor'             null            node|technology|mode|emission|year                          none
:underline:'duration_period'            null            year                                                        none
:underline:'duration_time'              null            time                                                        none
======================================= =============== =========================================================== =================



Decision variables
----


Constraints
----

