4 Energy flows
==============

4.1 	Constraints
----------------

Energy flows are modelled solely by linking the activity  variables of the different conversion technologies and the resource extraction, import and export variables in balance constraints. These constraints ensure that only the amounts of energy available are consumed. There are no further variables required to model energy flows.

Energy demands are also modelled  as part of a balance constraint: it is the right hand side and defines the amount to be supplied by the technologies in this constraint.

The following description of the energy flow constraints in MESSAGE is given for the following set of level identifiers:

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`U`
     - Useful energy (demand level),
   * - :math:`F`
     - Final energy (after distribution),
   * - :math:`T`
     - Final energy (after transmission),
   * - :math:`X`
     - Secondary energy,
   * - :math:`A`
     - Primary energy, and
   * - :math:`R`
     - Energy resources.

The identifier of the demand level (:math:`U`) which gives it a special meaning (see section :ref:`activitiesECT`) and imports and exports are given for primary energy. Clearly any other combination of technologies is also possible.

4.1.1 	Demand Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Ud.....t

Out of the predefined  levels each one can be chosen as demand  level. However, level ”:math:`U`” has a special feature. This is related to the fact that useful energy is usually produced on-site, e.g., space heat is produced by a central heating system, and the load variations over the year are all covered by this one system. Thus, an allocation of production technologies to the different areas of the load curve, like the model would set it up according to the relation between investment and operating costs would ignore the fact that these systems are not located in the same place and are not connected to each other. MESSAGE represents the
end-use technologies by one variable per period that produces the required useful energy in the load pattern needed and requires the inputs in the same pattern. For special technologies like, e.g., night storage heating systems, this pattern can be changed to represent the internal storage capability of the system.

This representation of end-use technologies has the advantage of reducing the size of the model, because the demand constraints, the activity  variables and the capacity constraints of the end-use technologies do not have to be generated for each load region.

If another level is chosen as demand  level or the demand level is not named ”:math:`U`”, all demand constraints for energy carriers that are modelled with load regions are generated for each load region. The general form of the demand constraints is

.. math::
   \sum_{svd}\epsilon_{svd}\times \sum_{e=0}^{e_d}k_e\times Usvd.e.t + \sum_{sv\delta} \beta_{sv\delta}^d \times \sum_{e=0}^{e_\delta }k_e \times Usv \delta ue.t \geq Ud.t ,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`U d.t`
     - is the annual demand for :math:`d` in period :math:`t`,
   * - :math:`U svd.e.t`
     - is the activity of end-use technology :math:`v` in period :math:`t`, elasticity class :math:`e` and period :math:`t` (see section  :ref:`activitiesECT`),
   * - :math:`\epsilon _{svd}`
     - is the efficiency of end-use technology :math:`v` in converting :math:`s` to :math:`d`,
   * - :math:`\beta _{sv\delta}^d`
     - is the efficiency of end-use technology :math:`v` in producing by-product :math:`d` from :math:`s` (:math:`\delta` is the main output of the technology),
   * - :math:`e_d`
     - is the number of steps of demand reduction modelled for own-price elasticities of demand :math:`d`, and
   * - :math:`ke`
     - is the factor giving the relation of total demand for :math:`d` to the demand reduced to level :math:`e` due to the demand elasticity. :math:`(k_e  \times U svd.e.t = U svd.0.t, k_0  = 1, k_e` is increasing monotonously.)

.. _distbal:

4.1.2 	Distribution Balance
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Fs....lt

This constraint, the final energy balance, matches the use of final energy needed in the
end-use technologies and the deliveries of the distribution systems. It is generated for each load region, if energy form :math:`s` is modelled with load regions.

.. math::
   \sum_{svs}\epsilon _{svs}\times Fsvs..lt-\sum_{svd}\eta _{d,l}\times \sum_{e=0}^{e_d}Usvd.e.t-\\ \sum_{\sigma vd}\beta _{\sigma vd}^s \times \eta _{d,l}\times \sum_{e=0}^{e_d}U\sigma vd.e.t\geq 0

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`F svs..lt`
     - is the activity of the distribution technology in load region :math:`l` and period :math:`t` (see section :ref:`activitiesECT`),
   * - :math:`\epsilon _{svs}`
     - is the efficiency of technology :math:`v` in distributing :math:`s`,
   * - :math:`U svd.e.t`
     - is the activity of end-use technology :math:`v` in period :math:`t` and elasticity class :math:`e`,
   * - :math:`\beta _{\sigma vd}^s`
     - is the use of fuel :math:`s` relative to fuel :math:`σ` (the main input) by technology :math:`v`, and
   * - :math:`\eta _{d,l}`
     - is the fraction of demand for :math:`d` occurring in load region :math:`l`.

.. _transmibal:

4.1.3 	Transmission or Transportation Balance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Ts....lt`

This constraint gives the simplest form of an energy balance equation of MESSAGE. It matches the output of transmission to the requirements of distribution systems. The difference to other levels (:math:`F`, :math:`X`, :math:`A`) is not built-in,  but emerges from the simplicity of energy transportation (i.e., transportation technologies do usually not have by-products and only one input).  Also big industrial consumers that are directly connected to the transmission system would have to be included in this constraint. Like level :math:`F` it does usually exist for all load regions if they are defined for the fuel.

.. math::
   \sum_{svs}\epsilon _{svs}\times Tsvs..lt-\sum_{svs}Fsvs..lt\geq 0 .

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`T svs..lt`
     - is the activity of the transportation technology :math:`v` (see section  :ref:`activitiesECT`), and

all the other entries to the equation are the same as in section :ref:`distbal`.
 
4.1.4 	Central  Conversion Balance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Xs....lt

In principle the secondary energy balance is built up in the same way as the two previous ones (sections :ref:`distbal` and :ref:`transmibal`). It matches the production of central conversion technologies to the requirements of the transmission  systems. Secondary energy imports and exports of secondary energy are usually assigned to level :math:`X`.

.. math::
   \sum_{rvs}\epsilon _{rvs}\times Xrvs..lt + \sum_{rv\sigma }\beta _{rv\sigma}^s \times Xrv\sigma ..lt - \sum_{svs}Tsvs..lt +\\ \sum _{c,p}IXscp.lt - \sum _{c,p}EXscp.lt \geq 0

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`X rvs..lt`
     - is the activity of central conversion technology :math:`v` in load region :math:`l` and period :math:`t` (see section :ref:`activitiesECT`); if the secondary energy form :math:`s` is not defined with load regions (i.e. :math:`l` = ”.”) and the activity of technology :math:`v` exists for each load region, this equation will contain the sum of the activity variables of technology :math:`v` over the load regions.
   * - :math:`\epsilon _{rvs}`
     - is the efficiency of technology :math:`v` in converting energy carrier :math:`r` into secondary energy form :math:`s`,
   * - :math:`\beta _{rv\sigma}^s`
     - is the efficiency of technology :math:`v` in converting energy carrier :math:`r` into the by-product :math:`s` of technology :math:`v`,
   * - :math:`Tsvs..lt`
     - is explained in section :ref:`transmibal`, and
   * - :math:`IXscp.lt`
     - and :math:`EXscp.lt` are import and export variables.

.. _resourceextraction:

4.1.5 	Resource Extraction,  Export  and Import  Balance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Ar.....t

This equation matches production and import of primary energy to the requirements of central conversion, transport and for export. In the general case primary energy does not have load regions. Some technologies, like, e.g., nuclear reactors need inventories of primary energy and also leave a last core that is available at the end of the lifetime. It may be necessary to model by-products of extraction technologies, for instance the availability of associated  gas at oil production sites.

.. math:: 
   \sum _{rvr}\epsilon _rvr \times Arvr..t - \sum _l \left [ \sum _{rvs} Xrvs..lt + \sum _{\rho vs} \beta _{\rho vs}^r \times Xpvs..lt\right ] + \sum _{c,p}IArcp..t -\\ \sum_{c,p}EArcp..t + \sum_{fvs} \left [ \frac{\Delta (t-{\pi _{fvs}})}{\Delta t} \times \rho (fvs,r) \times YXfvs..(t-\tau _{fvs}) - \\ \frac{\Delta (t+1)}{\Delta t}\times \iota (fvs,r) \times YXfvs..(t+1)) \right] \geq 0

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Arvr...t`
     - is the activity of technology :math:`v` extracting resource :math:`r`,
   * - :math:`\epsilon _rvr`
     - is the efficiency of technology :math:`v` in extracting fuel :math:`r` (this is usually 1.),
   * - :math:`\beta _{\rho vs}^r`
     - is the efficiency of technology :math:`v` in producing secondary energy form :math:`s` from the by-input :math:`\rho`,
   * - :math:`IArcp..t`
     - and :math:`EArcp..t` are the import and export variables,
   * - :math:`\tau _{fvs}`
     - is the plant life of technology :math:`v` in periods (depending on the lengths of the periods covered),
   * - :math:`YXfvs..t`
     - is the annual new installation of technology :math:`v` in period :math:`t` (see section :ref:`capacititesECT`),
   * - :math:`\iota (fvs,r)`
     - is the amount of fuel :math:`r` that is needed when technology :math:`v` goes into operation (usually this is the first core of a reactor). It has to be available in the period before technology :math:`v` goes into operation, the normal unit is kWyr/kW,
   * - :math:`\rho (fvs,r)`
     - is the amount of fuel :math:`r` that becomes available after technology :math:`v` goes out of operation (for a reactor this is the last core that goes to reprocessing). The unit is the same as for :math:`\iota (fvs,r)`, and
   * - :math:`\Delta t`
     - is the length of period :math:`t` in years.


4.1.6 	Resource Consumption
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Rr.....t

The resources produced by the extraction technologies in a period can come from different cost categories (also called grades), which can, e.g., represent the different effort to reach certain resources. Short-term variations in price due to steeply increasing demand can be represented by an elasticity approach (see section 9.11).

.. math::
   \sum_{g,p}RRrgp..t - \sum_{rvr}Arvr..t \geq 0,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`RRrgp..t`
     - is the annual extraction of resource :math:`r`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`, and
   * - :math:`Arvr...t`
     - is the activity of extraction technology :math:`v` in period :math:`t` (as described in section :ref:`activitiesECT`).

 
