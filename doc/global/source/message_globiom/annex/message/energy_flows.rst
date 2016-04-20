6.1 	Constraints


Energy flows are modelled solely by linking the activity  variables of the different conversion technologies and the resource extraction, import and export variables in balance constraints. These constraints ensure that only the amounts of energy available are consumed. There are no further variables required to model energy flows.

Energy demands are also modelled  as part of a balance constraint: it is the right hand side and defines the amount to be supplied by the technologies in this constraint.

The following description of the energy flow constraints in MESSAGE is given for the following set of level identifiers:


U  Useful energy (demand level),

F  Final energy (after distribution), T  Final energy (after transmission), X  Secondary energy,
A  Primary energy, and

R  Energy resources.


The identifier of the demand level (U ) which gives it a special meaning (see section 2.1.1) and imports and exports are given for primary energy. Clearly any other combination of technologies is also possible.
 


6.1.1 	Demand Constraints


U d.....t


Out of the predefined  levels each one can be chosen as demand  level. However, level ”U ” has a special feature. This is related to the fact that useful energy is usually produced on-site, e.g., space heat is produced by a central heating system, and the load variations over the year are all covered by this one system. Thus, an allocation of production technologies to the different areas of the load curve, like the model would set it up according to the relation between investment and operating costs would ignore the fact that these systems are not located in the same place and are not connected to each other. MESSAGE represents the
end-use technologies by one variable per period that produces the required useful energy in the load pattern needed and requires the inputs in the same pattern. For special technologies like, e.g., night storage heating systems, this pattern can be changed to represent the internal storage capability of the system.

This representation of end-use technologies has the advantage of reducing the size of the model, because the demand constraints, the activity  variables and the capacity constraints of the end-use technologies do not have to be generated for each load region.

If another level is chosen as demand  level or the demand level is not named ”U ”, all demand constraints for energy carriers that are modelled with load regions are generated for each load region. The general form of the demand constraints is

 



svd
 

Esvd ×
 
ed

e=0
 

ke × U svd.e.t +
 



svδ
 

d svδ
 
eδ
×
e=0
 

ke × U svδue.t  ≥ U d.t ,
 





where
U d.t 	is the annual demand for d in period t,
U svd.e.t	is the activity of end-use technology v in period t, elasticity class e and period t
(see section  2.1.1),
Esvd	is the efficiency of end-use technology v in converting s to d,
svδ	is the efficiency of end-use technology v in producing by-product d from s (δ is the main output of the technology),
ed 	is the number of steps of demand reduction modelled for own-price elasticities of demand d, and
ke	is the factor giving the relation of total demand for d to the demand reduced to level e due to the demand elasticity.
(ke  × U svd.e.t = U svd.0.t, k0  = 1, ke is increasing monotonously.)



6.1.2 	Distribution Balance


F s....lt
 


This constraint, the final energy balance, matches the use of final energy needed in the
end-use technologies and the deliveries of the distribution systems. It is generated for each load region, if energy form s is modelled with load regions.

 



svs
 

Esvs   × F svs..lt  −
 



svd
 

ηd,l  ×
 
ed

e=0
 

U svd.e.t −
 

 



σvd
 

σvd  × ηd,l  ×
 
ed

e=0
 

U σvd.e.t ≥ 0 ,
 




where
F svs..lt	is the activity of the distribution technology in load region l and period t (see section 2.1.1),
Esvs	is the efficiency of technology v in distributing s,
U svd.e.t	is the activity of end-use technology v in period t and elasticity class e,
σvd	is the use of fuel s relative to fuel σ (the main input) by technology v, and
ηd,l 	is the fraction of demand for d occurring in load region l.



6.1.3 	Transmission or Transportation Balance


T s....lt


This constraint gives the simplest form of an energy balance equation of MESSAGE. It matches the output of transmission to the requirements of distribution systems. The difference to other levels (F, X, A) is not built-in,  but emerges from the simplicity of energy transportation (i.e., transportation technologies do usually not have by-products and only one input).  Also big industrial consumers that are directly connected to the transmission system would have to be included in this constraint. Like level F it does usually exist for all load regions if they are defined for the fuel.


 


svs
 
Esvs   × T svs..lt  −
 


svs
 
F svs..lt  ≥ 0 .
 





where
T svs..lt	is the activity of the transportation technology v (see section  2.1.1), and all the other entries to the equation are the same as in section 6.1.2.
 


6.1.4 	Central  Conversion Balance

X s....lt


In principle the secondary energy balance is built up in the same way as the two previous ones (sections 6.1.2 and 6.1.3). It matches the production of central conversion technologies to the requirements of the transmission  systems. Secondary energy imports and exports of secondary energy are usually assigned to level X .



 


rvs
 
Ervs   × X rvs..lt  +
 


rvσ
 
s rvσ
 
× X rvσ..lt  −
 


svs
 
T svs..lt +
 


 


c,p
 
I X scp.lt  −
 


c,p
 
EX scp.lt  ≥ 0
 




where
X rvs..lt      is the activity of central conversion technology v in load region l and period t (see section 2.1.1); if the secondary energy form s is not defined with load regions (i.e. l = ”.”) and the activity of technology v exists for each load region, this equation will contain the sum of the activity variables of technology v over the load regions.
Ervs	is the efficiency of technology v in converting energy carrier r into secondary energy form s,
rvσ	is the efficiency of technology v in converting energy carrier r into the by-product
s of technology v,
T svs..lt	is explained in section 6.1.3, and
I X scp.lt	and EX scp.lt are the import and export variables explained in sections 5.1.1 and
5.1.2, respectively.



6.1.5 	Resource Extraction,  Export  and Import  Balance


Ar.....t


This equation matches production and import of primary energy to the requirements of central conversion, transport and for export. In the general  case primary energy does not have load regions. Some technologies,  like, e.g., nuclear reactors need inventories of primary energy and also leave a last core that is available at the end of the lifetime. It may be necessary to model by-products of extraction technologies, for instance the availability of associated  gas at oil production sites.

 



rvr
 

Ervr   × Arvr...t −
 



l	rvs
 

X rvs..lt  +
 



ρvs
 

r ρvs
 
l
× X ρvs..lt	+
 



c,p
 

I Arcp..t −
 



 


c,p
 

EArcp..t  +
 


f vs
 
\ ∆(t − τf vs)
∆t
 
× ρ(f vs, r) × Y X f vs..(t − τf vs) −
 

 
∆(t + 1)	l
∆t 	× ι(f vs, r) × Y X f vs..(t + 1)
 
≥ 0 ,
 




where
Arvr...t 	is the activity of technology v extracting resource r,
Ervr	is the efficiency of technology v in extracting fuel r (this is usually 1.),
ρvs	is the efficiency of technology v in producing secondary energy form s from the by-input ρ,
I Arcp..t 	and EArcp..t are the import and export variables described in section 5.1.1 and
5.1.2, respectively,
τf vs	is the plant life of technology v in periods (depending on the lengths of the periods covered),
Y X f vs..t 	is the annual new installation of technology v in period t (see section  2.1.2),
ι(f vs, r) 	is the amount of fuel r that is needed when technology v goes into operation (usually this is the first core of a reactor). It has to be available in the period before technology v goes into operation, the normal unit is kWyr/kW,
ρ(f vs, r) 	is the amount of fuel r that becomes available after technology v goes out of operation (for a reactor this is the last core that goes to reprocessing). The unit is the same as for ι(f vs, r), and
∆t 	is the length of period t in years.



6.1.6 	Resource Consumption

Rr.....t


The resources produced by the extraction technologies in a period can come from different cost categories (also called grades), which can, e.g., represent the different effort to reach certain resources. Short-term variations in price due to steeply increasing demand can be represented by an elasticity approach (see section 10.11).

 


g,p
 
RRrgp..t  −
 


rvr
 
Arvr...t ≥ 0 ,
 




where
RRrgp..t	is the annual extraction of resource r, cost category (grade) g and elasticity class
p in period t, and
Arvr...t 	is the activity of extraction technology v in period t (as described in section
2.1.1).
 





