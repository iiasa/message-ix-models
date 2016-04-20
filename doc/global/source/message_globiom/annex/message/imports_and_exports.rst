5.1 	Variables
----

Imports and exports are modelled by variables that represent the quantity imported per year in a period. A subdivision into countries and further into elasticity classes can be modelled.



5.1.1 	Import  Variables
~~~~~~~~~~~~~~~~~~~~~
I zscp.lt, where
I	identifies import variables,
z	is the level on that the imported energy form is defined (usually primary energy and secondary energy),
s	identifies the imported energy carrier,
c	is the identifier of the country or region the imports come from,
p	is the class of supply elasticity, which is defined for the energy carrier and country, or ”.”, if no elasticity is defined for this energy carrier and country,
l	is the load region identifier if s is modelled with load regions, otherwise ”.”, and
t	identifies the period.


The import variables are energy flow variables and represent the annual import of the identified energy carrier from the country or region given. If supply elasticities are defined for the import of this energy carrier and country one variable per elasticity class (identifier p in position 5) is generated.



5.1.2 	Export  Variables
~~~~~~~~~~~~~~~~~~~~~

Ezrcp.lt,
 




where
E 	is the identifier for export variables, and
z	is the level on that the exported energy form is defined (usually primary energy and secondary energy),
s	identifies the exported energy carrier,
c	is the identifier of the country or region the exports go to,
p	is the class of supply elasticity, which is defined for the energy carrier and country, or ”.”, if no elasticity is defined for this energy carrier and country,
l	is the load region identifier if s is modelled with load regions, otherwise ”.”, and
t	identifies the period.


The export variables are energy flow variables and represent the annual export of the identified energy carrier to the country or region given. If supply elasticities are defined for the export of this energy carrier and country one variable per elasticity class (identifier p in position 5) is generated.



5.2 	Constraints


5.2.1 	Imports per Country

I zrc.g..


Limits the imports of a fuel from a specific country c over the whole horizon.
∆t × I zrcp..t  ≤ I rc ,
p	t



where
I rc	is the total import limit  for r from country c,
I zrcp..t	is the annual import of r from country c, elasticity class p in period t, and
∆t 	is the length of period t in years.


5.2.2 	Maximum Annual Imports

I zr....t


Limits the annual imports of a fuel from all countries per period.

I zrcp..t  ≤ I rt ,
c	p



where
I rt 	is the annual import limit  for r in period t, and
I zrcp..t	is the annual import of r from country c, elasticity class p in period t.
 


5.2.3 	Maximum Annual Imports per Country

I zrc.a.t


Limits the imports from one country per year.

I zrcp..t  ≤ I rct ,
p




where
I rct 	is the limit  on the annual imports from country c, period t of fuel r, and
I zrcp..t	is the annual import of r from country c, elasticity class p in period t.


5.2.4 	Upper Dynamic Import  Constraints

M I zr...t


The annual import level of a fuel in a period can, like the resource extraction, be related to the previous one by a growth parameter and an increment resulting in upper dynamic constraints.
 


c,p
 
I zrcp..t  − γo
 


c,p
 
I zrcp..(t − 1) ≤ go ,
 




where
I zrcp..t	is the annual import of r from country c, elasticity class p in period t,
rt 	is the maximum increase of import of r between period t − 1 and t, and
rt 	is the initial  size (increment) of import of r in period t.


5.2.5 	Lower Dynamic Import  Constraints

LI zr...t


The annual import level of a fuel in a period can also be related to the previous one by a decrease parameter  and a decrement resulting in lower dynamic import constraints.

 


c,p
 
I zrcp..t  − γrt
 


c,p
 
I zrcp..(t − 1) ≥ − grt ,
 




where
I zrcp..t	is the annual import of r from country c, elasticity class p in period t,
γrt 	is the maximum decrease of import of r between period t − 1 and t, and
grt	is the ”last”  size (decrement) of import of r in period t.
 


5.2.6 	Dynamic Import  Constraints per Country

M I zrc..t and


LI zrc..t


The same kind of relations can be defined per country from that the fuel is imported.



5.2.7 	Constraints on Exports


The exports of fuels can principally be limited in the same way as the imports. In the identifiers of the variables and constraints the ”I ” is substituted by an ”E”.
 

