4.1 	Variables
----

Extraction of domestic resources is modelled by variables that represent the quantity extracted per year in a period. A subdivision into cost categories (which are called ”grades” in the model) and further into elasticity classes can be modelled.



4.1.1 	Resource Extraction  Variables
~~~~~~~~~~~~~~~~~
Rzrgp..t, where
R 	identifies resource extraction variables,
z	is the level on that the resource is defined (usually = R),
r	is the identifier of the resource being extracted,
g	is the grade (also called cost category) of resource r, g ∈ {a, b, c, ...}.
p	is the class of supply elasticity, which is defined for the resource and grade, or
”.”, if no elasticity is defined for this resource and grade, and
t	identifies the period.


The resource variables are energy flow variables and represent the annual rate of extraction
of resource r. If several grades are defined, one variable per grade is generated (identifier g in position 4). Supply elasticities can be defined for resource extraction as described  in section
10.11; in this case one variable per elasticity class (identifier p in position 5) is generated.



4.2 	Constraints


The overall availability of a resource is limited in the availability constraint per grade, annual resource consumption can be constrained  per grade (sum of the elasticity classes) and total. Additionally  resource depletion and dynamic resource extraction constraints can be modelled.
 


4.2.1 	Resource Availability per Grade

RRrg.g..


Limits the domestic resource available from one cost category (grade) over the whole time horizon. Total availability of a resource is defined  as the sum over the grades.




∆t × RRrgp..t  ≤ Rrg  − ∆t0Rrg,0 ,
p	t




where
Rrg	is the total amount of resource r, cost category g, that is available for extraction,
RRrgp..t	is the annual extraction of resource r, cost category (grade) g and elasticity class
p in period t,
∆t 	is the length of period t.
∆t0	is the number of years between the base year and the first model year, and
Rrg,0	is the extraction of resource r, grade g in the base year.



4.2.2 	Maximum Annual Resource Extraction

RRr....t


Limits the domestic resources available annually per period over all cost categories.


RRrgp..t  ≤ Rrt ,
g	p





where
Rrgt	is the maximum amount of resource r, grade g, that can be extracted per year of period t, and
RRrgp..t	is the annual extraction of resource r, cost category (grade) g and elasticity class
p in period t.



4.2.3 	Resource Depletion  Constraints

RRrg.d.t
 


The extraction of a resource in a period can be constrained  in relation to the total amount still existing in that period. For reasons of computerization these constraints can also be generated for imports and exports, although they do not have any relevance there (they could, e.g., be used for specific scenarios in order to stabilize the solution).

 

∆t 		RRrgp..t  ≤ δt p
 

Rrg  − ∆t0Rrg,0   −
 
t−1

τ =1
 
l
∆τ ×  RRrgp..τ	,
 





where
Rrg	is the total amount of resource r, cost category g, that is available for extraction,
RRrgp..t	is the annual extraction of resource r, cost category (grade) g and elasticity class
p in period t,
rg	is the maximum fraction of resource r, cost category g, that can be extracted in period t,
Rrg	is the total amount available in the base year,
∆t 	is the length of period t in years,
∆t0	is the number of years between the base year and the first model year, and
Rrg,0	is the extraction of resource r, grade g in the base year.



4.2.4 	Maximum Annual Resource Extraction  per Grade

RRrg.a.t


Limits the domestic resources available from one cost category per year.


RRrgp..t  ≤ Rrgt .
p





where
Rrg	is the total amount of resource r, cost category g, that is available for extraction, and
RRrgp..t	is the annual extraction of resource r, cost category (grade) g and elasticity class
p in period t.



4.2.5 	Upper Dynamic Resource Extraction  Constraints


M RRr...t
 


The annual extraction level of a resource in a period can be related to the previous one by a growth parameter and an increment of extraction capacity resulting in upper dynamic extraction constraints. For the first period the extraction is related to the activity in the baseyear.
 


g,p
 
RRrgp..t  − γo
 


g,p
 
RRrgp..(t − 1) ≤ go ,
 




where
rt 	is the maximum growth of extraction of resource r between period t − 1 and t,
rt 	is the initial  size (increment) of extraction of resource r in period t, and
RRrgp..t    is the annual extraction of resource r, cost category (grade) g and elasticity class
p in period t.



4.2.6    Lower Dynamic Resource Extraction  Constraints


LRRr...t


The annual extraction level of a resource in a period can also be related to the previous one by a decrease parameter  and a decrement resulting in lower dynamic extraction constraints. For the first period the extraction is related to the activity in the baseyear.


 


g,p
 
RRrgp..t  − γrt
 


g,p
 
RRrgp..(t − 1) ≥ − grt ,
 





where
γrt 	is the maximum decrease of extraction of resource r between period t − 1 and t,
grt	is the ”last”  size (decrement) of extraction of resource r in period t, and
RRrgp..t	is the annual extraction of resource r, cost category (grade) g and elasticity class
p in period t.



4.2.7 	Dynamic Extraction  Constraints per Grade

M RRrg..t, and
LRRrg..t


The same kind of relations as described  in sections 4.2.5 and 4.2.6 can be defined per grade of the resource.
