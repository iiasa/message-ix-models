8.1 	Constraints


The user-defined relations allow the user to construct constraints that are not included in the basic set of constraints. For each technology  the user can specify coefficients with that either the production variables (see section2.1.1),  the annual new installation variables  (see section
2.1.2) or the total capacity in a year (like it is used in the capacity constraints, see section
2.2.1) can be included in the relation. The relations can be defined with and without load regions, have a lower, upper or fix right hand side or remain free (non-binding) and be related to an entry in the objective function, i.e., all entries to this relation are also entered to the objective function with the appropriate discount factor. There are two types of user-defined constraints, for which the entries to the objective function–without discounting–are summed up under the cost accounting rows C AR1 and C AR2 (see chapter 9).

The formulation of the user-defined relations is given for relations, that are related to the main output of the technologies. It is also possible (e.g., for emissions) to relate the constraint to the main input of the technology, i.e. the amount of fuel used. In this case the efficiencies (E) would be omitted from the formulation.


8.1.1 	Relation without  Load Regions

N m.....t or P m.....t


Relations without load regions just sum up the activities (multiplied with the given coefficients) of all variables defined to be in this constraint. If a technology has load regions, the activity variables for all load regions of this technology are included. If the total capacity of a technology is included, all new capacities from previous periods still operating are included, if new capacities are included, the annual new installation of the current period is taken.

	ed	t	
 romt	mt
 

svd
 
svd  ×
 

e=0
 
U svd.e.t × Esvd   +
 

τ =t−ip
 
rcsvd   × Y U svd..τ  +
 




 


rvs
 
mlt
rvs   ×
 
zrvs..lt  × Ervs   +  romt    × zrvs...t  × Ervs  +
l

 
t		

 
f ree
t
 

τ =t−ip
 
rvs   × Y zrvs..τ 
 
≥ rhsm  ,
= rhst
   ≤ rhst
 




where
U svd.e.t	and Y U svd..t are the activity and capacity variables of the end-use technologies,
zrvs..lt,	zrvs...t and Y zrvs..t are the activity variables of technologies with and without load regions and the capacity variables of the technologies,
Ervs	and Esvd  are the efficiencies of the technologies; they are included by the code,
svd	is the relative factor per unit of output of technology v (coefficient) for relational constraint m,

svd	is the same per unit of new built capacity,

rvs	is the relative factor per unit of output of technology v (coefficient) for relational constraint m, load region l,

rvs	is the same per unit of new built capacity,

tl	is 1 for relations to construction and ∆τ for relations to total capacity,

ip 	is 1 for accounting during construction and
the plant life on periods for accounting of total capacity, and

 
rhst
 
is the right hand side of the constraint.
 


8.1.2 	Relation with Load Regions

N m....lt or P m....lt


The user defined relations can be defined with load regions. Then all entries of activities of technologies with load regions are divided by the length of the according load region resulting in a representation of the utilized power.


 
	ed
 romlt
 
t	
mlt
 

svd
 
svd  ×
 

e=0
 
U svd.e.t × Esvd   +
 

τ =t−ip
 
rcsvd   × Y U svd..τ  +
 

 


rvs
 
mlt rvs
λl 	×
 

zrvs..lt  × Ervs   +  romt
 

× zrvs...t  × Ervs  +
 



 

t		

 
f ree
t
 

τ =t−ip
 
rvs   × tl × Y zrvs..τ 
 
≥ rhsml  ,
= rhst
   < rhst
 




where
U svd.e.t	and Y U svd..t are the activity and capacity variables of the end-use technologies,
zrvs..lt,	zrvs...t and Y zrvs..t are the activity variables of technologies with and without load regions and the capacity variables of the technologies,
Ervs	and Esvd  are the efficiencies of the technologies; they are included by the code,
svd	is the relative factor per unit of utilized capacity of technology v (coefficient) for relational constraint m in load region l, period t (this constraint is adapted to
represent the utilized power, as stated above),

svd	is the same per unit of new built or installed capacity,

rvs	is the relative factor per unit of output of technology v (coefficient) for relational constraint m, load region l,

rvs	is the same per unit of new built capacity,

tl	is 1 for relations to construction and ∆τ for relations to total capacity,

ip 	is 1 for accounting during construction and
the plant life on periods for accounting of total capacity, and

 
rhst
 
and is the right hand side of the constraint.
 



8.1.3 	Construction of Relations between Periods


N m.....t or P m.....t


The change of activities over time can either be limited or included in the objective by constructing relations between periods: The relations expresses the difference between the annual activity in a period and the following period. This difference can either be limited or included in the objective function.


 



svd
 

mt
svd  ×
 
ed

e=0
 

U svd.e.t × Esvd   − rom(t−1)  ×
 

 
ed

e=0
 
l
U svd.e.(t − 1) × Esvd	+
 



rvs
 
\ 	mt rvs
 

× zrvs...t  × Ervs   − rom(t−1)  ×
 




 
zrvs...(t − 1) × Ervs  ] +
 


rvs
 
mlt
rvs   ×
 
zrvs..lt  × Ervs   − roml(t−1)  ×
l
 
   f ree
l 

 
zrvs..l(t − 1) × Ervs l
 
≥ rhsm  ,
= rhst
   < rhst
 



where
U svd.e.t	is the activity variable of the end-use technologies,
zrvs..lt	and zrvs...t are the activity  variables of technologies with and without load regions,
Ervs	and Esvd  are the efficiencies of the technologies; they are included by the code,
svd	is the relative factor per unit of output of technology v (coefficient) for relational constraint m, period t,
rvs	is the relative factor per unit of output of technology v (coefficient) for relational constraint m, load region l, and
 
rhst
 
and is the right hand side of the constraint.
 



For this type of constraints only the ro-coefficients have to be supplied by the user, the rest is included by the model. It can be defined with and without load regions.


8.1.4 	Special Handling of Demand Elasticities

P m.....t


The second type of user defined relations differs from the first one in the fact that the activity of the end-use technologies is multiplied by ke and therefore represents the production without reduction by demand elasticities.

Thus this constraint can be applied to force a certain reduction level due to the elasticities reached in one period to be also reached in the following period, allowing the interpretation of the reduction as investments  in saving. The coefficient of the technologies supplying a demand have to be the inverse of this demand in the current period, then. This constraint has the following form:
 
ed

sv	e=0

ed
 

U svd.e.t × Esvd   ×
 

κe
U d.t  −

κe
 


where
 

sv	e=0
 
U svd.e.(t − 1) × Esvd   ×
 
U d.(t −
 
1)   ≤ 0 ,
 

the coefficients are supplied by MESSAGE. The user can additionally define multiplicative factors for these coefficients.
 




