7 User-defined Relations
=======================

7.1 	Constraints
------------------

The user-defined relations allow the user to construct constraints that are not included in the basic set of constraints. For each technology  the user can specify coefficients with that either the production variables (see section2.1.1),  the annual new installation variables  (see section
2.1.2) or the total capacity in a year (like it is used in the capacity constraints, see section
2.2.1) can be included in the relation. The relations can be defined with and without load regions, have a lower, upper or fix right hand side or remain free (non-binding) and be related to an entry in the objective function, i.e., all entries to this relation are also entered to the objective function with the appropriate discount factor. There are two types of user-defined constraints, for which the entries to the objective function–without discounting–are summed up under the cost accounting rows *CAR*1 and *CAR*2 (see chapter 9).

The formulation of the user-defined relations is given for relations, that are related to the main output of the technologies. It is also possible (e.g., for emissions) to relate the constraint to the main input of the technology, i.e. the amount of fuel used. In this case the efficiencies (:math:`E`) would be omitted from the formulation.


7.1.1 	Relation without  Load Regions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:math:`N m.....t` or :math:`P m.....t`


Relations without load regions just sum up the activities (multiplied with the given coefficients) of all variables defined to be in this constraint. If a technology has load regions, the activity variables for all load regions of this technology are included. If the total capacity of a technology is included, all new capacities from previous periods still operating are included, if new capacities are included, the annual new installation of the current period is taken.

.. math::

	ed	t	 romt	mtsvd svd  ×e=0
 U svd.e.t × Esvd   +
τ =t−ip rcsvd   × Y U svd..τ  + rvs mlt
rvs   × zrvs..lt  × Ervs   +  romt    × zrvs...t  × Ervs  + l  t		  f ree t τ =t−ip
rvs   × Y zrvs..τ  ≥ rhsm  , = rhst    ≤ rhst

where
:math:`U svd.e.t`	  and :math:`Y U svd..t` are the activity and capacity variables of the end-use technologies,
:math:`zrvs..lt,	   :math:`zrvs...t` and :math:`Y zrvs..t` are the activity variables of technologies with and without load regions and the capacity variables of the technologies,
:math:`Ervs`       	and :math:`Esvd` are the efficiencies of the technologies; they are included by the code,
:math:`svd`        	is the relative factor per unit of output of technology :math:`v` (coefficient) for relational constraint :math:`m`,
:math:`svd`        	is the same per unit of new built capacity,
:math:`rvs`        	is the relative factor per unit of output of technology v (coefficient) for relational constraint :math:`m`, load region :math:`l`,
:math:`rvs`        	is the same per unit of new built capacity,
:math:`tl	          is 1 for relations to construction and :math:`∆τ` for relations to total capacity,
:math:`ip`         	is 1 for accounting during construction and
                    the plant life on periods for accounting of total capacity, and
:math:`rhst`        is the right hand side of the constraint.
 

7.1.2 	Construction of Relations between Periods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:math:`N m.....t` or :math:`P m.....t`


The change of activities over time can either be limited or included in the objective by constructing relations between periods: The relations expresses the difference between the annual activity in a period and the following period. This difference can either be limited or included in the objective function.

.. math::

svd mt svd  × ed e=0 U svd.e.t × Esvd   − rom(t−1)  × ed e=0 l U svd.e.(t − 1) × Esvd	+ rvs  	mt rvs
 

× zrvs...t  × Ervs   − rom(t−1)  ×
 

zrvs...(t − 1) × Ervs  ] +
 

rvs mlt rvs   ×
 
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
:math:`U svd.e.t`   is the activity variable of the end-use technologies,
:math:`zrvs..lt`	   and :math:`zrvs...t` are the activity  variables of technologies with and without load regions,
:math:`Ervs`       	and :math:`Esvd` are the efficiencies of the technologies; they are included by the code,
:math:`svd`        	is the relative factor per unit of output of technology :math:`v` (coefficient) for relational constraint :math:`m`, period :math:`t`,
:math:`rvs`        	is the relative factor per unit of output of technology :math:`v` (coefficient) for relational constraint :math:`m`, load region :math:`l`, and
 :math:`rhst`       and is the right hand side of the constraint.
 
For this type of constraints only the :math:`ro`-coefficients have to be supplied by the user, the rest is included by the model. It can be defined with and without load regions.

