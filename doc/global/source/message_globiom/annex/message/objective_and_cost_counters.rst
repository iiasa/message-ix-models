9.1 	Constraints


9.1.1 	Cost Accounting Rows


The different types of costs (i.e. entries for the objective function) can be accumulated  over all technologies in built-in  accounting rows. These rows can be generated per period or for the whole horizon and contain the sum of the undiscounted costs. They can also be limited. The implemented types are:


C C U R  –  fix (related to the installed capacity) and variable (related to the production)
operation and maintenance costs,
C C AP   –	investment costs; if the investments of a technology are distributed over the previous periods, also the entries to this accounting rows are distributed (if the capital costs are levellized, the total payments in a period can be taken from C I N V ; C C AP shows the share of investments in the according period, then),
C RES  –	domestic fuel costs,
C AR1  –	costs related to the user defined relations of type 1 (see section 8), C AR2  –	costs related to the user defined relations of type 2 (see section 8), C RED   –  costs for reducing demands due to demand elasticities, only related to
technologies supplying the demands directly,
C I M P  –  import costs,
C EX P  –  gains for export, and
C I N V  –	total investments (in case of levellized investment costs,  see C C AP )



9.1.2 	The Objective Function

F U N C


In its usual form the objective function contains the sum of all discounted costs, i.e. all kinds of costs that can be accounted for. All costs related to operation (i.e. resource use, operation
 


costs, costs of demand elasticities,...) are discounted from the middle of the current period to the first year. Costs related to construction are by default discounted from the beginning of the current period to the first year. By using the facility of distributing the investments or accounting during construction these costs can be distributed over some periods before or equal to the current one (see section 10.2). This distribution can also be performed  for user defined relations.

The objective function has the following general form:

 

m ∆t
t
 
(


svd	l
 

zsvd..lt  × Esvd   ×	ccur(svd, t) +
 
l
romlt   × cari(ml, t)	+
i 	m
 

 



svd
 

Esvd   ×
 
ed

e=0
 

U svd.e.t × Esvd   ×	κe  × (ccur(svd, t) +
 

mt svd
m
 

× car2(m, t)) +
 

 

cred(d, e) +
 

mt svd
m
 
l
× car1(m, t)	+
 



svd
 
t

τ =t−τsvd
 

∆τ × Y zsvd..τ × cf ix(svd, τ ) +
 


 


r	g	l	p
 
Rzrgp.lt  × cres(rgpl, t) +
 

 



c	l	p
 

I zrcp.lt  × cimp(rcpl, t) −
 



c	l	p
 
l  
Ezrcp.lt  × cexp(rcpl, t)	+
 

 
(
t
b   ×
svd
 
t+td

τ =t
 

∆(t − 1) × Y zsvd..τ ×
 
\

svd
 

 

rcmt
 

td −τ
 
l  l
 
svd  × cari(m, t) × f rasvd,m	,
i 	m




where
∆t 	is the length of period t in years,


 
βt	t−1
 
\
     1      )
 
l∆i
,
 
n
i=1
 

dr(i)
100
∆t
2
 
βt	t
 
     1      ) 	,
 
m   = βb   ×
 

dr(t)
100
 


dr(i)	is the discount rate in period i in percent,
zsvd..lt	is the annual consumption of technology v of fuel s load region l and period t; if
v has no load regions, l = ”.”.
Esvd	is the efficiency of technology v in converting s to d,
 


ccur(svd, t) 	are the variable operation and maintenance costs of technology v (per unit of main output) in period t,
svd	is the relative factor per unit of output of technology v for relational constraint
m in period t, load region l,
car1(m, t) 	and car2(m, t) are the coefficients for the objective function, that are related to the user defined relation m in period t,
car1(ml, t) 	and car2(ml, t) are the same for load region l, if relation m has load regions,
U svd.e.t	is the annual consumption of fuel s of end-use technology v in period t and elasticity class e,
κe 	is the factor giving the relation of total demand for d to the demand reduced due to the elasticity to level e,
svd	is the relative factor per unit of output of technology v for relational constraint
m in period t,
cred(d, e)	is the cost associated with reducing the demand for d to elasticity level e,
Y zsvd..t	is the annual new built capacity of technology v in period t,
cf ix(svd, t) 	are the fix operation and maintenance cost of technology v that was built in period t,
ccap(svd, t) 	is the specific investment cost of technology v in period t (given per unit of main output),
 
n svd

rcmt
 
is the share of this investment that has to be paid n periods before the first year of operation,
 
svd	is the relative factor per unit of new built capacity of technology v for user
defined relation m in period t,
 
n svd,m
 
is the share of the relative amount of the user defined relation m that occurs n periods before the first year of operation (this can, e.g., be used to account for the use of steel in the construction of solar towers over the time of construction),
 
Rzrgp.lt	is the annual consumption of resource r, grade g, elasticity class p in load region l and period t,
cres(rgpl, t)  is the cost of extracting resource r, grade g, elasticity class p in period t and load region l (this should only be given, if the extraction is not modelled explicitly),
I zrcp.lt	is the annual import of fuel r from country c in load region l, period t and elasticity class p; if r has no load regions l=”.”,
cimp(rcpl, t) is the cost of importing r in period t from country c in load region l and elasticity class p,
Ezrcp.lt	is the annual export of fuel r to country c in load region l, period t and elasticity class p; if r has no load regions l=”.”, and
cexp(rcpl, t)  is the gain for exporting r in period t to country c in load region l and elasticity class p.
