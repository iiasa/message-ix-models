7.1 	Variables


Generally MESSAGE does not generate any variables related to an energy carrier alone. However, in the case of man-made fuels, that are accumulated over time, a variable that shifts the quantities to be transferred from one period to the other is necessary.



7.1.1 	Stock-pile Variables
Qf b....t, where
Q	identifies stock-pile variables,
f	identifies the fuel with stock-pile,
b	distinguishes  the variable from the equation, and
t	is the period identifier.


The stock-pile variables represent the amount of fuel f that is transferred from period t into period t + 1. Note that these variables do not represent annual quantities, they refer to the period as a whole. These variables are a special type of storage, that just transfers the quantity of an energy carrier available in one period into the next period. Stock-piles are defined  as a separate level. For all other energy carriers any overproduction that occurs in a period is lost.



7.2 	Constraints


7.2.1 	Stock-piling Constraints

Qf.....t
 


Q is a special level on that energy forms can be defined that are accumulated over time and consumed in later periods. One example is the accumulation of plutonium and later use in fast breeder reactors.

The general form of this constraint is:


 
Qf b....t − Qf b....(t − 1) +
v	l
 
∆t × ( zf vd..lt + βf
 
× zφvd..lt −
 


 
Esvf   × zsvf u.lt − βf
 
× zsvφ..lt ) + ∆t × ι(svd, f ) × Y zsvd..(t + 1) −
 


∆(t − τsvd − 1) × ρ(svd, f ) ×  Y zsvd..(t − τsvd) ] = 0 ,





where
f	is the identifier of the man-made fuel (e.g. plutonium, U233),
τsvd 	is the plant life of technology v in periods,
ι(svd, f ) 	is the ”first  inventory”  of technology v of f (relative to capacity of main output),
ρ(svd, f ) 	is the ”last core” of f in technology v, see also section  6.1.5,
∆t 	is the length of period t in years,
zf vd..lt 	is the annual input of technology v of fuel f in load region l and period t (l is ”.”
if v does not have load regions), and
Y zf vd..t 	is the annual new installation of technology v in period t.
 






