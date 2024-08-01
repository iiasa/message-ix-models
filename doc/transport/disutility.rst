Disutility costs
****************

One form for a discrete choice model is the multinomial logit (MNL):

.. math::

   U_{in} & = V_{in} + \varepsilon_{in} = \beta^\prime x_{in} + \varepsilon_{in} \\
   P(i | C_n) & = P(U_{in} = \max_{j \in C_n}{U_{jn}})
   = \frac{\exp{\mu V_{in}}}{\sum_{j \in C_n}{\exp{\mu V_{jn}}}}

wherein:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Symbol
     - Description
   * - :math:`C_n`
     - Choice set of alternatives for agent :math:`n`
   * - :math:`U_{in}`
     - Agent :math:`n`'s utility of alternative :math:`i`
   * - :math:`\varepsilon_{in}`
     - Random part of utility :math:`\sim \text{ExtremeValue}(0, \mu)`
   * - :math:`V_{in}`
     - Systematic part of utility, a function of observables
   * - :math:`x_{in}`
     - Vector of observables for alternative :math:`i` and agent :math:`n`
   * - :math:`\beta`
     - Vector of parameters
   * - :math:`P(i | C_n)`
     - Probability that agent :math:`n` chooses alternative :math:`i`

Some points about this general formulation:

- The particular random distribution selected for :math:`\varepsilon_{in}` is what makes this a logit as opposed to probit or other kind of model.

- Commonly :math:`C_n` is the same for all :math:`n`.

- When individual data are used to *estimate* the model (i.e. :math:`\beta`), then :math:`n` may enumerate every individual.

  On the other hand, when the estimated model is used to *predict* the behaviour of 1 or more representative agents, then :math:`n` enumerates those agents: e.g. single representative agent for each country in a model; or a single representative agent for each of 2 or more consumer groups within each such country.

- Some elements of :math:`x_{in}` may be 0 for a given :math:`i` and for all :math:`n`.
  This allows the concept of "different functional forms," so long as all forms are linear.

  For example: suppose :math:`C_n` includes the alternatives bus and walk, for which the systematic utilities are:

  .. math::

     V_{\text{bus},n} & = a \cdot \text{speed}_n + b \cdot \text{ticket price}_n \\
     V_{\text{walk},n} & = a \cdot \text{speed}_n + c \cdot \text{temp}_n

  The observable :math:`\text{ticket price}` is only meaningful for the bus alternative, and not for walking.
  Likewise, the outdoor temperature :math:`\text{temp}` is only meaningful for the walking alternative, but not for riding the (climate controlled) bus.

  These functional forms appear different, but both are linear, so they can be written as:

  .. math::

     V_{in} & = \beta^\prime x_{in} \\
     \beta & = \begin{bmatrix}a & b & c\end{bmatrix} \\
     x_{in} & = \begin{bmatrix}\text{speed}_{in} & \text{ticket price}_{in} &  \text{temp}_in\end{bmatrix}

  Now when :math:`\text{ticket price}_{\text{bus},n} = 0 \forall n`, i.e. the third element is always 0 when the alternative is bus; and likewise
  :math:`\text{temp}_{\text{walk},n} = 0 \forall n`.

  This method also covers the possibility the agents are *differently* sensitive to the speeds of the bus and of walking.
  Then instead of :math:`\text{speed}`, define two distinct observables :math:`\text{speed}^\text{walk}` and :math:`\text{speed}^\text{bus}`, with distinct corresponding coefficients in :math:`\beta`, and treat them the same way.

Cost equivalents
================

Suppose :math:`C_n` is a set of some vehicle types: internal combustion engine vehicle (ICEV), electric vehicle (EV), etc.

.. math::

   x^{A\prime}_{in} & =
   \begin{bmatrix}
     \text{purchase price} \\
     \text{variable price} \\
     \text{fuel/energy price} \\
     \text{charging station availability} \\
     \text{technology novelty} \\
     \text{peer social influence} \\
   \end{bmatrix}_{in} \\
   \beta^A & = \begin{bmatrix} b_1 & b_2 & b_3 & b_4 & b_5 & b_6\end{bmatrix}

In this model, which we call the ‘full’ model:

- Some observables (the first three) are costs, i.e. they are measured in money, with units of EUR or USD.
- Some observables (the last three) are not costs.
  They are measured with non-monetary units.
- If we have estimated this model, then we have the choice probabilities :math:`P(i | C_n)` for every agent, and the values of :math:`\beta^A`.

Then we define **disutility cost equivalents** as follows.
We specify a ‘reduced’ model that has fewer elements in the vectors of observables :math:`x^B_{in}` and parameters :math:`\beta^B`, but yields the same choice probabilities.

.. math::

   \beta^{A\prime} x^A_{in} & = \beta^{B\prime} x^B_{in} \\
   x^{B\prime}_{in} & =
   \begin{bmatrix}
     \text{purchase price} \\
     \text{variable price} \\
     \text{fuel/energy price} \\
     \text{disutility cost} \\
   \end{bmatrix}_{in} \\
   \beta^B & = \begin{bmatrix} b_1 & b_2 & b_3 & -1\end{bmatrix} \\
   \text{disutility cost}_{in} & = \begin{bmatrix}
      b_4 \\
      b_5 \\
      b_6 \\
   \end{bmatrix}
   \cdot
   \begin{bmatrix}
     \text{charging station availability} \\
     \text{technology novelty} \\
     \text{peer social influence} \\
   \end{bmatrix}^\prime_{in}

Exercise: use the expression at the top of the page to show that :math:`V^B_{in} = \mu \beta^{B\prime} x^B_{in}` yields the same choice probabilities :math:`P(i | C_n)` as :math:`V^A_{in} = \mu \beta^{A\prime} x^A_{in}`.

Note that:

- Each observable, e.g. :math:`\text{charging station availability}`, may be 0 for all :math:`n` for certain alternatives :math:`i` (e.g. ICEV) where it is not relevant.
- The :math:`\text{disutility cost}` is a *positive* number with monetary units (e.g. 1024 USD); we arbitrarily select a fixed value of :math:`-1 \left[\frac{1}{\text{USD}}\right]` for its parameter in :math:`\beta^B`.
  This is for an intuitive and consistent interpretation: a *greater* disutility *reduces* the total systematic utility :math:`V^B_{in}`.
