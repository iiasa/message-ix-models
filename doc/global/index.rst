|name| global model
*******************

.. caution:: This section of the documentation is under revision
   on this branch.
   See :issue:`424` for details.

   Text on these pages may mix old and revised material;
   links may be broken;
   and in general these pages **should not** be used as reference
   until changes are reviewed and merged via :pull:`425`.

These pages document the IIASA Integrated Assessment Modeling (IAM) framework,
whose name, **MESSAGEix-GLOBIOM-GAINS**, marks its three central models.
|MESSAGEix| resolves the energy system technology by technology,
GLOBIOM represents the agriculture, forestry and other land use (AFOLU) system,
returning bioenergy supply potentials, AFOLU emissions,
and the cost of abating them,
and GAINS supplies air pollutant and non-CO2 greenhouse gas emissions
and their abatement options.

The version documented here produced the |name| contribution to
the `Scenario Model Intercomparison Project (ScenarioMIP)
<https://wcrp-cmip.org/mips/scenariomip/>`_,
as part of the seventh phase of the
`Coupled Model Intercomparison Project (CMIP7)
<https://wcrp-cmip.org/cmip-phases/cmip7/>`_,
whose design is set out in van Vuuren et al., 2026
:cite:`van_vuuren_scenariomip_2026`.
It builds on the predecessor version **MESSAGEix-GLOBIOM**
(Krey et al., 2020 :cite:`message_globiom_2020`),
which was developed for the quantification of the
Shared Socio-economic Pathways (SSPs)
(O’Neill et al., 2017 :cite:`oneill_roads_2017`, Riahi et al., 2017 :cite:`riahi_shared_2017`),
and produced their SSP2 marker scenario
(Fricko et al., 2017 :cite:`fricko_marker_2017`).

.. note:: The documentation in this section was originally available at https://docs.messageix.org/global/ and maintained in a separate repository at `iiasa/message_doc <https://github.com/iiasa/message_doc>`_.
   In the future, it will be maintained in `iiasa/message-ix-models <https://github.com/iiasa/message_doc>`_ and appear at the current URL.

   The overall :mod:`message_ix_models` documentation provides **technical** description of the Python package of the same name, associated data, and their usage for *all* models in the |name| ‘family’.
   This section provides a complete **conceptual and methodological** description
   of the particular, central, global-scope instance of |name|
   developed by the IIASA ECE Program, from which most other instances derive.

   This section is periodically updated and expanded with additional information to describe the current implementation and its changes over time.

When referring to |name| as described in this section,
please use the following citations: [1]_

.. bibliography::
   :list: bullet
   :style: unsrt
   :filter: key in {"message_globiom_gains_2026", "fricko_wu_2026"}

.. [1] Download these citations in :download:`RIS </messageix-globiom.ris>` or :download:`BibTeX </messageix-globiom.bib>` format (web only).

|name| is based on the :doc:`message-ix:framework`,
which provides a flexible, *generic* abstraction of energy systems optimization models
that can be parametrized in many ways.
:mod:`message_ix` includes the ‘MACRO’ computable general equilibrium (CGE) for implementing macro-economic feedback.
To refer to the generic MESSAGE, MACRO, and combined models
—rather than the particular |name| IAM instance
or its specific applications for various publications and assessments—
please follow the :ref:`“User guidelines and notice” section <message-ix:notice-cite>`
of the :mod:`message_ix` documentation.

We thank Edward Byers, Jessica Jewell, Ruslana Palatnik, Narasimha D. Rao, and Fabio Sferra for their valuable comments that helped improving the text.

.. toctree::
   :maxdepth: 1

   overview/index
   socio_econ/index
   energy/index
   macro
   land_use/index
   water/index
   emissions/index
   climate/index
   annex/index
   further-reading
   z_bibliography

.. Under development; excluded.
   See also the exclude_patterns setting in conf.py.

   glossary

.. Leave these lines commented for releases; uncommented during development/on
   `master`.

   .. toctree::
      :hidden:

      _extra/index
