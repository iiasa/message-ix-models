MESSAGEix-GLOBIOM global model
==============================

These pages document the IIASA Integrated Assessment Modeling (IAM) framework, also referred to as **MESSAGEix-GLOBIOM**, owing to the fact that the energy model |MESSAGEix| and the land use model GLOBIOM are its most important components.
|MESSAGEix|-GLOBIOM was developed for the quantification of the so-called Shared Socio-economic Pathways (SSPs) which are the first application of the IAM framework.

.. note:: The documentation in this section was originally available at https://docs.messageix.org/global/ and maintained in a separate repository at `iiasa/message_doc <https://github.com/iiasa/message_doc>`_.
   In the future, it will be maintained in `iiasa/message-ix-models <https://github.com/iiasa/message_doc>`_ and appear at the current URL.

   The overall :mod:`message_ix_models` documentation provides **technical** description of the Python package of the same name, associated data, and their usage for *all* models in the |MESSAGEix|-GLOBIOM ‘family’.
   This section provides a thorough **methodological** description of the particular, central, global-scope instance of |MESSAGEix|-GLOBIOM developed by the IIASA ECE Program, from which most other instances derive.

   This section is periodically updated and expanded with additional information to describe the current implementation and its changes over time.

When referring to |MESSAGEix|-GLOBIOM as described in this section, please use the following citations: [1]_

.. bibliography::
   :list: bullet
   :style: unsrt
   :filter: key in {"message_globiom_2020", "fricko_havlik_2017"}

.. [1] Download these citations in :download:`RIS </messageix-globiom.ris>` or :download:`BibTeX </messageix-globiom.bib>` format (web only).

|MESSAGEix|-GLOBIOM is based on the :doc:`message-ix:framework`, which provides a flexible, *generic* abstraction of energy systems optimization models that can be parametrized in many ways.
:mod:`message_ix` includes the ‘MACRO’ computable general equilibrium (CGE) for implementing macro-economic feedback.
To refer to the generic MESSAGE, MACRO, and combined models—rather than the particular |MESSAGEix|-GLOBIOM IAM instance or its specific applications for various publications and assessments—please follow the :ref:`“User guidelines and notice” section <message-ix:notice-cite>` of the :mod:`message_ix` documentation.

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
