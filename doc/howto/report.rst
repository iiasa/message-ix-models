Develop reporting code for :mod:`message_ix_models`
***************************************************

**Reporting**,
sometimes also “post-processing”,
is the term for all operations done after a MESSAGE model has solved.
This HOWTO gives current (as of 2026-Q1) guidance
on building and using reporting code and configuration.

Currently, this HOWTO focuses on generating reporting output in the IAMC data structure,
and does not discuss reporting output in other formats
or producing figures or other products.

.. contents::
   :local:
   :backlinks: none

Background
==========

**Read** the following, which contain or link to basic and essential information
that is not repeated below:

- :doc:`/api/report/index`, in particular:

  - The sections :ref:`report-intro` and :ref:`report-features`.
  - The upstream :mod:`message_ix.report` documentation and tutorial.

- :doc:`/api/report/legacy`

**Understand** the history and development context of reporting code
in :mod:`message_ix_models` and related packages:

- :mod:`message_data.tools.post_processing.iamc_report_hackathon`
  is the oldest collection of reporting code developed to work with 
  MESSAGEix-GLOBIOM global models.

  - As hinted by the module name, it was developed in a hackathon held in 2018.
    This was shortly after :mod:`message_ix` was created
    and replaced the older implementation known as “MESSAGE (V)”
    that did not use Python or :mod:`ixmp`.
  - This also predates the creation of the (public) :mod:`message_ix_models` package in 2021.

  This system is sometimes called **‘legacy’ reporting**,
  although that is a misnomer because the system is still in active use.
  This is the reason for quotes on ‘legacy’.

- Starting in 2019,
  :mod:`message_ix.report` and the supporting packages :mod:`ixmp.report` and :mod:`genno`
  were developed.
  They were designed and implemented to address a specific set of requirements.
  Those requirements were derived from by experiences
  using the :mod:`.iamc_report_hackathon` code,
  related to:

  - Performance.
  - Verbosity of the code.
  - Difficulty to generalize / adapt the code to match variations in model structure.

  This is sometimes called **genno-based reporting**,
  after the low-level package that provides the basic features.

- Work *began*, but as of 2026 *has not finished* on building a full/complete reporting
  implementation using :mod:`message_ix.report` that can exactly reproduce
  the output of :mod:`.iamc_report_hackathon`.

- Some newer developments such as :doc:`/transport/index` and :doc:`/material/index`
  have developed associated reporting using :mod:`message_ix.report`/:mod:`genno`.

- One version of the ‘legacy’ reporting was migrated to
  :mod:`message_ix_models.report.legacy` in :pull:`159`.

- In parallel, other projects and work
  such as :doc:`/project/scenariomip` and :doc:`/project/ssp`
  have continued to use and modify :mod:`.iamc_report_hackathon`.

  - Multiple versions of this code exist on various branches and forks of
    :mod:`message_data`.
  - Usually each version of the code is tailored to a specific model structure.
  - These associations are not fully documented.

The long-term goal is to have a single reporting code base that meets many criteria:
concise,
performant,
flexible/easily adapted and configured,
fully documented and tested,
open source,
etc.
The recommendations in this HOWTO balance, on the one hand,
using existing code effectively in ways that match available resources,
and on the other hand working towards this long-term goal.

Gather information and plan
===========================

**Consider the following questions.**
Post in the ``#message`` Slack channel describing your use-case and answers.
This will allow colleagues with complementary experience to confirm or correct your assessment.

“How much time do I have available?”
------------------------------------

- If the answer is *“very little time”*,
  then you should focus on understanding, configuring, and re-using existing code,
  maybe with light modifications,
  and seeking technical assistance from the colleagues who developed or maintain that code.

  This should yield results—that is, reporting outputs—in the minimum possible time.
  However, it may be more difficult to re-use, repeat, or build on the process in the future,
  and the possibility for new/changed behaviour is limited.

- If the answer is *“plenty of time”,
  then you can invest in learning more about the tools and underlying features,
  and understanding how to apply them to meet your reporting needs.

  This may yield code that is more concise, performance, robust, configurable, and maintainable.

“What, precisely, do I need to report?”
---------------------------------------

There can be multiple answers to this question.
They could include:

1. “Only IAMC-structured data.”
2. “Data in other formats, figures, and other products.”
3. “The full set of IAMC ‘variables’ as for the base MESSAGEix-GLOBIOM global model.”
4. “Specific quantities or IAMC ‘variables’ related to a model with variant structure,
   such as MESSAGEix-Buildings, -Materials, -Nexus, or -Transport.”

Answers like (3) may mean you need to find, maybe adapt, and run a particular
version of the legacy reporting; :ref:`see below <howto-report-legacy>`.

For answers like (2) and (4), you may be able to use genno-based reporting,
either :ref:`using existing mid-level tools <howto-report-material>` (recommended)
or :ref:`directly <howto-report-genno>` (for advanced users/developers).

Other answers like (1) do not imply any specific choice.

.. _howto-report-material:

Use :mod:`genno`-based reporting via existing tools
===================================================

For this approach,
**study and emulate or reuse** the code in :mod:`message_ix_models.model.material.report`.
This code has partial documentation,
and is tested, type hinted, and open source.

The basic pattern of this code is:

1. Prepare some configuration files in YAML format.

2. Use :mod:`message_ix.report` to generate ‘raw’ IAMC-structured data.

   This data is in the target structure, but the labels
   (especially for the IAMC ‘variable’ dimension)
   are not the desired or final output.
3. Invoke code that reads (1) and transforms (2) into final output.

Using this approach,
reporting mainly involves editing YAML files (1),
which should be more realistic for new users.
Existing files,
for instance :file:`data/material/reporting/ch4_emi.yaml`,
can be used as examples or templates.

.. _howto-report-legacy:

Use or adapt ‘legacy’ reporting
===============================

As of 2026-04-28, the most up-to-date version of the ‘legacy’ reporting code
is on the `ssp_dev branch of message_data <https://github.com/iiasa/message_data/tree/ssp_dev>`__.
This code is proprietary (not open source) and has no public documentation.

The page :doc:`/api/report/legacy` describes an older version of the legacy reporting code,
:doc:`migrated <migrate>` to :mod:`message_ix_models`,
and gives some description of how the code works.
This information may not be applicable to other versions of the legacy reporting code;
be sure to check (or ask) to confirm.

Some ways in which legacy reporting code can be used or adapted:

- Edit its configuration files, which have names like ``default_run_config.yaml``.
- Edit existing ‘table’ files that contain functions for reporting different quantities.
  These files have names like :file:`default_tables.py` or :file:`ENGAGE_SSP2_v417_tables.py`,
  and are referred to by the configuration file(s).
- Create new configuration or table files.

Because of the nature of the legacy reporting code:

- Most of these adjustments **should** be performed on a branch,
  to avoid interference with versions tailored for other model variants or projects.
- You **must** document the specific changes made for your application.
  The files are several thousand lines,
  and it will be difficult for colleagues to spot your changes without such documentation.

.. _howto-report-genno:

Use :mod:`genno` directly
=========================

For this approach,
**study and emulate or reuse** the code in :mod:`message_ix_models.model.transport.report`.

This code is compatible with :func:`message_ix_models.report.report`,
and uses a specific :func:`~.transport.report.callback` function
to add many keys and operations
that manipulate data in the :class:`genno.Quantity` data structure
with full dimensionality and units.

These data are later transformed into IAMC structure
(in contrast to the above section where this transformation occurs first),
into figures and other products,
and finally written to file.
