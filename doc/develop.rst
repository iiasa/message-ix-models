Development practices
*********************

This page describes development practices for :mod:`message_ix_models` and :mod:`message_data` intended to help reproducibility, interoperability, and reusability.

In the following, the bold-face words **required**, **optional**, etc. have specific meanings as described in `IETF RFC 2119 <https://tools.ietf.org/html/rfc2119>`_.

On other pages:

- :doc:`message-ix:contributing` in the MESSAGEix docs.
  *All* of these apply to contributions to :mod:`message_ix_models` and :mod:`message_data`, including the :ref:`message-ix:code-style`.
- :doc:`data`, for how to add and handle these.

On this page:

.. contents::
   :local:
   :backlinks: none

.. _check-support:

Advertise and check compatibility
=================================

There are multiple choices of the base structure for a model in the MESSAGEix-GLOBIOM family, e.g. different :doc:`pkg-data/node` and :doc:`pkg-data/year`.

Code that will only work with certain structures…

- **must** be documented, and include in its documentation any such limitation, e.g. “:func:`example_func` only produces data for R11 and year list B.”
- **should** use :func:`.check_support` in individual pieces of code to pre-emptively check and raise an exception.
  This prevents inadvertent use of the code where its data will be invalid:

.. code-block:: python

    def myfunc(context, *args):
        """A function that only works on R11 and years ‘B’."""

        check_support(
            context,
            dict(regions=["R11"], years=["B"]),
            "Example data produced"
        )

        # … function code to execute if the check passes

Code **may** also check a :class:`.Context` instance and automatically adapt data from certain structures to others, e.g. by interpolating data for certain periods or areas.
To help with validation, code that does this **should** log on the :data:`logging.INFO` level to advertise these steps.

.. _policy-upstream-versions:

Upstream version policy
=======================

:mod:`message_ix_models` is developed to be compatible with the following versions of its upstream dependencies.

:mod:`ixmp` and :mod:`message_ix`

   The most recent 4 minor versions, or all minor versions released in the past two (2) years—whichever is greater.

   For example, as of 2024-04-08:

   - The most recent release of :mod:`ixmp` and :mod:`message_ix` are versions 3.8.0 of each project.
     These are supported by :mod:`message_ix_models`.
   - The previous 3 minor versions are 3.7.0, 3.6.0, and 3.5.0.
     All were released since 2022-04-08.
     All are supported by :mod:`message_ix_models.`
   - :mod:`ixmp` and :mod:`message_ix` versions 3.4.0 were released 2022-01-24.
     These this is the fifth-most-recent minor version *and* was released more than 2 years before 2024-04-08, so it is not supported.

Python
   All currently-maintained versions of Python.

   The Python website displays a list of these versions (`1 <https://www.python.org/downloads/>`__, `2 <https://devguide.python.org/versions/#versions>`__).

   For example, as of 2024-04-08:

   - Python 3.13 is in "prerelease" or "feature" development status, and is *not* supported by :mod:`message_ix_models`.
   - Python 3.12 through 3.8 are in "bugfix" or "security" maintenance status, and are supported by :mod:`message_ix_models`.
   - Python 3.7 and earlier are in "end-of-life" status, and are not supported by the Python community or by :mod:`message_ix_models`.

- Support for older versions of dependencies **may** be dropped as early as the first :mod:`message_ix_models` version released after changes in upstream versions.

  - Conversely, some parts of :mod:`message_ix_models` **may** continue to be compatible with older upstream versions, but this compatibility is not tested and may break at any time.
  - Users **should** upgrade their dependencies and other code to newer versions; we recommend the latest.
- Some newer code is marked with a :func:`.minimum_version` decorator.

  - This indicates that the marked code relies on features only available in certain upstream versions (of one of the packages mentioned above, or another package), newer than those listed in `pyproject.toml <https://github.com/iiasa/message-ix-models/blob/main/pyproject.toml>`__.
  - These minima **must** be mentioned in the :mod:`message_ix_models` documentation.
  - Users wishing to use this marked code **must** use compatible versions of those packages.
