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
