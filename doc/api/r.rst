Integrate R code (:mod:`.util.r`)
*********************************

Developers working with :mod:`message_ix_models` can integrate existing R codes or use R to perform data processing related to building, running, and reporting models.
:mod:`message_ix_models.util.r` provides utility code to help with using :mod:`rpy2` to achieve this integration.

Two patterns are supported.
To use either, place a  :file:`r_code.R` *in-line* in the source tree, either in :mod:`message_ix_models`, :mod:`message_data`, or another package::

    message_ix_models/
        project/
            example/
                __init__.py
                module_a.py
                module_b.py
                r_code.R

The patterns are:

.. contents::
   :local:
   :backlinks: none


R ‘Modules’
===========

In this pattern, :func:`get_r_func` is used to source (i.e. import or run) R code from a file that defines one or more functions or other objects.
:func:`get_r_func` returns this function, which can then be called from Python code.
In this way, the R source file functions somewhat like a Python **‘module’**, while still being lighter weight than a full, installable R package.

.. code-block:: R
   :caption: :file:`r_code.R`

   # R file that defines functions and other entry points

   mul <- function(x, y) {
     # Multiply operands
     return(x, y)
   }

.. code-block:: python

   from message_ix_models.util import get_r_func

   # Source r_code.R, retrieve the function named "mul"
   mul = get_r_func("message_ix_models.project.example.r_code:mul")

   # Call the function
   result = mul(1.2, 3.4)

   # …use `result` in further Python code

.. currentmodule:: message_ix_models.util.r

.. autofunction:: get_r_func


Stand-alone scripts
===================

In this method, the R script is placed inline.
