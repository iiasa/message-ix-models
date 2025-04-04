HOWTO guides
************

This section contains some HOWTO guides.
The name alludes to the `Linux Documentation Project <https://tldp.org/FAQ/LDP-FAQ/index.html#AEN56>`_, wherein:

   A HOWTO is usually a step-by-step guide that describe[s], in detail, how to perform **a specific task**.

This contrasts with the :mod:`message_ix_models` API reference documentation, which describes exactly and completely WHAT code exists and does.
API documentation is neutral with regards to *how* a user (like you!) might choose to use/apply the code, while a HOWTO lays out a particular way to apply the code, given the task to be accomplished.

Each HOWTO is contained on its own page:

.. toctree::
   :maxdepth: 1
   :caption: HOWTO

   Run a baseline model <quickstart>
   Run mix-models on UnICC <unicc>
   path
   migrate
   Release message-ix-models <release>

Write a HOWTO
=============

1. Create a new page in this directory and add it to the ``..toctree::``.
2. Choose a title and write headings and sentences using the imperative case, the same as the MESSAGEix :ref:`message-ix:code-style` recommends for commit messages:

   Bad
      Debugging long solve times

      Setting up your virtual environment

   Good
      Debug long solve times

      Set up a virtual environment

3. Include a section on "Prerequisite knowledge" or similar.
   Give links to other documentation or external resources that contain information the user needs in order to successfully follow the HOWTO.
   These can include the original documentation, manpages, etc. for other tools to be used.
4. Throughout, if you repeat key details from those external resource
5. Use Sphinx :doc:`admonition, message, and warning directives <sphinx:usage/restructuredtext/directives>` to highlight information for the reader.
   **Do not** use UPPER CASE or other ad-hoc formatting for the same purpose.
6. Use :rfc:`2119` keywords in bold-face like **must** and **should**, to distinguish optional from mandatory steps and actions.
