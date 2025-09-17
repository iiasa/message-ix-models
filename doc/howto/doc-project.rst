Document research projects that use :mod:`message_ix_models`
************************************************************

This guide explains HOWTO record important information
about research project that apply MESSAGEix-GLOBIOM.
This information is critical for :doc:`/repro` of research.

.. contents::
   :local:
   :backlinks: none

Choose a repository
===================

Most projects **should** be documented in :mod:`message_ix_models`.
Because the repository is public, documentation here is most likely
to be and remain accessible
to yourself, collaborators, and other audiences.

Sometimes it is strictly necessary to create private/non-public
documentation, for instance if projects or collaborators require
that work be embargoed prior to publication.
In this case:

1. Create minimal/stub documentation in :mod:`message_ix_models`.
   Include all information that is *not* subject to restrictions.
2. Create private documentation in an appropriate location,
   for example in the :mod:`message_data` Sphinx documentation
   or on other platforms (SharePoint, Google Drive, etc.).
   Use the access control features of these tools
   to limit the information to an appropriate audience.
3. Add links, or at minimum clear references, from (1) to (2).
4. As soon as possible once relevant limitations are lifted,
   consolidate private documentation (2) into public documentation (1).
   See :doc:`/howto/migrate`.

Open a GitHub issue
===================

Before creating more permanent documentation,
the first step is to **create an issue** on the relevant repository.

1. Use the issue template for project documentation.
2. Use a title like “Document project X”.
   This title indicates what must be done to complete the issue.
3. Complete the issue template.
   Add or link to any other information
   that should be included in the documentation.

   It's okay if some of this information is not yet available;
   include whatever you have at hand.
   Don't worry about formatting.
4. Add issue metadata:

   - Assign yourself.
   - Add the ``doc`` label.
     If you anticipate having multiple issues and pull requests for the project,
     create a new label like ``p:PROJECT``.

This issue does several important tasks:

- It informs colleagues that:

  - your project exists.
  - you (and maybe others) are the people responsible for the project.
- It provides a place to collect other bits of information,
  in an organic and informal way,
  that can later be edited into (a) documentation page(s).

Create the documentation skeleton
=================================

**Create a new Git branch.**
Use a name like:

- ``issue/1234`` with the number of the “Document project X” issue
  you created above,
- ``project/{x}/doc``, or
- ``doc/{projectx}``.

**Create directories and files** according to the following schemes:

- For a model variant that can be documented on a single page:
  :file:`doc/model/{variant}.rst` or :file:`doc/{variant}.rst`.
- For a model variant with multiple documentation pages:
  :file:`doc/model/{variant}/index.rst` or :file:`doc/{variant}/index.rst`
  
  (Extensive documentation for a project or model variant
  **should** be organized with headings, tables of contents,
  and if necessary split into several files.)
- For a project that can be documented on a single page:
  :file:`doc/project/{name}.rst` or :file:`doc/{name}.rst`
- For a project with multiple documentation pages:
  :file:`doc/project/{name}/index.rst` or :file:`doc/{name}/index.rst`.

In either case, the ``{variant}`` or ``{name}`` **must** match
the corresponding Python model name (if any),
except for the substitution of hyphens for underscores.

In :mod:`message_data`, some docs have been placed ‘inline’ with the code,
for example in:

- :file:`message_data/model/{variant}/doc.rst`
- :file:`message_data/model/{variant}/doc/index.rst`
- :file:`message_data/project/{name}/doc.rst`
- :file:`message_data/project/{name}/doc/index.rst`

When code is :doc:`migrated <migrate>` from :mod:`message_data`,
these files **should** be moved to the :file:`/doc/` directory.

**Edit** the :code:`.. toctree::` directive in :file:`doc/index.rst`
to link to the single file or :file:`index.rst` in a directory with multiple files.
Keep the list in alphabetical order.

**Push** your branch and open a draft pull request.

Add information about the project, workflows, code, and data
============================================================

**Extend** your branch with one or more commits.
**Look** at existing documentation pages for other projects
and attempt to mirror the content and arrangement of those pages.

**Include** the following items,
in the order given,
if the information is relevant for your project
and you have it available.

1. The project acronym, as the page title::

     EPD
     ***

2. The full project name, in quotes::

     “Example Project for Documentation”

3. One or a few sentences summarizing the project topic.

4. A :code:`.. warning::` or other :doc:`Sphinx admonition <sphinx:usage/restructuredtext/directives>` block
   that indicates the project is ongoing or code is under development.
   This alerts readers that documentation may be incomplete
   or may change in the future.

   This admonition **should** contain:

   - Link(s) to GitHub, including:

     - A label for issues/PRs like ``p:PROJECT-X``,
       if you created one above.
     - A current tracking issue, which in turn can link to:

       - Other issues and PRs where work occurs.
       - Any of the items below.

     - A project board, if any.

   - Reference to all other locations where work is occurring,
     including any:

     - Branch(es)
       —``main``, ``dev``, or any others—
       in :mod:`message_ix_models` or :mod:`message_data`.
     - Fork(s) of these repos.
     - Other repository/-ies besides :mod:`message_ix_models`
       or :mod:`message_data`.

   This **does not** imply that the linked locations must be made public;
   only that their existence and contents should be mentioned.

5. A section titled “Project information”,
   and within it:

6. A link to the project website.
7. The project's duration.
8. The project's funder(s) or sponsor(s).
9. A list (bulleted or description list; flat or hierarchical)
   of work packages, tasks, or other activities
   that will occur during the project.
10. A list of “IIASA roles”, including especially:

    - The *project lead*.
    - The *lead modeler(s)*.
    - Any *technical advisor(s)*.

11. A section with *scenario identifiers*,
    including ‘base’ or starting scenarios for the project,
    and scenarios produced as part of the project.
    This information **should** include:

    - :mod:`ixmp` URLS giving the platform (‘database’),
      model name, scenario name, *and* version for any scenarios.
      These **must** allow a reader to distinguish between ‘main’
      or meaningful scenarios and other extras that should not be used.
    - Specific external databases, Scenario Explorer instances, etc.
12. A section on *data sources*,
    including references to code used to prepare data.
13. A section on *structure and parametrization* of the scenario(s)
    created and used in the project.

    This **should** include, in particular, any differences
    from the ‘base’ scenarios (11, above),
    and **should** allow quick/at-a-glance understanding
    of the model configuration used for a completed project.
    These can be described *directly*, or by *reference*;
    for the latter, write “same as <other project>”
    and add a ReST link to a full description elsewhere.

    - Spatial scope and resolution:
      i.e. which :doc:`pkg-data/node` is used.
    - Structure: members added to or removed from specific MESSAGE sets.
    - Names of specific MESSAGE parameters to which values are added or removed.
    - Specific functions in :mod:`message_ix_models` or other packages
      used to perform complex modifications to structure and data,
      including the configuration options passed these other codes.

14. A section describing the *workflow(s)* used in the project.

    This should describe *what* specific structure,
    parametrization, reporting, and other tasks are set up by the workflow(s)
    It should also include complete instructions to execute the workflows.
    This **may** include:

    - Versions of Python packages, other code, or data files known to work.

15. A “Code reference” section
    that uses Sphinx directives to (recursively) show the documentation
    of project-specific module(s) and their contents.
16. A section of *references* to other information.
    This **may** include relevant publications;
    add entries to :file:`doc/main.bib` and use the :code:`:cite:` ReST role.

Keep documentation up to date
=============================

The best practice is to **merge the documentation PR**
(with appropriate review)
as soon as you have added all information *currently* available.
As work on the project proceeds,
you can add to and revise the documentation page(s):

- as part of PRs that also add or modify code and data, or
- in dedicated, documentation-only PRs.

If you prefer,
you can keep open the initial documentation issue
as a place to collect this additional information.

