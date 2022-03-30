Multi-scenario workflows (:mod:`.workflow`)
*******************************************

.. contents::
   :local:
   :backlinks: none

Concept & design
================

Research with MESSAGEix models often involves multiple scenarios that are related to one another or derived from one another by certain modifications.
Together, the solutions/reported information from these scenarios provide the output data used in research products, e.g. a plot comparing total emissions in a policy scenario to a reference scenario.

:mod:`.model.build` provides tools to build models or scenarios based on (possibly empty) base scenarios; and :mod:`.tools` provides tools for manipulating scenarios or model input data (parameters).
The :class:`.Workflow` API provided in this module allows researchers to use these pieces and atomic, reusable functions to define arbitrarily complex workflows involving many, related scenarios; and then to solve, report, or otherwise operate on those scenarios.

The generic pattern for workflows is:

- Each scenario has zero or 1 (or more?) base/precursor/antecedent scenarios.
  These must exist before the target scenario can be created.
- A workflow ‘step’ includes:

  - The precursor scenario is cloned to the target scenario name.
  - Modifications are applied.
    These can be modifications to structure or to data. For example:

    - Setting up a model variant, e.g. adding the MESSAGEix-Materials structure to a base MESSAGEix-GLOBIOM model.
    - Changing policy variables via constraint parameters.
    - Any other possible modification.

  - The target scenario is optionally solved.
  - The target scenario is optionally reported.

- A workflow can consist of any number of scenarios and steps.
- The same precursor scenario can be used as the basis for multiple target scenarios.
- A workflow is ‘run’ starting with the earliest precursor scenario, ending with 1-to-many target scenarios.

The implementation is based on the observation that these form a graph (DAG) of nodes (scenarios) and edges (steps), in the same way that :mod:`message_ix.reporting` calculations do; and so the :mod:`dask` DAG features (via :mod:`genno`) can be used to organize the workflow.

Usage
=====

General
-------

Define a workflow using ordinary Python functions, each handling the modifications/manipulations in a single, atomic workflow step.
These functions they **must**:

- Accept 1 argument: either the precursor scenario, or :obj:`None`.
- Return either:

  - a :class:`.Scenario` object: required if the argument is :obj:`None`.
  - :class:`None`. In this case, the modifications are reflected in the :class:`.Scenario` given as an argument.

The functions **may** call any other code, and can be from one to many lines; they **should** be reusable, i.e. respond in simple and obvious ways to a few clearly-defined arguments.

.. code-block:: python

    def base_scenario(arg=None) -> Scenario:
        """Generate a base scenario."""
        return testing.bare_res(request, test_context, solved=False)

    def changes_a(s: Scenario) -> None:
        """Change a scenario by modifying structure data, but not data."""
        with s.transact():
            s.add_set("technology", "test_tech")

        # Here, invoke other code to further modify `s`

    def changes_b(s: Scenario, value=100.0) -> None:
        """Change a scenario by modifying parameter data, but not structure.

        This function takes an extra argument, `values`, so functools.partial()
        can be used to supply different values when it is used in different
        workflow steps. See below.
        """
        with s.transact():
            s.add_par(
                "technical_lifetime",
                make_df(
                    "technical_lifetime",
                    node_loc=s.set("node")[0],
                    year_vtg=s.set("year")[0],
                    technology="test_tech",
                    value=100.0,
                    unit="y",
                ),
            )

        # Here, invoke other code to further modify `s`

With the steps defined, the workflow is composed using a :class:`.Workflow` instance.
Call :meth:`.Workflow.add` to define each target model with its precursor and the function that will create the former from the latter:

.. code-block:: python

    from message_ix_models import Context, Workflow

    # Create the workflow
    ctx = Context.get_instance()
    wf = Workflow(ctx)

    # "Model/base" is created from nothing by calling base_scenario()
    wf.add("Model/base", None, base_scenario)

    # "Model/A" is created from "Model/base" by calling changes_a()
    wf.add("Model/A", "Model/base", changes_a)

    # "Model/B1" is created from "Model/A" by calling changes_b() with the
    # default value
    wf.add("Model/B1", "Model/A", changes_b)

    # "Model/B2" is similar, but uses a different value
    wf.add("Model/B2", "Model/A", partial(changes_b, value=200.0))

Finally, the workflow is triggered using :meth:`.Workflow.run`, giving either one scenario identifier or a list of identifiers.
The indicated scenarios are created and solved; if this requires any precursor scenarios, those are first created and solved, etc. as required.
Other, unrelated scenarios/steps are not created.

.. code-block:: python

    s1, s2 = wf.run(["Model/B1", "Model/B2"])

Common use cases
----------------

.. todo::

   Expand with discussion of workflow patterns common in research projects using MESSAGEix, e.g.:

   - Run the same scenario with multiple emissions budgets.

API reference
=============

.. currentmodule:: message_ix_models.workflow

.. automodule:: message_ix_models.workflow
   :members:
