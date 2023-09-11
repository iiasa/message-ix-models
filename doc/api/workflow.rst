Multi-scenario workflows (:mod:`.workflow`)
*******************************************

.. contents::
   :local:
   :backlinks: none

Concept & design
================

Research with MESSAGEix models often involves multiple scenarios that are related to one another or derived from one another by certain modifications.
Together, the solutions/reported information from these scenarios provide the output data used in research products, e.g. a plot comparing total emissions in a policy scenario to a reference scenario.

:mod:`.model.build` provides tools to build models or scenarios based on (possibly empty) base scenarios; and :mod:`~message_ix_models.tools` provides tools for manipulating scenarios or model input data (parameters).
The :class:`.Workflow` API provided in this module allows researchers to use these pieces and atomic, reusable functions to define arbitrarily complex workflows involving many, related scenarios; and then to solve, report, or otherwise operate on those scenarios.

The generic pattern for workflows is:

- Each scenario has zero or 1 (or more?) base/precursor/antecedent scenarios.
  These must exist before the target scenario can be created.
- A workflow ‘step’ includes:

  1. A precursor scenario is obtained.

     It may be returned by a prior workflow step, or loaded from a :class:`~ixmp.Platform`.
  2. (Optional) The precursor scenario is cloned to a target model name and scenario name.
  3. A function is called to operate on the scenario.
     This function may do zero or more of:

     - Apply structure or data modifications, for example:

       - Set up a model variant, e.g. adding the MESSAGEix-Materials structure to a base MESSAGEix-GLOBIOM model.
       - Change policy variables via constraint parameters.
       - Any other possible modification.

     - Solve the target scenario.
     - Invoke reporting.
  4. The resulting function is passed to the next workflow step.

- A workflow can consist of any number of scenarios and steps.
- The same precursor scenario can be used as the basis for multiple target scenarios.
- A workflow is :meth:`.Workflow.run` starting with the earliest precursor scenario, ending with 1 or many target scenarios.

The implementation is based on the observation that these form a graph (specifically, a directed, acyclic graph, or DAG) of nodes (= scenarios) and edges (= steps), in the same way that :mod:`message_ix.reporting` calculations do; and so the :mod:`dask` DAG features (via :mod:`genno`) can be used to organize the workflow.

Usage
=====

General
-------

Define a workflow using ordinary Python functions, each handling the modifications/manipulations in an atomic workflow step.
These functions **must**:

- Accept at least 2 arguments:

  1. A :class:`.Context` instance.
  2. The precursor scenario.
  3. Optionally additional, keyword-only arguments.

- Return either:

  - a :class:`~.message_ix.Scenario` object, that can be the same object provided as an argument, or a different scenario, e.g. a clone or a different scenario, even from a different platform.
  - :class:`None`.
    In this case, any modifications implemented by the step should be reflected in the Scenario given as an argument.

The functions **may**:

- call any other code, and
- be as short (one line) or long (many lines) as desired;

and they **should**:

- respond in documented, simple ways to settings on the Context argument and/or their keyword argument(s), if any.

.. code-block:: python

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
Call :meth:`.Workflow.add_step` to define each target model with its precursor and the function that will create the former from the latter:

.. code-block:: python

    from message_ix_models import Context, Workflow

    # Create the workflow
    ctx = Context.get_instance()
    wf = Workflow(ctx)

    # "Model name/base" is loaded from an existing platform
    wf.add_step(
        "base",
        None,
        target="ixmp://example-platform/Model name/base#123",
    )

    # "Model/A" is created from "Model/base" by calling changes_a()
    wf.add_step("A", "base", changes_a, target="Model/A")

    # "Model/B1" is created from "Model/A" by calling changes_b() with the
    # default value
    wf.add_step("B1", "A", changes_b, target="Model/B1")

    # "Model/B2" is similar, but uses a different value
    wf.add_step("B2", "A", partial(changes_b, value=200.0), target="model/B2")

Finally, the workflow is triggered using :meth:`.Workflow.run`, giving either one step name or a list of names.
The indicated scenarios are created (and solved, if the workflow steps involve solving); if this requires any precursor scenarios, those are first created and solved, etc. as required.
Other, unrelated scenarios/steps are not created.

.. code-block:: python

    s1, s2 = wf.run(["B1", "B2"])

Usage examples
--------------

- :mod:`message_data.projects.navigate.workflow`

.. todo::

   Expand with discussion of workflow patterns common in research projects using MESSAGEix, e.g.:

   - Run the same scenario with multiple emissions budgets.

API reference
=============

.. currentmodule:: message_ix_models.workflow

.. automodule:: message_ix_models.workflow
   :members:
   :exclude-members: WorkflowStep

.. autoclass:: WorkflowStep
   :members:
   :special-members: __call__
