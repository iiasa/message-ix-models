from functools import partial
from typing import Mapping
import logging

import message_ix

from message_data.tools import Code, ScenarioInfo, get_context, set_info
from .build import apply_spec
from .util import read_config
from .data import get_data, gen_data_steel
import message_data


log = logging.getLogger(__name__)


# Settings and valid values; the default is listed first
SETTINGS = dict(
    period_start=[2010],
    period_end=[2100],
    regions=["China"],
    res_with_dummies=[False],
    time_step=[10],
)


def create_res(context=None, quiet=True):
    """Create a 'bare' MESSAGE-GLOBIOM reference energy system (RES).

    Parameters
    ----------
    context : .Context
        :attr:`.Context.scenario_info`  determines the model name and scenario
        name of the created Scenario.

    Returns
    -------
    message_ix.Scenario
        A scenario as described by :func:`get_spec`, prepared using
        :func:`.build.apply_spec`.
    """
    mp = context.get_platform()

    # Model and scenario name for the RES
    model_name = context.scenario_info['model']
    scenario_name = context.scenario_info['scenario']

    # Create the Scenario
    scenario = message_ix.Scenario(mp, model=model_name,
                                   scenario=scenario_name, version='new')

    # TODO move to message_ix
    scenario.init_par('MERtoPPP', ['node', 'year'])

    # Uncomment to add dummy sets and data
    # context.res_with_dummies = True

    spec = get_spec(context)
    apply_spec(
        scenario,
        spec,
        # data=partial(get_data, context=context, spec=spec),
        data=gen_data_steel,
        quiet=quiet,
        message=f"Create using message_data {message_data.__version__}",
    )

    return scenario


def get_spec(context=None) -> Mapping[str, ScenarioInfo]:
    """Return the spec for the MESSAGE-China bare RES.

    Parameters
    ----------
    context : Context, optional
        If not supplied, :func:`.get_context` is used to retrieve the current
        context.

    Returns
    -------
    :class:`dict` of :class:`.ScenarioInfo` objects
    """
    # context = context or get_context(strict=True)
    context = read_config()
    context.use_defaults(SETTINGS)

    # The RES is the base, so does not require/remove any elements
    spec = dict(require=ScenarioInfo())

    # JM: For China model, we need to remove the default 'World'.
    remove = ScenarioInfo()
    # remove.set["node"] = context["material"]["common"]["region"]["remove"]

    add = ScenarioInfo()


    # Add technologies
    # JM: try to find out a way to loop over 1st/2nd level and to just context["material"][xx]["add"]
    add.set["technology"] = context["material"]["steel"]["technology"]["add"]

    # Add regions

    # # Load configuration for the specified region mapping
    # nodes = set_info(f"node/{context.regions}")
    #
    # # Top-level "World" node
    # world = nodes[nodes.index("World")]

    # Set elements: World, followed by the direct children of World
    add.set["node"] = context["material"]["common"]["region"]["require"]

    # Add the time horizon
    add.set['year'] = list(range(
        context.period_start, context.period_end + 1, context.time_step
    ))
    add.set['cat_year'] = [('firstmodelyear', context.period_start)]

    # Add levels
    # JM: For bare model, both 'add' & 'require' need to be added.
    add.set['level'] = context["material"]["steel"]["level"]["add"] + \
        context["material"]["common"]["level"]["require"]

    # Add commodities
    c_list = context["material"]["steel"]["commodity"]["add"] + \
        context["material"]["common"]["commodity"]["require"]
    add.set['commodity'] = c_list

    add.set['type_tec'] = context["material"]["common"]["type_tec"]["add"]
    add.set['mode'] = context["material"]["common"]["mode"]["require"]
    add.set['emission'] = context["material"]["common"]["emission"]["require"] +\
        context["material"]["common"]["emission"]["add"]

    # Add units, associated with commodities
    # JM: What is 'anno'
    # for c in c_list:
    #     try:
    #         unit = c.anno['unit']
    #     except KeyError:
    #         log.warning(f"Commodity {c} lacks defined units")
    #         continue
    #
    #     try:
    #         # Check that the unit can be parsed by the pint.UnitRegistry
    #         context.units(unit)
    #     except Exception:
    #         log.warning(f"Unit {unit} for commodity {c} not pint compatible")
    #     else:
    #         add.set['unit'].append(unit)

    # Deduplicate by converting to a set and then back; not strictly necessary,
    # but reduces duplicate log entries
    add.set['unit'] = sorted(set(add.set['unit']))

    # Manually set the first model year
    add.y0 = context.period_start

    if context.res_with_dummies:
        # Add dummy technologies
        add.set["technology"].extend([Code("dummy"), Code("dummy source")])
        # Add a dummy commodity
        add.set["commodity"].append(Code("dummy"))

    spec['add'] = add
    spec['remove'] = remove
    return spec
