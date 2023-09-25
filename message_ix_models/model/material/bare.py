from functools import partial
from typing import Mapping
import logging

import message_ix

from message_ix_models import Code, ScenarioInfo, get_context, set_info, add_par_data
from .build import apply_spec
from .util import read_config
from .data import get_data, gen_data_steel, gen_data_generic, gen_data_aluminum, \
gen_data_variable, gen_data_petro_chemicals


log = logging.getLogger(__name__)


# Settings and valid values; the default is listed first
# How do we add the historical period ?

SETTINGS = dict(
    #period_start=[2010],
    period_start=[1980],
    first_model_year = [2020],
    period_end=[2100],
    regions=["China"],
    res_with_dummies=[True],
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
    mp.add_unit('Mt')

    # Model and scenario name for the RES
    model_name = context.scenario_info['model']
    scenario_name = context.scenario_info['scenario']

    # Create the Scenario
    scenario = message_ix.Scenario(mp, model=model_name,
                                   scenario=scenario_name, version='new')

    # TODO move to message_ix
    scenario.init_par('MERtoPPP', ['node', 'year'])

    # Uncomment to add dummy sets and data
    context.res_with_dummies = True

    spec = get_spec(context)
    apply_spec(
        scenario,
        spec,
        # data=partial(get_data, context=context, spec=spec),
        data=add_data,
        quiet=quiet,
        message=f"Create using message_ix_models {message_ix_models.__version__}",
    )

    return scenario


DATA_FUNCTIONS = [
    gen_data_steel,
    gen_data_generic,
    gen_data_aluminum,
    gen_data_variable
]

# Try to handle multiple data input functions from different materials
def add_data(scenario, dry_run=False):
    """Populate `scenario` with MESSAGE-Material data."""

    # Information about `scenario`
    info = ScenarioInfo(scenario)

    # Check for two "node" values for global data, e.g. in
    # ixmp://ene-ixmp/CD_Links_SSP2_v2.1_clean/baseline
    if {"World", "R11_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R11_GLB' from node list for data generation")
        info.set["node"].remove("R11_GLB")

    for func in DATA_FUNCTIONS:
        # Generate or load the data; add to the Scenario
        log.info(f'from {func.__name__}()')
        add_par_data(scenario, func(scenario), dry_run=dry_run)

    log.info('done')



def get_spec(context=None) -> Mapping[str, ScenarioInfo]:
    """Return the spec for the MESSAGE-Material bare RES.

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
    # GU: Remove world is already specified in set.yaml.
    remove = ScenarioInfo()
    # remove.set["node"] = context["material"]["common"]["region"]["remove"]

    add = ScenarioInfo()

    # Add technologies
    # JM: try to find out a way to loop over 1st/2nd level and to just context["material"][xx]["add"]
    add.set["technology"] = context["material"]["steel"]["technology"]["add"] + \
        context["material"]["generic"]["technology"]["add"] + \
        context["material"]["aluminum"]["technology"]["add"] + \
        context["material"]["petro_chemicals"]["technology"]["add"]

    # Add regions

    # # Load configuration for the specified region mapping
    # nodes = set_info(f"node/{context.regions}")
    #
    # # Top-level "World" node
    # world = nodes[nodes.index("World")]

    # Set elements: World, followed by the direct children of World
    add.set["node"] = context["material"]["common"]["region"]["require"]

    add.set["relation"] = context["material"]["steel"]["relation"]["add"]

    # Add the time horizon
    add.set['year'] = list(range(
        context.period_start, context.period_end + 1, context.time_step
    ))

    # JM: Leave the first time period as historical year
    #add.set['cat_year'] = [('firstmodelyear', context.period_start + context.time_step)]

    # GU: Set 2020 as the first model year, leave the rest as historical year
    add.set['cat_year'] = [('firstmodelyear', context.first_model_year)]

    # Add levels
    # JM: For bare model, both 'add' & 'require' need to be added.
    add.set['level'] = context["material"]["steel"]["level"]["add"] + \
        context["material"]["common"]["level"]["require"] + \
        context["material"]["generic"]["level"]["add"] + \
        context["material"]["aluminum"]["level"]["add"] +\
        context["material"]["petro_chemicals"]["level"]["add"]

    # Add commodities
    add.set['commodity'] = context["material"]["steel"]["commodity"]["add"] + \
        context["material"]["common"]["commodity"]["require"] + \
        context["material"]["generic"]["commodity"]["add"] + \
        context["material"]["aluminum"]["commodity"]["add"]
        context["material"]["petro_chemicals"]["commodity"]["add"]

    # Add other sets

    add.set['type_tec'] = context["material"]["common"]["type_tec"]["add"]
    add.set['mode'] = context["material"]["common"]["mode"]["require"] +\
        context["material"]["generic"]["mode"]["add"] + \
        context["material"]["petro_chemicals"]["mode"]["add"]

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

    # JM: Manually set the first model year
    add.y0 = context.first_model_year

    if context.res_with_dummies:
        # Add dummy technologies
        add.set["technology"].extend([Code("dummy"), Code("dummy source")])
        # Add a dummy commodity
        add.set["commodity"].append(Code("dummy"))

    spec['add'] = add
    spec['remove'] = remove
    return spec
