from copy import copy
from functools import partial
import logging

from ixmp.reporting.utils import Quantity
from message_ix.reporting import Key, Reporter, configure
from message_ix.reporting.computations import write_report
from message_ix.reporting.computations import concat
import yaml

from . import computations
from .computations import combine, group_sum
from .util import collapse, infer_keys


log = logging.getLogger(__name__)


def prepare_reporter(scenario, config, key, output_path):
    """Prepare to report *key* from *scenario*.

    Parameters
    ----------
    scenario : ixmp.Scenario
        MESSAGE-GLOBIOM scenario containing a solution, to be reported.
    config : dict-like
        Reporting configuration.
    key : str or ixmp.reporting.Key
        Quantity or node to compute. The computation is not triggered (i.e.
        :meth:`get <ixmp.reporting.Reporter.get>` is not called); but the
        corresponding, full-resolution Key is returned.
    output_path : os.Pathlike
        If given, a computation ``cli-output`` is added to the Reporter which
        writes *key* to this path.

    Returns
    -------
    ixmp.reporting.Reporter
        Reporter prepared with MESSAGE-GLOBIOM calculations.
    ixmp.reporting.Key
        Same as *key*, in full resolution, if any.

    """
    # Apply global reporting configuration, e.g. unit definitions
    configure(config)

    log.info('Preparing reporter')

    # Create a Reporter for *scenario* and apply Reporter-specific config
    rep = Reporter.from_scenario(scenario) \
                  .configure(config)

    # Load the YAML configuration as a dict
    with open(config, 'r') as f:
        config = yaml.load(f)

    # -------------------------------------------------------------------------
    # NB Add code between these --- lines to expand the reporting for
    #    MESSAGEix-GLOBIOM:
    #    - Modify the reporter directly with add() or apply().
    #    - Create a function like add_quantities() below and call it.
    #    - Add to the file data/global.yaml, and then read its keys from the
    #      variable *config*.

    # Mapping of file sections to handlers
    sections = (
        ('aggregate', add_aggregate),
        ('combine', add_combination),
        ('iamc', add_iamc_table),
        ('report', add_report),
        ('general', add_general),
        )

    for section_name, func in sections:
        entries = config.get(section_name, [])

        # Handle the entries, if any
        if len(entries):
            log.info(f'--- {section_name!r} config section')
        for entry in entries:
            func(rep, entry)

    # -------------------------------------------------------------------------

    # If needed, get the full key for *quantity*
    key = rep.check_keys(key)[0]

    if output_path:
        # Add a new computation that writes *key* to the specified file
        rep.add('cli-output', (partial(write_report, path=output_path), key))
        key = 'cli-output'

    log.info('…done')

    return rep, key


def add_aggregate(rep, info):
    """Add items from the 'aggregates' tree in the config file."""
    # Copy for destructive .pop()
    info = copy(info)

    quantities = info.pop('_quantities')
    tag = info.pop('_tag')
    groups = {info.pop('_dim'): info}

    for qty in quantities:
        keys = rep.aggregate(qty, tag, groups, sums=True)

        log.info(f'Add {keys[0]!r} + {len(keys)-1} partial sums')


def add_combination(rep, info):
    """Add items from the 'combine' tree in the config file."""
    # Split inputs into three lists
    quantities, select, weights = [], [], []

    # Key for the new quantity
    key = Key.from_str_or_key(info['key'])

    # Loop over inputs to the combination
    for i in info['inputs']:
        # Required dimensions for this input: output key's dims, plus any
        # dims that must be selected on
        dims = set(key.dims) | set(i['select'].keys())
        quantities.append(infer_keys(rep, i['quantity'], dims))

        select.append(i['select'])
        weights.append(i['weight'])

    # Check for malformed input
    assert len(quantities) == len(select) == len(weights)

    # Computation
    c = tuple([partial(combine, select=select, weights=weights)] + quantities)

    added = rep.add(key, c, strict=True, index=True, sums=True)

    log.info(f"Add {key}\n  computed from {quantities!r}\n  + {len(added)-1} "
             "partial sums")


def add_iamc_table(rep, info):
    """Add IAMC tables from the 'iamc' tree in the config file."""
    # For each quantity, use a chain of computations to prepare it
    name = info['variable']

    # Chain of keys produced: first entry is the key for the base quantity
    keys = [Key.from_str_or_key(info['base'])]

    if 'select' in info:
        # Select a subset of data from the base quantity
        key = Key(name, keys[-1]._dims)
        rep.add(key, (Quantity.sel, keys[-1], info['select']), strict=True)
        keys.append(key)

    if 'group_sum' in info:
        # Aggregate data by groups
        args = dict(group=info['group_sum'][0], sum=info['group_sum'][1])
        key = Key.from_str_or_key(keys[-1], tag='agg')
        rep.add(key, (partial(group_sum, **args), keys[-1]), strict=True)
        keys.append(key)

    # 'format' section: convert the data to a pyam data structure

    # Copy for destructive .pop() operation
    args = copy(info['format'])
    args['var_name'] = name

    drop = set(args.pop('drop', [])) & set(keys[-1]._dims)

    key = f'{name}:iamc'
    rep.as_pyam(keys[-1], 'ya', key, drop=drop,
                collapse=partial(collapse, **args))
    keys.append(key)

    # Revise the 'message:default' report to include the last key in
    # the chain
    rep.add('message:default',
            rep.graph['message:default'] + (keys[-1],))

    log.info(f'Add {name!r} from {keys[0]!r}\n  keys {keys[1:]!r}')


def add_report(rep, info):
    """Add items from the 'report' tree in the config file."""
    log.info(f"Add {info['key']!r}")

    # Concatenate pyam data structures
    rep.add(info['key'], tuple([concat] + info['members']), strict=True)


def add_general(rep, info):
    """Add items from the 'general' tree in the config file.

    This is, as the name implies, the most generalized section of the config
    file. Entry *info* must contain:

    - 'comp': this refers to the name of a computation that is available in the
      namespace of message_data.reporting.computations.
    - 'args': (optional) keyword arguments to the computation.
    - 'inputs': a list of keys to which the computation is applied.
    - 'key': the key for the computed quantity.
    """
    log.info(f"Add {info['key']!r}")

    # Retrieve the function for the computation
    f = getattr(computations, info['comp'])
    kwargs = info.get('args', {})
    inputs = infer_keys(rep, info['inputs'])
    task = tuple([partial(f, **kwargs)] + inputs)

    rep.add(info['key'], task, strict=True)
