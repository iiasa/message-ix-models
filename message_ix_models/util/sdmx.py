"""Utilities for handling objects from :mod:`sdmx`."""
import logging
from copy import copy
from typing import Dict, List, Mapping, Union

from iam_units import registry
from sdmx.model import AnnotableArtefact, Annotation, Code, InternationalString

log = logging.getLogger(__name__)

CodeLike = Union[str, Code]


def as_codes(data: Union[List[str], Dict[str, CodeLike]]) -> List[Code]:
    """Convert *data* to a :class:`list` of :class:`.Code` objects.

    Various inputs are accepted:

    - :class:`list` of :class:`str`.
    - :class:`dict`, in which keys are :attr:`.Code.id` and values are further
      :class:`dict` with keys matching other :class:`.Code` attributes.
    """
    # Assemble results as a dictionary
    result: Dict[str, Code] = {}

    if isinstance(data, list):
        # FIXME typing ignored temporarily for PR#9
        data = dict(zip(data, data))  # type: ignore [arg-type]
    elif not isinstance(data, Mapping):
        raise TypeError(data)

    for id, info in data.items():
        # Pass through Code; convert other types to dict()
        if isinstance(info, Code):
            result[info.id] = info
            continue
        elif isinstance(info, str):
            _info = dict(name=info)
        elif isinstance(info, Mapping):
            _info = copy(info)
        else:
            raise TypeError(info)

        # Create a Code object
        code = Code(
            id=str(id),
            name=_info.pop("name", str(id).title()),
        )

        # Store the description, if any
        try:
            code.description = InternationalString(value=_info.pop("description"))
        except KeyError:
            pass

        # Associate with a parent
        try:
            parent_id = _info.pop("parent")
        except KeyError:
            pass  # No parent
        else:
            result[parent_id].append_child(code)

        # Associate with any children
        for id in _info.pop("child", []):
            try:
                code.append_child(result[id])
            except KeyError:
                pass  # Not parsed yet

        # Convert other dictionary (key, value) pairs to annotations
        for id, value in _info.items():
            code.annotations.append(
                Annotation(id=id, text=value if isinstance(value, str) else repr(value))
            )

        result[code.id] = code

    return list(result.values())


def eval_anno(obj: AnnotableArtefact, id: str):
    """Retrieve the annotation `id` from `obj`, run :func:`eval` on its contents.

    This can be used for unpacking Python values (e.g. :class:`dict`) stored as an
    annotation on a :class:`~sdmx.model.Code`.

    Returns :obj:`None` if no attribute exists with the given `id`.
    """
    try:
        value = str(obj.get_annotation(id=id).text)
    except KeyError:  # No such attribute
        return None

    try:
        return eval(value, {"registry": registry})
    except Exception as e:  # Something that can't be eval()'d, e.g. a plain string
        log.debug(f"Could not eval({value!r}): {e}")
        return value
