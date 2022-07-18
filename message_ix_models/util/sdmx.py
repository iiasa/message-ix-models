from iam_units import registry
from sdmx.model import AnnotableArtefact


def eval_anno(obj: AnnotableArtefact, id: str):
    """Retrieve the annotation `id` from `obj`, run :func:`eval` on its contents.

    This can be used for unpacking Python values (e.g. :class:`dict`) stored as an
    annotation on a :class:`~sdmx.model.Code`.

    Returns :obj:`None` if no attribute exists with the given `id`.
    """
    try:
        value = str(obj.get_annotation(id=id).text)
    except KeyError:
        # No such attribute
        return None

    try:
        return eval(value, {"registry": registry})
    except Exception as e:
        print(e, repr(value))
        # Something that can't be eval()'d, e.g. a string
        return value
