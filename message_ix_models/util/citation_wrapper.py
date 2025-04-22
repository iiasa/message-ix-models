import functools
from collections import UserDict, UserList
from datetime import datetime

_CITATION_REGISTRY = {}

# New wrapper class for dicts with citation metadata
class CitationWrappedDict(UserDict):
    def __init__(self, orig, citation, doi, description=None, metadata=None, **kwargs):
        super().__init__(orig)
        self.citation = citation
        self.doi = doi
        self.description = description
        self.metadata = metadata or {}
        self.date = datetime.now().strftime("%Y-%m-%d")
        for k, v in kwargs.items():
            setattr(self, k, v)

# New wrapper class for lists with citation metadata
class CitationWrappedList(UserList):
    def __init__(self, orig, citation, doi, description=None, metadata=None, **kwargs):
        super().__init__(orig)
        self.citation = citation
        self.doi = doi
        self.description = description
        self.metadata = metadata or {}
        self.date = datetime.now().strftime("%Y-%m-%d")
        for k, v in kwargs.items():
            setattr(self, k, v)

def citation_wrapper(
    citation: str, doi: str, description=None, metadata=None, **kwargs):
    """Decorator/wrapper to attach citation, doi, description, and metadata to a
    function or container.

    If applied to a function the decorator behaves as before.
    When applied to a dict or list (via a functional call rather than decorator syntax),
    the original container is wrapped (using UserDict or UserList) so that it behaves
    like the original container while exposing additional metadata attributes.
    """
    def decorator(obj):
        if callable(obj):
            @functools.wraps(obj)
            def wrapped(*args, **inner_kwargs):
                return obj(*args, **inner_kwargs)
            wrapped.citation = citation
            wrapped.doi = doi
            wrapped.description = description
            wrapped.metadata = metadata or {}
            wrapped.date = datetime.now().strftime("%Y-%m-%d")
            for k, v in kwargs.items():
                setattr(wrapped, k, v)
            # Register the function using its name
            _CITATION_REGISTRY[obj.__name__] = {
                "citation": citation,
                "description": description,
                "metadata": wrapped.metadata,
                "date": wrapped.date,
            }
            return wrapped
        elif isinstance(obj, dict):
            return CitationWrappedDict(
                obj, citation, doi, description, metadata, **kwargs)
        elif isinstance(obj, list):
            return CitationWrappedList(
                obj, citation, doi, description, metadata, **kwargs)
        else:
            # Fallback: wrap any other object with a generic wrapper.
            class GenericWrapper:
                def __init__(self, wrapped):
                    self.wrapped = wrapped
                def __getattr__(self, attr):
                    return getattr(self.wrapped, attr)
            wrapper = GenericWrapper(obj)
            wrapper.citation = citation
            wrapper.doi = doi
            wrapper.description = description
            wrapper.metadata = metadata or {}
            wrapper.date = datetime.now().strftime("%Y-%m-%d")
            for k, v in kwargs.items():
                setattr(wrapper, k, v)
            return wrapper
    return decorator

def list_cited_functions(filter_by=None):
    """List all functions with citations, optionally filtered by metadata."""
    if filter_by:
        return {
            k: v
            for k, v in _CITATION_REGISTRY.items()
            if filter_by.items() <= v.items()
        }
    return _CITATION_REGISTRY
