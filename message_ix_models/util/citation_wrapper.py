import functools
from datetime import datetime

_CITATION_REGISTRY = {}


# New function to allow external registration
def register_citation_entry(
    key: str,
    citation: str,
    doi: str,
    description: str | None,
    metadata: dict | None,
    date: str,
):
    """Register a citation entry directly into the _CITATION_REGISTRY."""
    if key in _CITATION_REGISTRY:
        # Handle potential key collisions if necessary
        print(f"Warning: Citation key '{key}' already exists. Overwriting.")
    _CITATION_REGISTRY[key] = {
        "citation": citation,
        "doi": doi,
        "description": description,
        "metadata": metadata or {},
        "date": date,
    }


def citation_wrapper(
    citation: str, doi: str, description=None, metadata=None, **kwargs
):
    """Decorator to attach citation, doi, description, and metadata to a function."""

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
            register_citation_entry(
                key=obj.__name__,
                citation=citation,
                doi=doi,
                description=description,
                metadata=wrapped.metadata,
                date=wrapped.date,
            )
            return wrapped
        else:
            raise TypeError(
                "citation_wrapper is now only for decorating functions. "
                "For data, use Constants class which handles its own citation."
            )

    return decorator


def list_cited_functions(filter_by=None):
    """List all cited entities (functions and constants),
    optionally filtered by metadata."""
    if filter_by:
        return {
            k: v
            for k, v in _CITATION_REGISTRY.items()
            if filter_by.items() <= v.items()
        }
    return _CITATION_REGISTRY
