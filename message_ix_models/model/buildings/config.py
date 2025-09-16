# message_ix_models/model/buildings/config.py
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for MESSAGEix-Buildings
    (moving cli options to context so that build buildings can be called
    in other workflows).

    This dataclass stores and documents all configuration settings required and used by
    :mod:`~message_ix_models.model.buildings`. It also handles (via
    :meth:`.from_context`) loading configuration and values from files like
    :file:`config.yaml`, while respecting higher-level configuration, for instance
    :attr:`.model.Config.regions`.
    """

    with_materials: bool = True

    # @classmethod
    # def from_context(cls, context, options=None):
    #     config = cls()
    #     if options:
    #         config = replace(config, **options) # type: ignore
    #     return config
