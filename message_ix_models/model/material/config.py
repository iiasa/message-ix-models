# message_ix_models/model/material/config.py
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for MESSAGEix-Materials
    (moving cli options to context so that build material can be called in other
    workflows).

    This dataclass stores and documents all configuration settings required and used by
    :mod:`~message_ix_models.model.material`. It also handles (via
    :meth:`.from_context`) loading configuration and values from files like
    :file:`config.yaml`, while respecting higher-level configuration, for instance
    :attr:`.model.Config.regions`.
    """

    old_calib: bool = False
    iea_data_path: str = "P:ene.model\\IEA_database\\Florian\\"
    modify_existing_constraints: bool = True  # hardcoded to True

    @classmethod
    def from_context(cls, context, options=None):
        """Configure `context` for building MESSAGEix-Materials.

        :py:`context.material` is updated with configuration values from this Config
        class.
        """
        from dataclasses import replace

        config = cls()
        if options:
            config = replace(config, **options)
        return config
