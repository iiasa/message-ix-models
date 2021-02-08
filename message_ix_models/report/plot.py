import logging

import matplotlib
import plotnine as p9

log = logging.getLogger(__name__)


try:
    matplotlib.use("cairo")
except ImportError:
    log.info(
        f"'cairo' not available; using {matplotlib.get_backend()} matplotlib backend"
    )


class Plot:
    """Class for reporting plots.

    To use this class:

    1. Create a subclass that overrides :attr:`name`, :attr:`inputs`, and
       :meth:`generate`.

    2. Call :meth:`computation` to get a tuple (callable, followed by key
       names) suitable for adding to a Reporter::

         rep.add("foo", P.computation())
    """

    #: Path fragments for output.
    path = []
    #: Filename base for saving the plot.
    name = ""
    #: Keys for reporting quantities needed by :meth:`generate`.
    inputs = []
    #: Keyword arguments for :meth:`plotnine.ggplot.save`.
    save_args = dict(verbose=False)

    # TODO add static geoms automatically in generate()
    __static = []

    def __call__(self, config, *args):
        path = config["report_path"].joinpath(*self.path, f"{self.name}.pdf")
        log.info(f"Generate {path}")
        path.parent.mkdir(parents=True, exist_ok=True)

        plot_or_plots = self.generate(*args)

        try:
            # Single plot
            plot_or_plots.save(path, **self.save_args)
        except AttributeError:
            # Iterator containing multiple plots
            p9.save_as_pdf_pages(plot_or_plots, path, **self.save_args)

        return path

    @classmethod
    def computation(cls):
        """Return a computation :class:`tuple` to add to a Reporter."""
        return tuple([cls(), "config"] + cls.inputs)

    def generate(*args):
        """Generate and return the plot."""
        return p9.ggplot(*args)
