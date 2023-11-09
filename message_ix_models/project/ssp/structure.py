"""Manipulate data structures for working with the SSPs."""
import logging
from textwrap import wrap
from typing import TYPE_CHECKING, Optional

import sdmx
import sdmx.model.v30 as m
import sdmx.urn

from message_ix_models.util.sdmx import make_enum, write

if TYPE_CHECKING:
    from os import PathLike

    from message_ix_models import Context

log = logging.getLogger(__name__)


DESC = """
This code list is *not* officially published by ICONICS; rather, it is a rendering into
SDMX of structural information that is provided by ICONICS participants. It may change
or be superseded at any time.

Each code has a single digit ID like "1"; the "original-id" annotation (and the start of
the name) give a string like "SSP1". These original IDs are *only* unique if using data
enumerated by solely by one code list or the other; if mixing the two, then they will be
ambiguous. The URNs of the codes or parts thereof (for instance, "ICONICS:SSP(2017).1")
are, by construction, unique.
"""

CL_INFO = (
    dict(
        version="2017",
        name_extra="2017 edition",
        desc_extra="This is the original set of SSPs as described in"
        " https://doi.org/10.1016/j.gloenvcha.2016.05.009.",
    ),
    dict(
        version="2024",
        name_extra="2024 edition",
        desc_extra="This set of SSPs is currently under development; for details, see "
        "https://depts.washington.edu/iconics/.",
    ),
)

CODE_INFO = (
    dict(
        id="1",
        name="Sustainability",
        name_long="Sustainability – Taking the Green Road",
        description="""Low challenges to mitigation and adaptation.

The world shifts gradually, but pervasively, toward a more sustainable path, emphasizing
more inclusive development that respects perceived environmental boundaries. Management
of the global commons slowly improves, educational and health investments accelerate the
demographic transition, and the emphasis on economic growth shifts toward a broader
emphasis on human well-being. Driven by an increasing commitment to achieving
development goals, inequality is reduced both across and within countries. Consumption
is oriented toward low material growth and lower resource and energy intensity.""",
    ),
    dict(
        id="2",
        name="Middle of the Road",
        name_long="Middle of the Road",
        description="""Medium challenges to mitigation and adaptation.

The world follows a path in which social, economic, and technological trends do not
shift markedly from historical patterns. Development and income growth proceeds
unevenly, with some countries making relatively good progress while others fall short of
expectations. Global and national institutions work toward but make slow progress in
achieving sustainable development goals. Environmental systems experience degradation,
although there are some improvements and overall the intensity of resource and energy
use declines. Global population growth is moderate and levels off in the second half of
the century. Income inequality persists or improves only slowly and challenges to
reducing vulnerability to societal and environmental changes remain.""",
    ),
    dict(
        id="3",
        name="Regional Rivalry",
        name_long="Regional Rivalry – A Rocky Road",
        description="""High challenges to mitigation and adaptation.

A resurgent nationalism, concerns about competitiveness and security, and regional
conflicts push countries to increasingly focus on domestic or, at most, regional issues.
Policies shift over time to become increasingly oriented toward national and regional
security issues. Countries focus on achieving energy and food security goals within
their own regions at the expense of broader-based development. Investments in education
and technological development decline. Economic development is slow, consumption is
material-intensive, and inequalities persist or worsen over time. Population growth is
low in industrialized and high in developing countries. A low international priority for
addressing environmental concerns leads to strong environmental degradation in some
regions.""",
    ),
    dict(
        id="4",
        name="Inequality",
        name_long="Inequality – A Road Divided",
        description="""Low challenges to mitigation, high challenges to adaptation.

Highly unequal investments in human capital, combined with increasing disparities in
economic opportunity and political power, lead to increasing inequalities and
stratification both across and within countries. Over time, a gap widens between an
internationally-connected society that contributes to knowledge- and capital-intensive
sectors of the global economy, and a fragmented collection of lower-income, poorly
educated societies that work in a labor intensive, low-tech economy. Social cohesion
degrades and conflict and unrest become increasingly common. Technology development is
high in the high-tech economy and sectors. The globally connected energy sector
diversifies, with investments in both carbon-intensive fuels like coal and
unconventional oil, but also low-carbon energy sources. Environmental policies focus on
local issues around middle and high income areas.""",
    ),
    dict(
        id="5",
        name="Fossil-fueled Development",
        name_long="Fossil-fueled Development – Taking the Highway",
        description="""High challenges to mitigation, low challenges to adaptation.

This world places increasing faith in competitive markets, innovation and participatory
societies to produce rapid technological progress and development of human capital as
the path to sustainable development. Global markets are increasingly integrated. There
are also strong investments in health, education, and institutions to enhance human and
social capital. At the same time, the push for economic and social development is
coupled with the exploitation of abundant fossil fuel resources and the adoption of
resource and energy intensive lifestyles around the world. All these factors lead to
rapid growth of the global economy, while global population peaks and declines in the
21st century. Local environmental problems like air pollution are successfully managed.
There is faith in the ability to effectively manage social and ecological systems,
including by geo-engineering if necessary.""",
    ),
)


def generate(context: "Context", base_dir: Optional["PathLike"] = None):
    """Generate SDMX code lists containing the SSPs."""
    # Create an AgencyScheme containing ICONICS
    as_ = m.AgencyScheme(
        id="AGENCIES",
        description="Agencies referenced by data structures in message_ix_models",
        version="0.1",
    )

    IIASA_ECE = m.Agency(
        id="IIASA_ECE", name="IIASA Energy, Climate, and Environment Program"
    )

    ICONICS = m.Agency(
        id="ICONICS",
        name="International Committee on New Integrated Climate Change Assessment "
        "Scenarios",
        contact=[m.Contact(uri=["https://depts.washington.edu/iconics/"])],
    )

    as_.maintainer = IIASA_ECE
    as_.append(IIASA_ECE)
    as_.append(ICONICS)

    if context.dry_run:
        log.info(f"(dry run) Would write:\n{repr(as_)}")
    else:
        write(as_, base_dir)

    for cl_info in CL_INFO:
        # Create the codelist: format the name and description
        cl: m.Codelist = m.Codelist(
            id="SSP",
            name=f"Shared Socioeconomic Pathways ({cl_info['name_extra']})",
            description="\n".join(
                wrap(DESC.strip(), width=len(DESC)) + ["", cl_info["desc_extra"]]
            ),
            version=cl_info["version"],
            maintainer=ICONICS,
        )

        # Add one Code for each SSP
        for info in CODE_INFO:
            # Construct the original ID
            original_id = f"SSP{info['id']}"

            # Format the name, description; add an annotation
            c = m.Code(
                id=info["id"],
                name=f"{original_id}: {info['name']}",
                description="\n".join(
                    [f"{original_id}: {info['name_long']}", "", info["description"]]
                ),
                annotations=[m.Annotation(id="original-id", text=original_id)],
            )

            # Append to the code list
            cl.append(c)

            # Construct a URN
            c.urn = sdmx.urn.make(c, maintainable_parent=cl)

        if context.dry_run:
            log.info(f"(dry run) Would write:\n{repr(cl)}")
            continue

        write(cl, base_dir)


SSP = SSP_2017 = make_enum("ICONICS:SSP(2017)")
SSP_2024 = make_enum("ICONICS:SSP(2024)")
