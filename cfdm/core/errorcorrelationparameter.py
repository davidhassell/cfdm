from . import abstract


class ErrorCorrelationParameter(abstract.PropertiesData):
    """TODOU A cell bounds component.

    That is, a cell bounds component of a coordinate or domain
    ancillary construct of the CF data model.

    An array of cell bounds spans the same domain axes as its
    coordinate array, with the addition of an extra dimension whose
    size is that of the number of vertices of each cell. This extra
    dimension does not correspond to a domain axis construct since it
    does not relate to an independent axis of the domain. Note that,
    for climatological time axes, the bounds are interpreted in a
    special way indicated by the cell method constructs.

    .. versionadded:: (cfdm) NEXTVERSION

    """
