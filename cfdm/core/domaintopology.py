from . import abstract


class DomainTopology(abstract.PropertiesData):
    """A domain topology construct of the CF data model.

    A domain topology construct describes explicitly the connectivity
    of domain cells indexed by a single domain axis construct. When
    two cells are connected, operations on the data stored on them may
    be assumed to be continuous across their common boundary.

    The domain topology array must be a symmetric matrix (i.e. a
    square matrix that is equal to its transpose), and is interpreted
    in a boolean context. The diagonal elements of this array must be
    False.

    A domain topology construct describes logically and explicitly the
    contiguity of domain cells indexed by a single domain axis
    construct, where two cells are described as contiguous if and only
    if they share at least one common boundary vertex. A domain
    construct allows contiguity to be ascertained without comparison
    of boundary vertices, which may be co-located for non-contiguous
    cells.

    A domain topology construct contains an array that spans a single
    domain axis construct with the addition of an extra dimension that
    indexes the cell bounds for the corresponding coordinates.
    Identical array values indicate that the corresponding cell
    vertices map to the same node of the domain, but otherwise the
    array values are arbitrary.

    In CF-netCDF a domain topology can only be provided for a domain
    defined by a UGRID mesh topology variable, supplied by a node
    connectivity variable, such as is named by a
    "face_node_connectivity" attribute. The indices contained in a
    node connectivity variable may be used directly to create a domain
    topology construct but the CF data model attaches no significance
    to the values, other than the fact that not all indices are the
    same.

    See CF Appendix I "The CF Data Model".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    @property
    def construct_type(self):
        """Return a description of the construct type.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `str`
                The construct type.

        **Examples:**

        >>> d = {{package}}.{{class}}()
        >>> d.construct_type
        'domain_topology'

        """
        return "domain_topology"
