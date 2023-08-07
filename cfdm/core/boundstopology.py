from . import abstract


class BoundsTopology(abstract.Topology):
    """A bounds topology construct of the CF data model.

    The topology type is returned by `get_topology`. Defined types
    are:

    * ``'edge_node_connectivity'``: Identifying the vertex nodes for
      every edge cell.

    * ``'face_node_connectivity'``: Identifying the vertex nodes for
      every face cell.

    * ``'volume_node_connectivity'``: Identifying the vertex nodes for
      every volume cell.

    TODOUGRIDVER

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
        'bounds_topology'

        """
        return "bounds_topology"
