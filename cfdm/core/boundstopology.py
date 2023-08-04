from . import abstract


class BoundsTopology(abstract.Topology):
    """A bounds topology construct of the CF data model.

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

    def set_topology(self, topology):
        """Set the topology type.

        The topology type specifies which aspect of the mesh topology
        is represented by the bounds topology construct.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_topology`, `get_topology`, `has_topology`

        :Parameters:

            topology: `str`
                The value for the topology. Valid values are
                
                * ``'edge_node_connectivity'``: Identifying the vertex
                  nodes for every edge cell.

                * ``'face_node_connectivity'``: Identifying the vertex
                  nodes for every face cell.

                * ``'volume_node_connectivity'``: Identifying the vertex
                  nodes for every volume cell.

        :Returns:

             `None`

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_topology('face_node_connectivity')
        >>> c.has_topology()
        True
        >>> c.get_topology()
        'face_node_connectivity'
        >>> c.del_topology()
        'face_node_connectivity'
        >>> c.has_topology()
        False
        >>> print(c.del_topology(None))
        None
        >>> print(c.get_topology(None))
        None

        """
        return super().set_topology(topology)
