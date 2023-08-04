from . import abstract


class CellTopology(abstract.Topology):
    """A cell topology construct of the CF data model.

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
        'cell_topology'

        """
        return "cell_topology"
    
    def set_topology(self, topology):
        """Set the topology type.

        The topology type specifies which aspect of the mesh topology
        is represented by the bounds topology construct.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_topology`, `get_topology`, `has_topology`

        :Parameters:

            topology: `str`
                The value for the topology type. Valid values are
                        
                * ``'node_node_connectivity'``: Identifying which node
                  cells are connected to which others. The connections
                  are via the externally defined edges of the mesh.

                * ``'edge_edge_connectivity'``: Identifying which edge
                  cells are connected to which others via shared nodes.

                * ``'face_face_connectivity'``: Identifying which face
                  cells are connected to which others via shared edges.

                * ``'volume_volume_connectivity'``: Identifying which
                  volume cells are connected to which others via
                  shared faces.

        :Returns:

             `None`

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_topology('face_face_connectivity')
        >>> c.has_topology()
        True
        >>> c.get_topology()
        'face_face_connectivity'
        >>> c.del_topology()
        'face_face_connectivity'
        >>> c.has_topology()
        False
        >>> print(c.del_topology(None))
        None
        >>> print(c.get_topology(None))
        None

        """
        return super().set_topology(topology)
