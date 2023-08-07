from . import abstract


class CellTopology(abstract.Topology):
    """A cell topology construct of the CF data model.

    The topology type is returned by `get_topology`. Defined types
    are:

    * ``'node_node_connectivity'``: Identifying which node cells are
      connected to which others. The connections are via the
      externally defined edges of the mesh.

    * ``'edge_edge_connectivity'``: Identifying which edge cealls are
      connected to which others via shared nodes.

    * ``'face_face_connectivity'``: Identifying which face cells are
      connected to which others via shared edges.

    * ``'volume_volume_connectivity'``: Identifying which volume cells
      are connected to which others via shared faces.

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
    
