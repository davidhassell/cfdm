from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopoologyFromFacesSubarray(MeshSubarray, PointTopology):
    """A subarray of a compressed.TODOUGRID

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def _connected_nodes(self, node, node_connectivity):
        """Return nodes that are joined to *node* by edges.

        The input *node* is included in the returned array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            node: `int`
                A node identifier.

            node_connectivity: `numpy.ndarray`
                A "face_node_connectivity" array.

        :Returns:

            `numpy.ndarray`
                The 1-d integer array of all nodes that are joined to
                *node*, including *node* itself.

        """
        return NotImplementedError("TODOUGRID")
