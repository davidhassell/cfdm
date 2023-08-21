from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopoologyFromVolumesSubarray(MeshSubarray, PointTopology):
    """A subarray of a compressed.

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def _dddddd(self, node, node_connectivity):
        """Return nodes that are joined to *node* by volume edges.

        The input *node* is included in the returned array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            node: `int`
                TODOUGRID

            node_connectivity: `numpy.ndarray`
                A "volume_node_connectivity" array.

        :Returns:

            `numpy.ndarray`
                TODOUGRID

        """
        return NotImplementedError("UGRID volumes are not currently supported")
