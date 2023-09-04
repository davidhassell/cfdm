import numpy as np

from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopoologyFromFacesSubarray(MeshSubarray, PointTopology):
    """A subarray of an point topology array compressed by UGRID faces.

    A subarray describes a unique part of the uncompressed array.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def _connected_nodes(self, node, node_connectivity, masked):
        """Return nodes that are joined to *node* by face edges.

        The input *node* is included in the returned array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            node: `int`
                A node identifier.

            node_connectivity: `numpy.ndarray`
                A "face_node_connectivity" array.

            masked: `bool`
                Whether or not *node_connectivity* has masked
                elements.

        :Returns:

            `list`
                All nodes that are joined to *node*, including *node*
                itself.

        """
        # Find face that contain this node:
        if masked:
            where = np.ma.where
        else:
            where = np.where

        faces = where(node_connectivity == node)[0]

        nodes = []
        nodes_extend = nodes.extend

        for face_nodes in node_connectivity[faces]:
            if masked:
                face_nodes = face_nodes.compressed()

            face_nodes = face_nodes.tolist()
            face_nodes.append(face_nodes[0])
            nodes_extend(
                [
                    m
                    for m, n in zip(face_nodes[:-1], face_nodes[1:])
                    if n == node
                ]
            )

        nodes = list(set(nodes))
        nodes.append(node)
        return nodes
