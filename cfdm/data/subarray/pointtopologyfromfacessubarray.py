import numpy as np

from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopologyFromFacesSubarray(PointTopology, MeshSubarray):
    """A subarray of a point topology array compressed by UGRID faces.

    A subarray describes a unique part of the uncompressed array.

    .. versionadded:: (cfdm) 1.11.0.0

    """

    @classmethod
    def _connected_nodes(self, node, node_connectivity, masked, edges=False):
        """Return nodes that are joined to *node* by face edges.

        The input *node* is included at the start of the returned
        list.

        .. versionadded:: (cfdm) 1.11.0.0

        :Parameters:

            node: `int`
                A node identifier.

            node_connectivity: `numpy.ndarray`
                A UGRID "face_node_connectivity" array.

            masked: `bool`
                Whether or not *node_connectivity* has masked
                elements.

            edges: `bool`, optional
                By default *edges* is False and a flat list of nodes,
                including *node* itself at the start, is returned. If
                True then a list of edge definitions (i.e. a list of
                sorted, hashable tuple pairs of nodes) is returned
                instead.

                .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `list`
                All nodes that are joined to *node*, including *node*
                itself at the start. If *edges* is True then a list of
                edge definitions is returned instead.

        **Examples**

        >>> p._connected_nodes(7, nc)
        [7, 2, 1, 9]
        >>> p._connected_nodes(7, nc, edges=True)
        [(2, 7), (1, 7), (7, 9)]

        >>> p._connected_nodes(2, nc)
        [2, 8, 7]
        >>> p._connected_nodes(2, nc, edges=True)
        [(2, 8), (2, 7)]

        """
        if masked:
            where = np.ma.where
        else:
            where = np.where

        # Find the faces that contain this node:
        rows, cols = where(node_connectivity == node)

        nodes = []
        nodes_extend = nodes.extend

        # For each face, find which two of its nodes are neighbours to
        # 'node'.
        for row, col in zip(node_connectivity[rows], cols):
            if masked:
                row = row.compressed()

            row = row.tolist()

            # Find the position of 'node' in the face, and get its
            # neighbours.
            if not col:
                # 'node' is in position 0
                nodes_extend((row[-1], row[1]))
            elif col == len(row) - 1:
                # 'node' is in position -1
                nodes_extend((row[-2], row[0]))
            else:
                # 'node' is in any other position
                nodes_extend((row[col - 1], row[col + 1]))

        nodes = list(set(nodes))

        if edges:
            # Return a list of ordered edge definitions
            nodes = [(node, n) if node < n else (n, node) for n in nodes]
        else:
            # Return a flat list of nodes, including 'node' at the
            # start.
            nodes.insert(0, node)

        return nodes
