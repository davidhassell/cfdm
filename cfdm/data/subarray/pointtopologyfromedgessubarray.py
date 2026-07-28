import numpy as np

from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopologyFromEdgesSubarray(PointTopology, MeshSubarray):
    """A subarray of a point topology array compressed by UGRID edges.

    A subarray describes a unique part of the uncompressed array.

    .. versionadded:: (cfdm) 1.11.0.0

    """

    def _connected_nodes(self, node, node_connectivity, masked):
        """Return the nodes that are joined to *node* by edges.

        The input *node* is included at the start of the returned
        list.

        .. versionadded:: (cfdm) 1.11.0.0

        :Parameters:

            node: `int`
                A node identifier.

            node_connectivity: `numpy.ndarray`
                A UGRID "edge_node_connectivity" array.

            masked: `bool`, optional
                Whether or not *node_connectivity* has masked
                elements.

        :Returns:

            `list`
                All nodes that are joined to *node*, including *node*
                itself at the start.

        """
        nodes = sorted(
            set(
                node_connectivity[np.where(node_connectivity == node)[0]]
                .flatten()
                .tolist()
            )
        )

        # Move 'node' to the front of the list
        nodes.remove(node)
        nodes.insert(0, node)

        return nodes
    
    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data for edge connectivity."""
        from math import isnan
        print(123345456)

        start_index = self.start_index
        node_connectivity = self._select_data(check_mask=True)

        # ------------------------------------------------------------
        # E.g. 'node_connectivity' could be (two quadrilaterals and
        #      one triangle):
        #
        #      [[1 6]
        #       [3 6]
        #       [3 1]
        #       [0 1]
        #       [2 0]
        #       [2 3]
        #       [2 4]
        #       [5 4]
        #       [3 5]]
        # ------------------------------------------------------------

        masked = np.ma.isMA(node_connectivity)

        largest_node_id = node_connectivity.max()
        #if not start_index:
        #    if largest_node_id == np.iinfo(node_connectivity.dtype).max:
        #        node_connectivity = node_connectivity.astype(int, copy=False)
        #
        #    node_connectivity = node_connectivity + 1
        #    largest_node_id = largest_node_id + 1

        # ====================================================================
        # VECTORIZED EDGE-BASED ADJACENCY MATRIX CONSTRUCTION
        # ====================================================================

        # 1. Convert to dense array with dummy fill if masked
        if masked:
            nc = node_connectivity.filled(-1)
        else:
            nc = np.asarray(node_connectivity)

        # Extract edge pairs directly from columns (0 -> 1 and 1 -> 0)
        n1 = nc[:, 0]
        n2 = nc[:, 1]

        # Create bi-directional neighbor pairs (n1 -> n2 AND n2 -> n1)
        src_neighbours = np.concatenate([n1, n2])
        dst_neighbours = np.concatenate([n2, n1])

        u = self._point_point_connectivity(nc, src_neighbours, dst_neighbours)
        
        if any(map(isnan, self.shape)):
            # Store the shape, now that it is known.
            self._set_component("shape", u.shape, copy=False)

        if indices is not Ellipsis:
            u = u[indices]

        # Mask the padding values
        u = np.ma.where(u == -1, np.ma.masked, u)

        return u

