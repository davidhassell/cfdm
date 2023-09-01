from math import nan

import numpy as np

from ....functions import integer_dtype


class PointTopology:
    """Mixin class for point topology array compressed by UGRID.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        from scipy.sparse import csr_array

        start_index = self.start_index
        node_connectivity = self._select_data(check_mask=False)
        masked = np.ma.isMA(node_connectivity)

        max_node = node_connectivity.max()
        if not start_index:
            if max_node == np.iinfo(node_connectivity.dtype).max:
                node_connectivity = node_connectivity.astype(int, copy=False)

            # Add 1 to remove all zeros (0 is the fill value in the
            # sparse array)
            node_connectivity = node_connectivity + 1
            max_node += 1

        p = 0
        pointers = [0]
        cols = []
        u = []

        pointers_append = pointers.append
        cols_extend = cols.extend
        u_extend = u.extend

        # WARNING: This loop is a potential performance bottleneck
        for node in np.unique(node_connectivity).tolist():
            # Find the collection of all nodes that are joined to this
            # node via links in the mesh, including this node itself.
            nodes = self._connected_nodes(node, node_connectivity, masked)

            # Move 'node' to the front of the list
            nodes.remove(node)
            nodes.insert(0, node)

            n_nodes = len(nodes)
            p += n_nodes
            pointers_append(p)
            cols_extend(range(n_nodes))
            u_extend(nodes)

        u = np.array(u, dtype=integer_dtype(max_node))
        u = csr_array((u, cols, pointers))
        u = u.toarray()

        if nan in self.shape:
            # Now that the shape is known, store it.
            self._set_component("shape", u.shape, copy=False)

        if indices is not Ellipsis:
            u = u[indices]

        # Mask all zeros
        u = np.ma.where(u == 0, np.ma.masked, u)

        if not start_index:
            # Subtract 1 to get back to zero-based node identities
            u -= 1

        return u
