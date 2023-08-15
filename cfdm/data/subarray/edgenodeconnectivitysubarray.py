import numpy as np

from .abstract import ConnectivitySubarray


class EdgeNodeConnectivitySubarray(ConnectivitySubarray):
    """A subarray of a compressed edge-edge via node connectivity array.

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        `scipy` Compressed Sparse Row (CSR) array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        from scipy.sparse import csr_array

        edge_node_connectivity = self._select_data(check_mask=False)

        p = 0
        pointers = [0]
        col = []
        col_extend = col.extend
        pointers_append = pointers.append
        np_where = np.where

        # WARNING: This loop is a potential performance bottleneck
        i = self.start_index
        while True:
            # Find all edge cells that are connected with node i
            connected_edges = np_where(edge_node_connectivity == i)[0]
            m = connected_edges.size
            if not m:
                # We've now checked all of the nodes
                pointers_append(p)
                break
        
            i += 1
            connected_edges = connected_edges.tolist()
            connected_edges.remove(i)

            p += m - 1
            pointers_append(p)
            col_extend(connected_edges)

        data = np.ones((pointers[-1],), dtype=bool)
        c = csr_array((data, col, pointers), shape=self.shape)

        if indices is Ellipsis:
            return c

        return c[indices]
