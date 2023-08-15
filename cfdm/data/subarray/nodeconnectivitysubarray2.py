import numpy as np

from .abstract import ConnectivitySubarray


class NodeConnectivitySubarray(ConnectivitySubarray):
    """A subarray of a compressed UGRID connectivity array.

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

        node_connectivity = self._select_data(check_mask=False)
        shape = node_connectivity.shape
        n_cells = shape[0]

        #        row = []
        col = []
        pointers = [0]
        #        row_extend = row.extend
        col_extend = col.extend
        pointers_append = pointers.append

        np_compressed = np.ma.compressed
        np_isin = np.isin
        np_where = np.where

        # WARNING: Potential performance bottleneck due to iteration
        #          through a numpy array
        n = 0
        for i, nodes in enumerate(node_connectivity):
            # Find all of the cells that share at least one node with
            # cell i
            nodes = np_compressed(nodes).tolist()
            shared_nodes = np_isin(node_connectivity, nodes)
            connected_cells = set(np_where(shared_nodes)[0].tolist())
            connected_cells.remove(i)

            #            row_extend((i,) * len(connected_cells))
            n += len(connected_cells) + 1
            pointers_append(n)

            col_extend([i] + sorted(connected_cells))

        data = np.ones((n,), dtype=bool)
        #        c = csr_array((data, (row, col)), shape=(n_cells, n_cells))
        c = csr_array((data, col, pointers), shape=(n_cells, n_cells))

        if indices is Ellipsis:
            return c

        return c[indices]
