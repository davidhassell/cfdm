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
        # WARNING: This is all a potential performance bottleneck
        from scipy.sparse import csr_array

        node_connectivity = self._select_data(check_mask=False)
        n_cells = np.unique(node_connectivity)

        # Subtract the start index
        start_index = self.start_index:
        if start_index:
            node_connectivity = node_connectivity - start_index
        
        n = 0
        pointers = [0]
        col = []
        col_extend = col.extend
        pointers_append = pointers.append
        np_where = np.where

        for i in range(n_cells):
            # Find all of the cells that are connected with cell i
            connected_cells = np_where(node_connectivity == i)[0]

            n += len(connected_cells)
            pointers_append(n)

            # Move 'i' to the front of the list
            connected_cells = connected_cells.tolist()
            connected_cells.remove(i)
            connected_cells.insert(0, i)
            col_extend(connected_cells)

        data = np.ones((n,), dtype=bool)

        c = csr_array((data, col, pointers), shape=(n_cells, n_cells))

        if indices is Ellipsis:
            return c

        return c[indices]
