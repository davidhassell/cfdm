import numpy as np

from ....functions import integer_dtype
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
        data = []
        pointers_append = pointers.append
        col_extend = col.extend
        data_append = data.append
        data_extend = data.extend
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
            data_append(i)

            connected_edges += 1
            connected_edges = connected_edges.tolist()
            connected_edges.remove(i)

            p += m
            pointers_append(p)
            col_extend(connected_edges)
            data_extend(connected_edges)

        #        data = np.ones((pointers[-1],), dtype=bool)
        #        c = csr_array((data, col, pointers), shape=self.shape)
        #
        #        if indices is Ellipsis:
        #            return c
        #
        #        return c[indices]

        shape = self.shape
        dtype = integer_dtype(shape[0] + 1)
        if dtype == np.dtype("int32"):
            data = np.array(data, dtype=dtype)

        data = csr_array((data, col, pointers), shape=shape)
        data = (data + data.T).todense()

        if indices is not Ellipsis:
            return data[indices]

        data -= 1
        data[:, 1:].sort(axis=1)
        return data
