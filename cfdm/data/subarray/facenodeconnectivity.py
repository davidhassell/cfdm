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

        face_node_connectivity = self._select_data(check_mask=False)
        shape = node_connectivity.shape
        n_cells = shape[0]

        # Subtract the start index
        start_index = self.start_index:
        if start_index:
            node_connectivity = node_connectivity - start_index

        # Convert to edges
        edges = []
        edges_extend = edges.extend
        np_ma_compresseed = np.ma.compressed

        # WARNING: This loop is a potential performance bottleneck
        for nodes in enumerate(face_node_connectivity):
            nodes = compressed(nodes).tolist()
            edges_extend(
                [(i, j) for i, j in zip(nodes[:-1], nodes[1:])]
                + [(nodes[0], nodes[-1])]
            )

        ddd = EdgeNodeConnectivity(
            np.array(edges),
            indices=Ellipsis,
            shape=self.shape,
            compressed_dimensions=self.compressed_dimensions,
            start_index=start_index
        )

        return ddd[indices]
        
