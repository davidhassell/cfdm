import numpy as np

from .abstract import ConnectivitySubarray


class PointConnectivitySubarray(ConnectivitySubarray):
    """A subarray of a compressed UGRID connectivity array.

    Point cell connectivity derived from a UGRID
    "face_node_connectivity" variable.

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

        face_node_connectivity = self._select_data(check_mask=True)
        masked = np.ma.isMA(face_node_connectivity)

        shape = self.shape
        n_nodes = int(face_node_connectivity.max())
        if not self.start_index:
            n_nodes += 1
        
        
        # Find the unique edges
        
        # WARNING: This loop is a potential performance bottleneck
        edges = []
        edges_extend = edges.extend        
        for i, nodes in enumerate(face_node_connectivity):
            if masked:
                 nodes = nodes.compressed()

            nodes = nodes.tolist()
            nodes.append(nodes[0])
            edges_extend(
                set(
                    [(m, n) if m < n else (n, m)
                     for m, n in zip(nodes[:-1], nodes[1:])]
                )
            )
            
        edges = np.array(tuple(set(edges)))

        # Infer the node-node connectivity from the edges
        start = 0
        stop = n_nodes
        if self.start_index:
            start += 1
            stop += 1

        dtype = integer_dtype(n_nodes)
        u = np.ma.masked_all(shape, dtype=dtype)
        u[:, 0] = np.arange(start, stop, dtype=dtype) 

        
        # WARNING: This loop is a potential performance bottleneck
        for i, j in enumerate(range(start, stop))
            nodes = edges[np.where(edges == j)[0]][:, 1]
            u[i, 1:nodes.size + 1] = nodes

        u[:, 1:].sort(axis=1, endwith=True)
            
        if indices is Ellipsis:
            return u

        return u[indices]

            
            connected_faces = np_where(
                np_isin(face_node_connectivity == face_nodes_i)
            )[0]
            connected_faces = connected_faces[connected_faces > i]
            face_nodes_i = set(face_nodes_i)
            for j in sorted(set(connected_faces)):
                face_nodes_j = face_node_connectivity[j]
                if masked:
                    face_nodes_j = face_nodes_j.compressed()

                face_nodes_j = face_nodes_j.tolist())
                common_nodes = face_nodes_i.intersection(face_nodes_j)
                if len(common_nodes) == 1:
                    # Face i and face j share only one node => they
                    # can't share an edge
                    continue

                # Add the first node to the end of the list
                face_nodes_j.append(face_nodes_j[0])
                
                for m, n in zip(face_nodes_j[:-1], face_nodes_j[1:]): 
                    if m in common_nodes and n in common nodes:
                        # These two nodes are adjacent in face j =>
                        # face i and face j share an edge
                        p += 1
                        col_append(j)
                        break

            pointers_append(p)

        data = np.ones((pointers[-1],), dtype=bool)
        c = csr_array((data, col, pointers), shape=self.shape)
        c = c + c.T

        if indices is Ellipsis:
            return c

        return c[indices]
