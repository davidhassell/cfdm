import numpy as np

from .abstract import ConnectivitySubarray


class FaceEdgeConnectivitySubarray(ConnectivitySubarray):
    """A subarray of a compressed face-face via edge connectivity array.

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
        face_node_connectivity = self._select_data(check_mask=True)
        masked = np.ma.isMA(face_node_connectivity)

        shape = self.shape
        n_nodes = int(face_node_connectivity.max())
        if not self.start_index:
            n_nodes += 1

        x = n_nodes + 1
        
        # Find the unique edges
        
        # WARNING: This loop is a potential performance bottleneck
        edges = []
        edges_append = edges.append
        edges_extend = edges.extend        
        for i, nodes in enumerate(face_node_connectivity):
            if masked:
                 nodes = nodes.compressed()

            nodes = nodes.tolist()
            nodes.append(nodes[0])
            edges.append(
                [(m* x + n) if m < n else (n*x +  m)
                 for m, n in zip(nodes[:-1], nodes[1:])]
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

        p = 0
        pointers = [0]
        col = []
        data = []
        pointers_append = pointers.append
        col_append = col.append
        data_append = data.append
        np_isin = np.isin

        # WARNING: This loop is a potential performance bottleneck
        for i, face_nodes_i in enumerate(face_node_connectivity):
            data_append(i+1)
            p += 1
            
            if masked:
                 face_nodes_i = face_nodes_i.compressed()

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
                        data_append(j+1)
                        break

            pointers_append(p)

        shape = self.shape
        dtype = integer_dtype(shape[0] + 1)
        if dtype == np.dtype('int32'):
            data = np.array(data, dtype=dtype)
            
        data = csr_array((data, col, pointers), shape=shape)
        data = (data + data.T).todense()
    
        # Replace zeos with missing data and sort the values
        if masked:
            data = np.ma.where(data == 0, np.ma.masked, data)
            data[:, 1:].sort(axis=1, endwith=True)
        else:
            data[:, 1:].sort(axis=1)

        data -= 1
        if indices is Ellipsis:
            return data

        return data[indices]
