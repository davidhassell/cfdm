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

            # 'pointers[p]' is the number faces connected to face i,
            # plus one.
            pointers_append(p)

        shape = self.shape
        dtype = integer_dtype(shape[0] + 1)
        if dtype == np.dtype('int32'):
            data = np.array(data, dtype=dtype)
            
        data = csr_array((data, col, pointers), shape=shape)
        data = (data + data.T).todense()
        if masked:
            # Replace zeos with missing data, and move masked elements
            # to the end of each row.
            data = np.ma.where(data == 0, np.ma.masked, data)
            data[:, 1:].sort(axis=1, endwith=True)

        data -= 1
        if indices is Ellipsis:
            return data

        return data[indices]
