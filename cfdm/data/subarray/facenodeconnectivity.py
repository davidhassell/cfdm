import numpy as np

from .abstract import ConnectivitySubarray


class FaceEedgeConnectivitySubarray(ConnectivitySubarray):
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

        face_node_connectivity = self._select_data(check_mask=False)

        p = 0
        pointers = [0]
        col = []
        col_append = col.append
        pointers_append = pointers.append
        np_ma_compresseed = np.ma.compressed


        # WARNING: This loop is a potential performance bottleneck
        for i, face_nodes in enumerate(face_node_connectivity):
            face_nodes_i = np_ma_compressed(nodes)
            connected_faces = np_where(
                np_isin(face_node_connectivity == face_nodes_i)
            )[0]
            connected_faces = connected_faces[connected_faces > i]
            face_nodes_i = set(face_nodes_i)            
            for j in sorted(set(connected_faces)):
                face_nodes_j = np_ma_compressed(face_node_connectivity[j])
                face_nodes_j = face_nodes_j.tolist()
                
                common_nodes = tuple(face_nodes_i.intersection(face_nodes_j))
                if len(common_nodes) < 2:
                    continue

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
#
#
#
#    set_nodes = set(nodes)
#
#        # Find all of the edges j that possibly share an edge
#        # with face i
#        node_test = connectivity == nodes[0]
#        for node in nodes[1:]:
#            node_test |= connectivity == node
#
#        possibly_connected_faces = set(np.where(node_test)[0].tolist())
#        possibly_connected_faces.remove(i)
#
#        # For each possibly connected face j, check to see if
#        # it really connected to face i. Faces i and j are
#        # connected if they share two or more nodes which are
#        # adjacent in both faces.
#        for j in possibly_connected_faces:
#            nodes1 = compressed(connectivity[j]).tolist()
#            common_nodes = set_nodes.intersection(nodes1)
#            n_common_nodes = len(common_nodes)
#            if n_common_nodes < 2:
#                # Faces i and j share fewer than two nodes =>
#                # they are not connected
#                continue
#
#            if n_common_nodes == 2:
#                # Faces i and j share two nodes
#                x = common_nodes.pop()
#                y = common_nodes.pop()
#                diff = abs(nodes.index(x) - nodes.index(y))
#                if diff == 1 or diff == n_face_nodes_minus_1[i]:
#                    # The two common nodes are adjacent in
#                    # face i
#                    diff = abs(nodes1.index(x) - nodes1.index(y))
#                    if diff == 1 or diff == n_face_nodes_minus_1[j]:
#                        # The two common nodes are also
#                        # adjacent in face j => faces i and j
#                        # are connected
#                        row_ind.append(i)
#                        col_ind.append(j)
#            else:
#                # TODOUGRID: 3 or more shared nodes
#                pass
#
#    
#    # WARNING: This loop is a potential performance bottleneck
   #     for nodes in enumerate(face_node_connectivity):
   #         nodes = compressed(nodes).tolist()
   #         edges_extend(
   #             [(i, j) for i, j in zip(nodes[:-1], nodes[1:])]
   #             + [(nodes[0], nodes[-1])]
   #         )
   #
   #     ddd = EdgeNodeConnectivity(
   #         np.array(edges),
   #         indices=Ellipsis,
   #         shape=self.shape,
   #         compressed_dimensions=self.compressed_dimensions,
   #         start_index=start_index
   #     )
   #
   #     return ddd[indices]
        
