import numpy as np

from ...core.utils import cached_property
from .abstract import Subarray


class ConnectivitySubarray(Subarray):
    """A subarray of a compressed UGRID connectivity array.

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) 1.10.0.0

    """

    def __init__(
        self,
        data=None,
        indices=None,
        shape=None,
        compressed_dimensions={},
        source=None,
        copy=True,
        context_manager=None,
    ):
        """**Initialisation**

        :Parameters:

            data: array_like
                The full compressed array spanning all subarrays, from
                which the elements for this subarray are defined by
                the *indices*.

            indices: `tuple` of `slice`
                The indices of *data* that define this subarray.

            shape: `tuple` of `int`
                The shape of the uncompressed subarray.

            compressed_dimensions: `dict`
                Mapping of compressed to uncompressed dimensions.

                A dictionary key is a position of a dimension in the
                compressed data, with a value of the positions of the
                corresponding dimensions in the uncompressed
                data. Compressed array dimensions that are not
                compressed must be omitted from the mapping.

                *Parameter example:*
                  ``{2: (2,)}``

            {{init source: optional}}

            {{init copy: `bool`, optional}}

            context_manager: function, optional
                A context manager that provides a runtime context for
                the conversion of data defined by *data* to a `numpy`
                array.

        """
        super().__init__(
            data=data,
            indices=indices,
            shape=shape,
            compressed_dimensions=compressed_dimensions,
            source=source,
            copy=copy,
            context_manager=context_manager,
        )

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        `scipy` Compressed Sparse Row (CSR) array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        from scipy.sparse import csr_array

        connectivity = self._select_data(check_mask=False)
        
        n_cells = connectivity.shape[0]
        n_nodes = np.ma.count(connectivity)

        row_ind = []
        col_ind = []

        cell_dimension = self.get_cell_dimension()
        if cell_dimension == 2:
            # Number of nodes per face, minus 1
            n_face_nodes_minus_1 = np.ma.count(connectivity, axis=1) - 1

            compressed = np.ma.compressed
            for i, nodes in enumerate(connectivity):
                nodes = compressed(nodes).tolist()
                set_nodes = set(nodes)                
                
                # Find all of the edges j that possibly share an edge
                # with face i
                node_test = connectivity == nodes[0]
                for node in nodes[1:]:
                    node_test |=  connectivity == node            
                    
                possibly_connected_faces = set(np.where(node_test)[0].tolist())
                possibly_connected_faces.remove(i)

                # For each possibly connected face j, check to see if
                # it really connected to face i. Faces i and j are
                # connected if they share two or more nodes which are
                # adjacent in both faces.
                for j in possibly_connected_faces:
                    nodes1 = compressed(connectivity[j]).tolist()
                    common_nodes = set_nodes.intersection(nodes1)
                    n_common_nodes = len(common_nodes)
                    if n_common_nodes < 2:
                        # Faces i and j share fewer than two nodes =>
                        # they are not connected
                        continue

                    if n_common_nodes == 2:
                        # Faces i and j share two nodes
                        x = common_nodes.pop()
                        y = common_nodes.pop()
                        diff = abs(nodes.index(x) - nodes.index(y))
                        if diff == 1 or diff == n_face_nodes_minus_1[i]:
                            # The two common nodes are adjacent in
                            # face i
                            diff = abs(nodes1.index(x) - nodes1.index(y))
                            if diff == 1 or diff == n_face_nodes_minus_1[j]:
                                # The two common nodes are also
                                # adjacent in face j => faces i and j
                                # are connected
                                row_ind.append(i)
                                col_ind.append(j)
                    else:
                        # TODOUGRID: 3 or more shared nodes
                        pass
                    
        elif cell_dimension == 1:            
            for i, nodes in enumerate(connectivity):                
                # Find all of the edges j that share one or two nodes
                # with face i
                node_test = connectivity == nodes[0]
                node_test |= connectivity == nodes[1]
                connected_edges = set(np.where(node_test)[0].tolist())
                connected_edges.remove(i)        
                for j in connected_edges:
                    row_ind.append(i)
                    col_ind.append(j)
                
        elif cell_dimension > 2:
            raise ValueError("Can't do volumes!!!!")

        # Create a upper diagonal sparse array, and then symmetrically
        # copy its values to the lower diagonal.
        data = np.ones((len(row_ind),), dtype=bool)
        c = csr_array((data, (row_ind, col_ind)), shape=(n_cells, n_cells))
        c = c[indices]
        # TODOUGRID: what if c[indices] does not create suitable square?

        return c + c.T

    #        if self.cell_cell_connectivity:
    #            if np.ma.is_masked(connectivity):
    #                indptr = shape[1] - np.ma.getmaskarray(connectivity).sum(axis=1)
    #                indptr = np.insert(indptr, 0, 0)
    #                connectivity = connectivity.compressed()
    #            else:
    #                indptr = np.full((shape0 + 1,), shape[1])
    #                indptr[0] = 0
    #                connectivity = connectivity.flatten()
    #
    #            indptr = np.cumsum(indptr, out=indptr)
    #
    #            start_index = self.get_start_index()
    #            if start_index:
    #                connectivity -= start_index
    #
    #            data = np.ones((connectivity.size,), dtype=bool)
    #            c = csc_array((data, connectivity, indptr), shape=(shape0, self.data.shape[0])))
    #            return c[:, self.indices[1])

    @cached_property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return np.dtype(bool)
