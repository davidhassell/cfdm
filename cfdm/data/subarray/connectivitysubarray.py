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
        shape = connectivity.shape
        shape0 = shape[0]

        topology_dimension = self.get_topology_dimension()
        row_ind = []
        col_ind = []
        
        if topology_dimension == 2:
            compressed = np.ma.compressed
            for i, nodes0 in enumerate(connectivity):
                nodes0 = compressed(nodes0).tolist()
                n_nodes0_minus_1 = len(nodes0) - 1
                set_nodes0 = set(nodes0)
                for j, nodes1 in enumerate(connectivity[i+1:]):
                    nodes1 = compressed(nodes1).tolist()
                    common = set_nodes0.intersection(nodes1)
                    n_common = len(common)
                    if n_common < 2:
                        # face0 and face1 are not connected
                        continue

                    if n_common == 2:
                        # Check that the common nodes comprise
                        # adjacent vertices in both faces
                        x = common.pop()
                        y = common.pop()
                        diff =  abs(nodes0.index(x) - nodes0.index(y))
                        if diff == 1  or diff == n_nodes0_minus_1:
                            # The common nodes are adjacent vertices
                            # in face0
                            diff =  abs(nodes1.index(x) - nodes1.index(y))
                            if diff == 1 or diff == len(nodes1) - 1:
                                # The common nodes are also adjacent
                                # vertices in face1 => face0 and face1
                                # are connected.
                                row_ind.append(i)
                                col_ind.append(i + 1 + j)
                    else:
                        # face0 and face1 are connected
                        row_ind.append(i)
                        col_ind.append(i + 1 + j)

        elif topology_dimension == 1:
            for i, nodes0 in enumerate(connectivity):
                set_nodes0 = set(nodes0.tolist())
                for j, nodes1 in enumerate(connectivity[i+1:]):
                    common = set_nodes0.intersection(nodes1.tolist())
                    if not common:
                        # edge0 and edge1 are not connected
                        continue

                    # edge0 and edge1 are connected
                    row_ind.append(i)
                    col_ind.append(i + 1 + j)
                            
        elif topology_dimension > 2:
            raise ValueError("Can't do volumes!!!!")

        # Create a upper diagonal sparse array, and then symmetrically
        # copy its values to the lower diagonal.
        data = np.ones((len(row_ind),), dtype=bool)
        c =  csr_array((data, (row_ind, col_ind)), shape=(shape0, shape0))
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
