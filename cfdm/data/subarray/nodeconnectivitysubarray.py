import numpy as np

from ...core.utils import cached_property
from .abstract import Subarray


class ConnectivitySubarray(Subarray):
    """A subarray of a compressed UGRID connectivity array.

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

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

        node_connectivity = self._select_data(check_mask=False)
        
        row = []
        col = []
        indptr = [0]
#        row_extend = row.extend
        col_extend = col.extend
        indptr_append = indptr.append

        np_compressed = np.ma.compressed
        np_isin = np.isin
        np_where = np.where

        # WARNING: Potential performance bottleneck due to iteration
        #          through a numpy array
        n = 0
        for i, nodes in enumerate(node_connectivity):                
            # Find all of the cells that at least one node with cell i
            nodes = np_compressed(nodes).tolist()
            shared_nodes = np_isin(node_connectivity, nodes)
            connected_cells = set(np_where(shared_nodes)[0].tolist())
            connected_cells.remove(i)

            #            row_extend((i,) * len(connected_cells))
            n += len(connected_cells)
            indptr_append(n)
            
            col_extend(connected_cells)

        data = np.ones((len(row),), dtype=bool)
        n_cells = node_connectivity.shape[0]
#        c = csr_array((data, (row, col)), shape=(n_cells, n_cells))
        c = csr_array((data, col, indptr), shape=(n_cells, n_cells))



        if indices is Ellipsis:
            return c
        
        return c[indices]

    @cached_property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return np.dtype(bool)
