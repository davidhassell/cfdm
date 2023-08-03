import numpy as np

from ...core.utils import cached_property
from .abstract import Subarray


class CellConnectivitySubarray(Subarray):
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

        cell_connectivity = self._select_data(check_mask=False)
        shape = cell_connectivity.shape
        n_cells = shape[0]
        
        start_index = self.start_index
        if start_index:
            cell_connectivity = cell_connectivity - start_index
            
        if np.ma.is_masked(indices):
#            pointers = shape[1] - np.ma.getmaskarray(cell_connectivity).sum(axis=1)
            pointers = np.ma.count(cell_connectivity, axis=1)
            pointers = np.insert(pointers, 0, 0)
            cell_connectivity = cell_connectivity.compressed()
        else:
            pointers = np.full((n_cells + 1,), shape[1])
            pointers[0] = 0
            cell_connectivity = cell_connectivity.flatten()
            
        pointers = np.cumsum(pointers, out=pointers)
        
        start_index = self.start_index
        if start_index:
            cell_connectivity = cell_connectivity - start_index
            
        data = np.ones((cell_connectivity.size,), bool)
        c = csr_array((data, cell_connectivity, pointers),
                      shape=(n_cells, n_cells))
    
        if indices is Ellipsis:
            return c

        return c[indices]

    @cached_property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return np.dtype(bool)
