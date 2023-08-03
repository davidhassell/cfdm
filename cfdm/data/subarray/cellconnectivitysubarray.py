import numpy as np

from ...core.utils import cached_property
from .abstract import ConnectivitySubarray


class CellConnectivitySubarray(ConnectivitySubarray):
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

        cell_connectivity = self._select_data(check_mask=False)
        shape = cell_connectivity.shape
        n_cells = shape[0]

        start_index = self.start_index
        if start_index:
            cell_connectivity = cell_connectivity - start_index

        if np.ma.is_masked(indices):
            #            pointers = shape[1] - np.ma.getmaskarray(cell_connectivity).sum(axis=1)
            indptr = np.ma.count(cell_connectivity, axis=1)
            indptr = np.insert(pointers, 0, 0)
            cell_connectivity = cell_connectivity.compressed()
        else:
            indptr = np.full((n_cells + 1,), shape[1])
            indptr[0] = 0
            cell_connectivity = cell_connectivity.flatten()

        indptr = np.cumsum(indptr, out=indptr)

        start_index = self.start_index
        if start_index:
            cell_connectivity = cell_connectivity - start_index

        data = np.ones((cell_connectivity.size,), bool)
        c = csr_array(
            (data, cell_connectivity, indptr), shape=(n_cells, n_cells)
        )

        if indices is Ellipsis:
            return c

        return c[indices]
