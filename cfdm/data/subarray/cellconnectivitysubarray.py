import numpy as np

from .abstract import MeshSubarray


class CellConnectivitySubarray(MeshSubarray):
    """A subarray of a compressed UGRID connectivity array.

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a TODOUGRID

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        shape = self.shape
        start = 0
        stop = shape[0]
        start_index = self.start_index
        if start_index:
            start += 1
            stop += 1

        data = self._select_data(check_mask=True)
        data_dtype = data.dtype            
        if stop -1 == np.iinfo(data_dtype).max:
            dtype= np.dtype(int)
        else:
            dtype =  data_dtype
            
        if np.ma.isMA(data):
            empty = np.ma.empty
        else:
            empty  = np.empty
            
        dtype = self.dty
        u = empty(shape, dtype=dtype)
        u[:, 0] = np.arange(start, stop, dtype=dtype) 
        u[:, 1:] = data

        if indices is not Ellipsis:
            u =  u[indices]

        # Make sure the values are zero-based
        if start_index:
            u -= 1
            
        if u.dtype != data_dtype:
            u = u.astype(data_dtype, copy=False)
        
        return u
