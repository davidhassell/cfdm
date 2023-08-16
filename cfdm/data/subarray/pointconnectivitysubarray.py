#import numpy as np
#
#from .abstract import ConnectivitySubarray
#
#
#class PointConnectivitySubarray(ConnectivitySubarray):
#    """A subarray of a compressed UGRID connectivity array.
#
#    A subarray describes a unique part of the uncompressed array.
#
#    See CF section 5.9 "Mesh Topology Variables".
#
#    .. versionadded:: (cfdm) TODOUGRIDVER
#
#    """
#
#    def __getitem__(self, indices):
#        """Return a subspace of the uncompressed data.
#
#        x.__getitem__(indices) <==> x[indices]
#
#        Returns a TODOUGRID
#
#        .. versionadded:: (cfdm) TODOUGRIDVER
#
#        """
#        shape = self.shape
#        n_cells = shape[0]
#        dtype = integer_dtype(n_cells)
#
#        start = 0
#        stop = n_cells
#        if self.start_index:
#            start += 1
#            stop += 1
#
#        u = np.ma.empty((n_cells, shape[1] + 1), dtype=dtype)
#        u[:, 0] = np.arange(start, stop, dtype=dtype)
#
#        ddd =  self._select_data(check_mask=False)
#        # Need to 
#        
#        u[:, 1:] = ddd
#        
#        if indices is Ellipsis:
#            return u
#
#        return u[indices]
#
