from math import nan

import numpy as np

from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopoologyFromEdgesSubarray(MeshSubarray, PointTopology):
    """A subarray of a compressed 

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """
    def _dddddd(self, node, node_connectivity):
        """ Find all nodes that are joined to *node* by an edge
        
        .. versionadded:: (cfdm) TODOUGRIDVER
        
        """
        return np.unique(
            node_connectivity[
                np.where(node_connectivity == node)[0]
            ]
        )
    
#    def __getitem__(self, indices):
#        """Return a subspace of the uncompressed data.
#
#        x.__getitem__(indices) <==> x[indices]
#
#        .. versionadded:: (cfdm) TODOUGRIDVER
#
#        """
#        from scipy.sparse import csr_array
#
#        start_index = self.start_index        
#        node_connectivity = self._select_data(check_mask=False)
#
#        dtype = node_connectivity.dtype
#        if not start_index:
#            if  max(node_connectivity) == np.iinfo(dtype).max:
#                node_connectivity = node_connectivity.astype(
#                    int, copy=False
#                )
#                dtype = node_connectivity.dtype
#                
#            # Add 1 to remove all zeros (0 is the fill value in the
#            # sparse array)
#            node_connectivity = node_connectivity + 1        
#        
#        p = 0
#        pointers = [0]
#        cols = []
#        u = []
#        pointers_append = pointers.append
#        col_extend = col.extend
#        u_extend = u.extend
#        np_unique = np.unique
#        np_where = np.where
#
#        # WARNING: This loop is a potential performance bottleneck
#        for node in np_unique(node_connectivity).tolist():
#            # Find all nodes that are joined to 'node' by an edge
#            nodes = np_unique(
#                node_connectivity[
#                    np_where(node_connectivity == node)[0]
#                ]
#            )
#            n_nodes = nodes.size
#            nodes = nodes.tolist()
#            nodes.remove(node)
#            nodes.insert(0, node)
#
#            p += n_nodes 
#            pointers_append(p)
#            cols_extend(range(n_nodes ))
#            u_extend(nodes)
#
#        u = np.array(u, dtype=dtype)
#        
#        u = csr_array((u, col, pointers))
#        u = u.toarray()
#
#        if self.shape == (nan, nan):
#            # Store the uncompressed shape
#            self._set_component('shape', u.shape, copy=False)
#        
#        if indices is not Ellipsis:
#            u = u[indices]
#
#        # Mask all zeros
#        u = np.ma.where(u == 0, np.ma.masked, u)
#
#        if not start_index:
#            # Subtract 1 to get back to zero-based node identities
#            u -= 1
#            u = u.astype(self.dtype, copy=False)
#        
#        return u
##
##    @property
##    def dtype(self):
##        """The data-type of the uncompressed data.
##
##        .. versionadded:: (cfdm) 1.10.0.0
##
##        """
##        return self._get_component('dtype')
      
