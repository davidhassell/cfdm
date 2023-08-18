from math import nan

import numpy as np


class PointTopology:
    """TODOUGRID

    .. versionadded:: (cfdm) TODOUGRIDVER

    """
 
    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        from scipy.sparse import csr_array

        start_index = self.start_index
        node_connectivity = self._select_data(check_mask=False)

        dtype = node_connectivity.dtype
        if not start_index:
            if  max(node_connectivity) == np.iinfo(dtype).max:
                node_connectivity = node_connectivity.astype(
                    int, copy=False
                )
                dtype = node_connectivity.dtype
                
            # Add 1 to remove all zeros (0 is the fill value in the
            # sparse array)
            node_connectivity = node_connectivity + 1        
        
        p = 0
        pointers = [0]
        cols = []
        u = []
        
        pointers_append = pointers.append
        col_append = col.append
        u_extend = u.extend

        # WARNING: This loop is a potential performance bottleneck
        for node in np.unique(node_connectivity).tolist():
            # Find all nodes that are joined to 'node' by an edge of a
            # face
            nodes = self._dddddd(node, node_connectivity)
        
            n_nodes = nodes.size
            nodes = nodes.tolist()
            nodes.remove(node)
            nodes.insert(0, node)

            p += n_nodes 
            pointers_append(p)
            cols_extend(range(n_nodes ))
            u_extend(nodes)

        u = np.array(u, dtype=dtype)
        
        u = csr_array((u, col, pointers))
        u = u.toarray()

        if self.shape == (nan, nan):
            # Store the uncompressed shape
            self._set_component('shape', u.shape, copy=False)
        
        if indices is not Ellipsis:
            u = u[indices]

        # Mask all zeros
        u = np.ma.where(u == 0, np.ma.masked, u)

        if not start_index:
            # Subtract 1 to get back to zero-based node identities
            u -= 1
            u = u.astype(self.dtype, copy=False)
        
        return u
