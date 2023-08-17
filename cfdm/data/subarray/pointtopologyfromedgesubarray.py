from math import nan

import numpy as np

from .abstract import PointTopoologySubarray


class PointtopoologyFromEdgesSubarray(PointTopoologySubarray):
    """A subarray of a compressed 

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

        edge_node_connectivity = self._select_data(check_mask=False)

        p = 0
        pointers = [0]
        cols = []
        u = []
        pointers_append = pointers.append
        col_append = col.append
        u_append = u.append
        u_extend = u.extend
        np_unique = np.unique
        np_where = np.where

        # WARNING: This loop is a potential performance bottleneck
        start_index = self.start_index
        i = 0
        node = start_index
        while True:          
            # Find all edge cells that include node i

            nodes = np_unique(
                edge_node_connectivity[
                    np_where(edge_node_connectivity == node)[0]
                ]
            )
            n_nodes = nodes.size
            if not n_nodes :
                # We've now checked all of the nodes
                break

            nodes = nodes.tolist()
            nodes.remove(i)
            nodes.insert(0, i)

            p += n_nodes 
            pointers_append(p)
            cols_extend(range(n_nodes ))
            u_extend(nodes)
  
            i += 1            
            node += 1            

        dtype = integer_dtype(i + 1)
        u = np.array(u, dtype=dtype)

        # Add 1 to remove all zeros (0 is the fill value in the sparse
        # array)
        u += 1
        
        u = csr_array((u, col, pointers)).toarray()

        if self.shape == (nan, nan):
            # Store the shape, now that we know what it is.
            self._set_component('shape', u.shape, copy=False)
        
        if indices is not Ellipsis:
            u = u[indices]
            
        u = np.ma.where(u == 0, np.ma.masked, u)

        # Subtract 1 to get back to zero-based node identities
        u -= 1
        return u

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) 1.10.0.0

        """
        return self._get_component('dtype')
      
