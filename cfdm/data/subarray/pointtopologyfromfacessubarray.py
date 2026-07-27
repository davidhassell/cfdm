import numpy as np

from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopologyFromFacesSubarray(PointTopology, MeshSubarray):
    """A subarray of a point topology array compressed by UGRID faces.

    A subarray describes a unique part of the uncompressed array.

    .. versionadded:: (cfdm) 1.11.0.0

    """

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data."""
        from math import isnan
        print('HERE HARE')
        from cfdm.functions import integer_dtype

        start_index = self.start_index
        node_connectivity = self._select_data(check_mask=True)

        # ------------------------------------------------------------
        # E.g. 'node_connectivity' could be (two quadrilaterals and
        #      one triangle):
        #
        #      [[2 3 1 0]
        #       [4 5 3 2]
        #       [6 1 3 --]]
        # ------------------------------------------------------------

        masked = np.ma.isMA(node_connectivity)

        # Fill missing values in the face_node_connectivity array with -1
        if masked:
            nc = node_connectivity.filled(-1)
        else:
            nc = node_connectivity
        
        # Extract left and right face-neighbours simultaneously
        nc_ravel = nc.ravel()
        src_neighbours = np.concatenate([nc_ravel, nc_ravel])
        del nc_ravel
        
        left = np.roll(nc, shift=1, axis=1)
        right = np.roll(nc, shift=-1, axis=1)
        dst_neighbours = np.concatenate([left.ravel(), right.ravel()])
        del left, right

        # Create the full point_point_connectivity matrix (padded with
        # -1)
        u = self._point_point_connectivity(nc, src_neighbours, dst_neighbours)
        
        if any(map(isnan, self.shape)):
            # Store the shape, now that it is known.
            self._set_component("shape", u.shape, copy=False)

        # Subspace the full point connectivity matrix
        if indices is not Ellipsis:
            u = u[indices]

        # Mask the padding values
        u = np.ma.where(u == -1, np.ma.masked, u)

        # ------------------------------------------------------------
        # E.g. For face_node_connectivity example above, 'u' would be:
        #
        #      [[0 1 2 -- --]
        #       [1 0 3 6 --]
        #       [2 0 3 4 --]
        #       [3 1 2 5 6]
        #       [4 2 5 -- --]
        #       [5 3 4 -- --]
        #       [6 1 3 -- --]]        
        # ------------------------------------------------------------

        return u

