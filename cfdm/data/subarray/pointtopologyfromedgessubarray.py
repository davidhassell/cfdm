import numpy as np

from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopologyFromEdgesSubarray(PointTopology, MeshSubarray):
    """A subarray of a point topology array compressed by UGRID edges.

    A subarray describes a unique part of the uncompressed array.

    .. versionadded:: (cfdm) 1.11.0.0

    """
    
    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data for edge connectivity."""
        from math import isnan
#        print(123345456)

        node_connectivity = self._select_data(check_mask=True)

        # ------------------------------------------------------------
        # E.g. Two quadrilaterals and one triangle:
        #
        # node_connectivity = [[1 6]
        #                      [3 6]
        #                      [3 1]
        #                      [0 1]
        #                      [2 0]
        #                      [2 3]
        #                      [2 4]
        #                      [5 4]
        #                      [3 5]]
        # ------------------------------------------------------------

        if np.ma.isMA(node_connectivity):
            # Fill missing values in the face_node_connectivity array
            # with -1
            nc = node_connectivity.filled(-1)
        else:
            nc = np.asarray(node_connectivity)

        # Extract edge pairs directly from columns (0 -> 1 and 1 -> 0)
        n1 = nc[:, 0]
        n2 = nc[:, 1]

        # Create bi-directional neighbor pairs (n1 -> n2 and n2 -> n1)
        src_neighbours = np.concatenate([n1, n2])
        dst_neighbours = np.concatenate([n2, n1])

        # Create the full point_point_connectivity matrix (padded with
        # -1)
        u = self._point_point_connectivity(src_neighbours, dst_neighbours)
        del nc, src_neighbours, dst_neighbours
        
        if any(map(isnan, self.shape)):
            # Store the shape, now that it is known.
            self._set_component("shape", u.shape, copy=False)

        if indices is not Ellipsis:
            u = u[indices]

        # Mask the padding values
        u = np.ma.where(u == -1, np.ma.masked, u)

        # ------------------------------------------------------------
        # E.g. For the 'node_connectivity' example above:
        #
        # u = [[0 1 2 -- --]
        #      [1 0 3 6 --]
        #      [2 0 3 4 --]
        #      [3 1 2 5 6]
        #      [4 2 5 -- --]
        #      [5 3 4 -- --]
        #      [6 1 3 -- --]]        
        # ------------------------------------------------------------

        return u

