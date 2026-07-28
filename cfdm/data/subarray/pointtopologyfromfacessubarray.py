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
        # ------------------------------------------------------------
        # E.g. Two quadrilaterals and one triangle:
        #
        # node_connectivity = [[2 3 1 0]
        #                      [4 5 3 2]
        #                      [6 1 3 --]]
        # ------------------------------------------------------------
        
        from math import isnan
#        print('HERE HARE', self.indices)
        node_connectivity = self._select_data(check_mask=True)
        is_masked = np.ma.is_masked(node_connectivity)

        if not is_masked:
            # No masked elements in 'node_connectivity'
            nc_ravel = node_connectivity.ravel()
            src_neighbours = np.concatenate([nc_ravel, nc_ravel])
            del nc_ravel

            left = np.roll(node_connectivity, shift=1, axis=1)
            right = np.roll(node_connectivity, shift=-1, axis=1)
            del node_connectivity
            dst_neighbours = np.concatenate([left.ravel(), right.ravel()])
            del left, right

        else:
            # Masked elements in 'node_connectivity'
            node_connectivity = node_connectivity.filled(-1)
            
            num_cols = node_connectivity.shape[1]
            col_0 = node_connectivity[:, 0]

            src_list, dst_list = [], []

            for col in range(num_cols):
                next_col = (col + 1) % num_cols
                prev_col = (col - 1) % num_cols

                u_col = node_connectivity[:, col]
                v_right = node_connectivity[:, next_col]
                v_left = node_connectivity[:, prev_col]

                # Right: If neighbour in next column is -1, wrap
                # around to column 0
                masked_right = (v_right == -1)
                if np.any(masked_right):
                    v_right = np.where(masked_right, col_0, v_right)

                # Left neighbour: If current node is -1, it has no
                # neighbours. If current node is in column 0, its left
                # neighbour is the last valid node (which is found
                # automatically via bi-directional pairing in
                # `_point_point_connectivity`)

                # Forward direction (current -> right)
                src_list.append(u_col)
                dst_list.append(v_right)

                # Backward direction (current -> left)
                src_list.append(u_col)
                dst_list.append(v_left)

            del node_connectivity

            src_neighbours = np.concatenate(src_list)
            dst_neighbours = np.concatenate(dst_list)

        # Create the full point_point_connectivity matrix (padded with -1)
        u = self._point_point_connectivity(src_neighbours, dst_neighbours)
        del src_neighbours, dst_neighbours

        if any(map(isnan, self.shape)):
            # Store the shape, now that it is known.
            self._set_component("shape", u.shape, copy=False)

        # Subspace the full point connectivity matrix
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
