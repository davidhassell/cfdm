import numpy as np


class PointTopology:
    """Mixin class for point topology array compressed by UGRID.

    Subclasses must also inherit from `MeshSubarray`.

    .. versionadded:: (cfdm) 1.11.0.0

    """
    
    @staticmethod
    def _point_point_connectivity(nc, src_neighbours, dst_neighbours):
        """Mixin method for `__getitem__`.

        Keyword arguments have are identical to the smae variables in
        `__getitem__`.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        from cfdm.functions import integer_dtype

        # Filter out invalid/masked nodes and self-loops
        valid = (
            (src_neighbours >= 0)
            & (dst_neighbours >= 0)
            & (src_neighbours != dst_neighbours)
        )
        src_neighbours = src_neighbours[valid]
        dst_neighbours = dst_neighbours[valid]
        del valid

        # De-duplicate edge/neighbour pairs
        edges = np.column_stack([src_neighbours, dst_neighbours])
        del src_neighbours, dst_neighbours

        unique_edges = np.unique(edges, axis=0)
        del edges

        src_neighbours_uniq = unique_edges[:, 0]
        dst_neighbours_uniq = unique_edges[:, 1]
        all_nodes = np.unique(unique_edges)
        del unique_edges

        # Sort the neighbour pairs (dst_neighbours sorted per
        # src_node)
        sort_idx = np.lexsort((dst_neighbours_uniq, src_neighbours_uniq))
        src_neighbours_sorted = src_neighbours_uniq[sort_idx]
        dst_neighbours_sorted = dst_neighbours_uniq[sort_idx]
        del src_neighbours_uniq, dst_neighbours_uniq, sort_idx

        # Prepend the target node itself to every group so it sits at
        # column 0
        src_sorted = np.concatenate([all_nodes, src_neighbours_sorted])
        del src_neighbours_sorted
        dst_sorted = np.concatenate([all_nodes, dst_neighbours_sorted])
        del dst_neighbours_sorted
        
        num_unique_nodes = len(all_nodes)
        del all_nodes
        
        # Stable sort ensures column 0 contains 'node' followed by
        # sorted neighbours
        final_idx = np.argsort(src_sorted, kind="mergesort")
        src_sorted = src_sorted[final_idx]
        dst_sorted = dst_sorted[final_idx]
        del final_idx

        # 5. Find group boundaries and maximum node degree (row width)
        _, start_indices, counts = np.unique(
            src_sorted, return_index=True, return_counts=True
        )
        del src_sorted
        max_degree = counts.max()

        cols = np.arange(len(dst_sorted)) - np.repeat(start_indices, counts)
        del start_indices
        rows = np.repeat(np.arange(num_unique_nodes), counts)
        del counts
        
        # Initialise the 2-d uncompressed matrix with -1 everywhere
        largest_node_id = nc.max()
        dtype = integer_dtype(largest_node_id)
        u = np.full((num_unique_nodes, max_degree), -1, dtype=dtype)

        # Fill 'u' with the node indices
        u[rows, cols] = dst_sorted

        return u
