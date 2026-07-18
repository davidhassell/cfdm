from math import ceil, prod

import numpy as np

def calculate_chunk_metadata(shape, chunksizes):
    """
    Calculates the exact HDF5 B-tree metadata overhead for a chunked dataset.
    """
    if chunksizes is None:
        return 0  # Contiguous datasets have no chunk index

    rank = len(shape)
    
    # 1. Total Number of Chunks
    num_chunks = prod([ceil(s / c) for s, c in zip(shape, chunksizes)])
    
    # 2. Maximum memory size of a single B-Tree Node for this rank
    entry_size = 16 + (8 * rank)
    node_size_bytes = 24 + (32 * entry_size) + (33 * 8)
    
    # 3. Simulate the B-Tree allocation to find total nodes
    current_level_nodes = ceil(num_chunks / 32.0)
    total_nodes = current_level_nodes
    
    while current_level_nodes > 1:
        current_level_nodes = ceil(current_level_nodes / 32.0)
        total_nodes += current_level_nodes
        
    return total_nodes * node_size_bytes

#def calculate_optimal_meta_block(raw_payload_bytes):
#    # 1. Add the Fixed NetCDF-4/HDF5 Baseline Tax (16 KB)
#    # This covers `_NCProperties`, FSM tables, and hidden dimension scales.
#    baseline_tax = 16384
#    total_raw = raw_payload_bytes
#    
#    # 2. Double the footprint to guarantee safety against local heap bloat
#    buffered_bytes = total_raw * 2.0
#    
#    # 3. Round up to the nearest OS/Disk Page (4096 bytes)
#    os_page_size = 4096
#    final_size = ceil(buffered_bytes / os_page_size) * os_page_size
#
#    return final_size + baseline_tax

def calculate_group_metadata(group_name): #, num_children):
    """Calculates the HDF5 metadata overhead for a sub-group based on
    its storage layout.  num_children is the count of direct variables
    and sub-groups inside this group.

    """
#    if not group_name:
#        # Root group
#        return 2048

    parent_link_overhead = 40 + len(group_name)
    return 2048 + parent_link_overhead 
    
    # HDF5 switches to dense fractal heaps at around 8 members
    if num_children <= 8:
        base_overhead = 256
    else:
        # Allocates a v2 B-Tree and Local Heap for the group index
        base_overhead = 2048
        
    return parent_link_overhead + base_overhead

def Ax(attributes, **kwargs):    
    size = 0
    for name, value in attributes.items():
        size += 32 + len(name.encode("utf-8"))
        try:
            size += len(value.encode("utf-8"))
        except AttributeError:
            size += np.asanyarray(value).nbytes
    
    return size

def calculate_netcdf4_overhead(n_variables):
    size = 4096 + 256 * n_variables
    return size

def Cx(varname, shape, dimensions, contiguous, chunksizes, **kwargs):
    size = (
        80
        + len(varname.encode("utf-8"))
        + 8 * len(dimensions)
    )
    
    if contiguous:
        size += 32
    else:
        size += calculate_chunk_metadata(shape, chunksizes)
        
    return size

def Dx(n_vars):
    size = 224 + (16 * n_vars)
    return size
